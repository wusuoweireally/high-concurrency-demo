import express from 'express';
import { join } from 'node:path';
import type { RowDataPacket } from 'mysql2/promise';
import {
  BUYERS_KEY, GEN_KEY, INITIAL_STOCK, PRODUCT_ID, QUEUE_KEY, STOCK_KEY,
  pool, redis, sleep, withWorkerLock,
} from './db';
import { startWorker, strategies } from './strategies';

const app = express();
app.use(express.json());
app.use(express.static(join(import.meta.dirname, '../public')));

// 一次下单的结果: 唯一索引冲突 = 同一用户重复下单 (由数据库兜底拦截), 其他异常 = ERROR
async function buyOnce(strategy: string, userId: string) {
  const handler = strategies[strategy];
  if (!handler) return { result: 'ERROR', code: 'UNKNOWN_STRATEGY' };
  try {
    return { result: await handler.run(userId) };
  } catch (e: any) {
    return e.code === 'ER_DUP_ENTRY' ? { result: 'DUPLICATE' } : { result: 'ERROR', code: e.code };
  }
}

// 策略清单: 页面的下拉框、说明文字和压测脚本都从这里取, 不再各写一份
app.get('/strategies', (_req, res) => {
  res.json(Object.entries(strategies).map(([name, s]) => ({ name, label: s.label, desc: s.desc })));
});

// 同一个接口, 用 :strategy 选择下单策略: naive | pessimistic | optimistic | atomic | redis
app.post('/buy/:strategy', async (req, res) => {
  res.json(await buyOnce(req.params.strategy, req.body.userId));
});

// 页面用的"同一时刻抢购": 浏览器对同一域名只开约 6 个并发连接, 所以把并发放在服务端执行
app.post('/burst/:strategy', async (req, res) => {
  res.json(await Promise.all(req.body.userIds.map((userId: string) => buyOnce(req.params.strategy, userId))));
});

// 以下接口只给压测脚本和页面用, 不要暴露到生产环境
app.post('/reset', async (_req, res) => {
  // 和 worker 落库互斥; 同时自增重置代号, 让 worker 丢弃重置之前抢到的队列项
  await withWorkerLock(async () => {
    await redis.incr(GEN_KEY);
    await pool.query('UPDATE products SET stock = ?, version = 0 WHERE id = ?', [INITIAL_STOCK, PRODUCT_ID]);
    await pool.query('DELETE FROM orders');
    await redis.del(QUEUE_KEY, BUYERS_KEY);
    await redis.set(STOCK_KEY, INITIAL_STOCK);
  });
  res.json({ ok: true });
});

async function readStats() {
  const [[product]] = await pool.query<RowDataPacket[]>('SELECT stock FROM products WHERE id = ?', [PRODUCT_ID]);
  const [[order]] = await pool.query<RowDataPacket[]>('SELECT COUNT(*) AS n FROM orders');
  const stock: number = product.stock;
  const orders: number = order.n;
  const sold = INITIAL_STOCK - stock;
  // 正确: 没超卖, 且卖出的数量 (初始库存 - 剩余) 和订单数一致
  const verdict = stock < 0 || orders > INITIAL_STOCK ? '❌ 超卖' : orders !== sold ? '❌ 库存与订单对不上' : '✅ 正确';
  return { stock, orders, initialStock: INITIAL_STOCK, verdict };
}

// waitFor: 最多等 5 秒直到订单数追上, 给 redis 策略的异步落库留时间, 否则会误判成"库存与订单对不上"
app.get('/stats', async (req, res) => {
  const waitFor = Number(req.query.waitFor) || 0;
  let stats = await readStats();
  for (let i = 0; i < 50 && stats.orders < waitFor; i++) {
    await sleep(100);
    stats = await readStats();
  }
  res.json(stats);
});

app.listen(3000, () => console.log('server: http://localhost:3000'));
startWorker();
