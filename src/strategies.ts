import type { ResultSetHeader, RowDataPacket } from 'mysql2/promise';
import {
  BUYERS_KEY, GEN_KEY, PRODUCT_ID, QUEUE_KEY, STOCK_KEY,
  inTx, pool, redis, sleep, withWorkerLock,
} from './db';

export type Result = 'OK' | 'SOLD_OUT' | 'DUPLICATE' | 'BUSY' | 'ERROR';

const MAX_RETRY = 3;

// 1. 朴素版: 先查后改, 查询和更新之间没有任何保护 (check-then-act 竞态)
//    也没有事务: 扣库存成功、插订单失败时, 库存少了却没有订单
async function naive(userId: string): Promise<Result> {
  const [[p]] = await pool.query<RowDataPacket[]>('SELECT stock FROM products WHERE id = ?', [PRODUCT_ID]);
  if (p.stock <= 0) return 'SOLD_OUT';
  await pool.query('UPDATE products SET stock = stock - 1 WHERE id = ?', [PRODUCT_ID]);
  await pool.query('INSERT INTO orders (user_id, product_id) VALUES (?, ?)', [userId, PRODUCT_ID]);
  return 'OK';
}

// 2. 悲观锁: FOR UPDATE 锁住商品行, 其他事务排队等待; 正确, 但请求被串行化, 吞吐受限
async function pessimistic(userId: string): Promise<Result> {
  return inTx(async (conn) => {
    const [[p]] = await conn.query<RowDataPacket[]>('SELECT stock FROM products WHERE id = ? FOR UPDATE', [PRODUCT_ID]);
    if (p.stock <= 0) return 'SOLD_OUT';
    await conn.query('UPDATE products SET stock = stock - 1 WHERE id = ?', [PRODUCT_ID]);
    await conn.query('INSERT INTO orders (user_id, product_id) VALUES (?, ?)', [userId, PRODUCT_ID]);
    return 'OK';
  });
}

// 3. 乐观锁: 不加锁, 更新时把读到的 version 作为条件; 被别人抢先改过 (affectedRows=0) 就重试
//    竞争越激烈重试越多, 次数用完仍失败的请求返回 BUSY
async function optimistic(userId: string): Promise<Result> {
  for (let i = 0; i < MAX_RETRY; i++) {
    const result = await inTx<Result | 'RETRY'>(async (conn) => {
      const [[p]] = await conn.query<RowDataPacket[]>('SELECT stock, version FROM products WHERE id = ?', [PRODUCT_ID]);
      if (p.stock <= 0) return 'SOLD_OUT';
      const [r] = await conn.query<ResultSetHeader>(
        'UPDATE products SET stock = stock - 1, version = version + 1 WHERE id = ? AND version = ?',
        [PRODUCT_ID, p.version],
      );
      if (r.affectedRows === 0) return 'RETRY';
      await conn.query('INSERT INTO orders (user_id, product_id) VALUES (?, ?)', [userId, PRODUCT_ID]);
      return 'OK';
    });
    if (result !== 'RETRY') return result;
  }
  return 'BUSY';
}

// 4. 原子条件更新: 判断和扣减合成一条 SQL, 行锁保证不会超卖, 应用层不用自己加锁; 生产里最常用的写法
async function atomic(userId: string): Promise<Result> {
  return inTx(async (conn) => {
    const [r] = await conn.query<ResultSetHeader>('UPDATE products SET stock = stock - 1 WHERE id = ? AND stock > 0', [PRODUCT_ID]);
    if (r.affectedRows === 0) return 'SOLD_OUT';
    await conn.query('INSERT INTO orders (user_id, product_id) VALUES (?, ?)', [userId, PRODUCT_ID]);
    return 'OK';
  });
}

// 5. Redis 预扣减: Lua 在 Redis 内原子执行 (查重 + 扣库存 + 入队), 大部分请求在这里就被挡掉, 不会打到 MySQL
//    返回 OK 只代表抢到名额, 订单由下面的 worker 异步写入 MySQL
//    队列项格式为 "重置代号:userId", 代号在入队时就固定下来, 用来识别重置前抢到的旧数据
const BUY_LUA = `
if redis.call('SISMEMBER', KEYS[2], ARGV[1]) == 1 then return 2 end
if tonumber(redis.call('GET', KEYS[1]) or '0') <= 0 then return 0 end
redis.call('DECR', KEYS[1])
redis.call('SADD', KEYS[2], ARGV[1])
redis.call('LPUSH', KEYS[3], (redis.call('GET', KEYS[4]) or '0') .. ':' .. ARGV[1])
return 1
`;
// 下标对应 Lua 返回值: 0 售罄, 1 抢到, 2 重复下单; 越界说明脚本返回值不符合预期
const LUA_RESULTS: Result[] = ['SOLD_OUT', 'OK', 'DUPLICATE'];

async function redisPreDeduct(userId: string): Promise<Result> {
  const code = (await redis.eval(BUY_LUA, 4, STOCK_KEY, BUYERS_KEY, QUEUE_KEY, GEN_KEY, userId)) as number;
  return LUA_RESULTS[code] ?? 'ERROR';
}

// 落库: Redis 只负责发名额, MySQL 才是库存和订单的最终依据
// 落库没成功的必须把 Redis 名额退还, 否则 Redis 少了库存, MySQL 却没有对应订单
async function settle(userId: string) {
  try {
    const result = await atomic(userId);
    if (result === 'OK') return;
    // MySQL 说没库存而 Redis 说有名额: 两边已经不一致, 以 MySQL 为准把 Redis 库存清零止损
    await redis.set(STOCK_KEY, 0);
    console.error('落库时 MySQL 已售罄, Redis 库存已按 MySQL 清零', userId);
  } catch (e: any) {
    await redis.incr(STOCK_KEY);
    if (e.code === 'ER_DUP_ENTRY') {
      // MySQL 里已经有这个用户的订单: 保留去重标记, 只退还这次预扣的库存
      console.error('落库时发现重复订单, 已退还 Redis 名额', userId);
      return;
    }
    // 其他异常 (比如 MySQL 连不上): 连去重标记一起清掉, 让用户可以重试
    await redis.srem(BUYERS_KEY, userId);
    console.error('落库失败, 已退还 Redis 名额', userId, e.code ?? e.message);
  }
}

// 异步落库 worker: 从队列取出抢到名额的用户, 复用 atomic 的事务写入 MySQL
// BRPOP 会阻塞连接, 所以用 duplicate() 单独开一个连接, 不占用主连接
export async function startWorker() {
  const queue = redis.duplicate();
  for (;;) {
    try {
      const [, raw] = (await queue.brpop(QUEUE_KEY, 0))!;
      const sep = raw.indexOf(':');
      const gen = raw.slice(0, sep);
      const userId = raw.slice(sep + 1);
      await withWorkerLock(async () => {
        // 代号对不上说明这条是 /reset 之前抢到的, 重置已经把库存和订单清空, 直接丢弃
        if ((await redis.get(GEN_KEY)) !== gen) {
          console.log('丢弃重置前的队列项', userId);
          return;
        }
        await settle(userId);
      });
    } catch (e: any) {
      console.error('worker 异常, 1 秒后重试', e.code ?? e.message);
      await sleep(1000);
    }
  }
}

export interface Strategy {
  label: string; // 页面下拉框里的短名称
  desc: string; // 页面上展示的策略说明
  run: (userId: string) => Promise<Result>;
}

// 策略清单的唯一来源: 页面和压测脚本都通过 GET /strategies 读取, 新增策略只需要改这里
export const strategies: Record<string, Strategy> = {
  naive: {
    label: '朴素版',
    desc: '先查库存再扣减, 两步之间没有任何保护. 多个请求同时读到库存 > 0, 全部通过判断, 于是超卖.',
    run: naive,
  },
  pessimistic: {
    label: '悲观锁',
    desc: 'SELECT ... FOR UPDATE 锁住商品行, 其他请求排队等待. 结果正确, 但请求被串行化.',
    run: pessimistic,
  },
  optimistic: {
    label: '乐观锁',
    desc: '不加锁, 更新时带上 version 条件, 冲突了就重试. 竞争越激烈, 重试耗尽失败的越多.',
    run: optimistic,
  },
  atomic: {
    label: '原子条件更新',
    desc: 'UPDATE ... WHERE stock > 0 一条语句同时完成判断和扣减, 由行锁保证原子性.',
    run: atomic,
  },
  redis: {
    label: '预扣减 + 队列',
    desc: 'Redis Lua 原子地完成查重、扣库存、入队, 挡住大部分请求; 进队列的人由 worker 异步写入 MySQL.',
    run: redisPreDeduct,
  },
};
