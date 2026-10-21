import { randomUUID } from 'node:crypto';
import Redis from 'ioredis';
import { createPool, type PoolConnection } from 'mysql2/promise';

export const PRODUCT_ID = 1;
export const INITIAL_STOCK = 100;

// Redis 里的 key: 剩余库存 / 已抢到名额的用户 (防重复) / 待落库的订单队列 / worker 锁 / 重置代号
export const STOCK_KEY = 'seckill:stock';
export const BUYERS_KEY = 'seckill:buyers';
export const QUEUE_KEY = 'seckill:queue';
export const WORKER_LOCK_KEY = 'seckill:worker:lock';
export const GEN_KEY = 'seckill:gen';

// 连接池: 连接复用, connectionLimit 就是同时打到 MySQL 的最大连接数
export const pool = createPool({
  host: '127.0.0.1',
  user: 'seckill',
  password: 'seckill',
  database: 'seckill',
  connectionLimit: 10,
});

export const redis = new Redis();

export const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

// worker 落库和 /reset 重置互斥, 保证不会出现"重置刚做完, 又开始写进一单旧的"顺序问题
export async function withWorkerLock<T>(fn: () => Promise<T>): Promise<T> {
  const token = randomUUID();
  const deadline = Date.now() + 20_000;
  // PX 是兜底: 持锁进程挂掉时 10 秒后自动释放, 不会永久卡死
  while (!(await redis.set(WORKER_LOCK_KEY, token, 'PX', 10_000, 'NX'))) {
    if (Date.now() > deadline) throw new Error('等待 worker 锁超时');
    await sleep(20);
  }
  try {
    return await fn();
  } finally {
    await redis.del(WORKER_LOCK_KEY);
  }
}

// 事务包装: 回调成功则 COMMIT, 抛错则 ROLLBACK, 多条 SQL 要么全部生效要么全部不生效
export async function inTx<T>(fn: (conn: PoolConnection) => Promise<T>): Promise<T> {
  const conn = await pool.getConnection();
  try {
    await conn.beginTransaction();
    const result = await fn(conn);
    await conn.commit();
    return result;
  } catch (e) {
    await conn.rollback();
    throw e;
  } finally {
    conn.release();
  }
}
