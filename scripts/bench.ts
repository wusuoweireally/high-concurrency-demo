// 压测脚本: 每种策略各跑一轮, 同一时刻发出 500 个下单请求 (250 个用户, 每人相邻发出两次, 模拟手抖连点)
// 库存 100, 理想结果: 成功 100, 重复 100 (被拦截), 售罄 300, 库存 0, 订单 100 单, 没有超卖
const BASE = 'http://localhost:3000';
const REQUESTS = 500;
const USERS = REQUESTS / 2;

interface Stats {
  stock: number;
  orders: number;
  initialStock: number;
  verdict: string;
}

const post = (path: string, body: object = {}): Promise<any> =>
  fetch(BASE + path, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
  }).then((r) => r.json());
// waitFor: 让服务端等到订单数追上再返回, 避免 redis 策略的异步落库还没写完就统计
const getStats = (waitFor = 0): Promise<Stats> =>
  fetch(`${BASE}/stats?waitFor=${waitFor}`).then((r) => r.json());

async function main() {
  const list: { name: string }[] = await fetch(`${BASE}/strategies`).then((r) => r.json());
  const { initialStock } = await getStats();
  console.log(`库存 ${initialStock}, 并发 ${REQUESTS} 个请求, ${USERS} 个用户`);

  const rows = [];
  for (const { name } of list) {
    await post('/reset');
    const start = performance.now();
    const results: { result: string }[] = await Promise.all(
      Array.from({ length: REQUESTS }, (_, i) => post(`/buy/${name}`, { userId: `u${Math.floor(i / 2)}` })),
    );
    const count = (result: string) => results.filter((r) => r.result === result).length;

    const stats = await getStats(count('OK'));
    rows.push({
      策略: name,
      '耗时(ms)': Math.round(performance.now() - start),
      成功: count('OK'),
      售罄: count('SOLD_OUT'),
      重复: count('DUPLICATE'),
      失败: count('BUSY') + count('ERROR'),
      订单: stats.orders,
      剩余库存: stats.stock,
      结论: stats.verdict,
    });
  }
  console.table(rows);
}

main();
