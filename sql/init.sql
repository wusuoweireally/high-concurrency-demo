USE seckill;

CREATE TABLE IF NOT EXISTS products (
  id      INT PRIMARY KEY,
  name    VARCHAR(64) NOT NULL,
  stock   INT NOT NULL,            -- 剩余库存
  version INT NOT NULL DEFAULT 0   -- 乐观锁版本号, 每次扣库存 +1
);

CREATE TABLE IF NOT EXISTS orders (
  id         BIGINT AUTO_INCREMENT PRIMARY KEY,
  user_id    VARCHAR(64) NOT NULL,
  product_id INT NOT NULL,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  -- 唯一索引: 同一用户对同一商品只能下一单, 数据库层防重复下单
  UNIQUE KEY uk_user_product (user_id, product_id)
);

INSERT INTO products (id, name, stock, version) VALUES (1, '秒杀商品', 100, 0);
