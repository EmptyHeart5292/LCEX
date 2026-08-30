-- 订单表:order 服务拥有写入权(clearing 仅更新 reserved 列)
BEGIN;

CREATE TABLE orders (
    order_id        BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_id         BIGINT NOT NULL,
    symbol          TEXT NOT NULL,
    client_order_id TEXT NOT NULL,
    side            TEXT NOT NULL CHECK (side IN ('bid', 'ask')),
    order_type      TEXT NOT NULL CHECK (order_type IN ('limit', 'market')),
    tif             TEXT NOT NULL CHECK (tif IN ('gtc', 'ioc', 'fok')),
    stp             TEXT NOT NULL DEFAULT 'none' CHECK (stp IN ('none', 'cancel_taker')),
    post_only       BOOLEAN NOT NULL DEFAULT FALSE,
    price           BIGINT,             -- 定点 ×1e8;市价单 NULL
    qty             BIGINT NOT NULL,
    filled_qty      BIGINT NOT NULL DEFAULT 0,
    reserved        BIGINT NOT NULL DEFAULT 0,  -- 本单冻结余额(quote=买 / base=卖),随成交扣减
    status          TEXT NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending', 'open', 'partially_filled', 'filled', 'canceled', 'rejected')),
    reject_code     INT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (user_id, client_order_id)
);
CREATE INDEX idx_orders_user ON orders(user_id, created_at DESC);
CREATE INDEX idx_orders_symbol_open ON orders(symbol, order_id)
    WHERE status IN ('open', 'partially_filled');

COMMIT;
