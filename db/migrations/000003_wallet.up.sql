-- 钱包:充值地址、链上充值、提现(wallet 服务拥有写入权)
BEGIN;

CREATE TABLE deposit_addresses (
    id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_id     BIGINT NOT NULL,
    currency    TEXT NOT NULL REFERENCES currencies(code),
    network     TEXT NOT NULL,
    address     TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (user_id, currency, network),
    UNIQUE (address)
);

CREATE TABLE deposits (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_id         BIGINT NOT NULL,
    currency        TEXT NOT NULL REFERENCES currencies(code),
    network         TEXT NOT NULL,
    address         TEXT NOT NULL,
    amount          BIGINT NOT NULL CHECK (amount > 0),
    txid            TEXT NOT NULL,
    output_index    INT NOT NULL DEFAULT 0,
    confirmations   INT NOT NULL DEFAULT 0,
    required_conf   INT NOT NULL,
    status          TEXT NOT NULL CHECK (status IN ('pending', 'credited')),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    credited_at     TIMESTAMPTZ,
    UNIQUE (txid, output_index)
);
CREATE INDEX idx_deposits_user ON deposits(user_id, created_at DESC);

CREATE TABLE withdrawals (
    id               BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_id          BIGINT NOT NULL,
    currency         TEXT NOT NULL REFERENCES currencies(code),
    network          TEXT NOT NULL,
    address          TEXT NOT NULL,
    amount           BIGINT NOT NULL CHECK (amount > 0),
    fee              BIGINT NOT NULL CHECK (fee >= 0),
    client_order_id  TEXT,
    txid             TEXT,
    status           TEXT NOT NULL CHECK (status IN
                     ('pending_review', 'broadcasting', 'completed', 'rejected', 'failed')),
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at     TIMESTAMPTZ,
    UNIQUE (user_id, client_order_id)
);
CREATE INDEX idx_withdrawals_user ON withdrawals(user_id, created_at DESC);

COMMIT;
