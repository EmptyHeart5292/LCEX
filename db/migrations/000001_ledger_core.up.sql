-- 账本核心:币种、账户、业务凭证(journal)、分录(ledger_entries)
-- 归属:clearing 服务拥有写权限;order/wallet 经其接口入账
-- 不变式(见 db/README.md):
--   1. 每个 journal 的分录借贷必平(同币种)
--   2. accounts.balance == 该账户全部分录按方向累计(v_balance_mismatch 返回 0 行)
--   3. 幂等:journals(biz_type, biz_id) 唯一,重复入账在 DB 层被拒绝

BEGIN;

CREATE TABLE currencies (
    code        TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    precision   SMALLINT NOT NULL,
    scale       INT NOT NULL DEFAULT 8,
    active      BOOLEAN NOT NULL DEFAULT TRUE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO currencies (code, name, precision, scale) VALUES
    ('USDT', 'Tether',  8, 8),
    ('BTC',  'Bitcoin', 8, 8),
    ('ETH',  'Ethereum', 8, 8),
    ('SOL',  'Solana',  8, 8);

-- 账户:余额节点。amount 全部为定点整数(× 1e8),杜绝浮点
-- 方向语义(经典会计):
--   资产类(system = 链上热/冷钱包):debit 增加,credit 减少
--   负债/权益/收入类(user / market_maker / fee / equity):credit 增加,debit 减少
--   每条 journal 的 debit 总额 == credit 总额(逐币种),所有账户余额恒非负
CREATE TABLE accounts (
    id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    owner_id    BIGINT NOT NULL,
    owner_type  TEXT NOT NULL DEFAULT 'user'
                CHECK (owner_type IN ('user', 'market_maker', 'system', 'fee', 'equity')),
    currency    TEXT NOT NULL REFERENCES currencies(code),
    type        TEXT NOT NULL CHECK (type IN ('available', 'frozen')),
    balance     BIGINT NOT NULL DEFAULT 0 CHECK (balance >= 0),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (owner_id, owner_type, currency, type)
);
CREATE INDEX idx_accounts_owner ON accounts(owner_id, owner_type);

-- 业务凭证:一次业务操作(冻结、成交、手续费、充值、提现)
CREATE TABLE journals (
    id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    biz_type    TEXT NOT NULL CHECK (biz_type IN
                ('order_freeze', 'order_unfreeze', 'trade', 'fee', 'deposit', 'withdraw', 'adjust')),
    biz_id      TEXT NOT NULL,
    memo        TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (biz_type, biz_id)
);

-- 分录:每条 journal 至少两条,借贷必平;balance_after 为审计快照
CREATE TABLE ledger_entries (
    id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    journal_id    BIGINT NOT NULL REFERENCES journals(id),
    account_id    BIGINT NOT NULL REFERENCES accounts(id),
    direction     TEXT NOT NULL CHECK (direction IN ('debit', 'credit')),
    amount        BIGINT NOT NULL CHECK (amount > 0),
    currency      TEXT NOT NULL,
    balance_after BIGINT NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_ledger_entries_journal ON ledger_entries(journal_id);
CREATE INDEX idx_ledger_entries_account ON ledger_entries(account_id, id);

-- 不变式 2 校验:正常必须返回 0 行;非 0 行 = 账实不符,立即熔断告警
-- 推导公式按账户类别:资产类 = Σ(debit - credit);其余 = Σ(credit - debit)
CREATE VIEW v_balance_mismatch AS
SELECT a.id AS account_id, a.balance, COALESCE(d.derived, 0) AS derived
FROM accounts a
LEFT JOIN (
    SELECT le.account_id,
           SUM(
               CASE
                   WHEN a2.owner_type = 'system' THEN
                       CASE WHEN le.direction = 'debit' THEN le.amount ELSE -le.amount END
                   ELSE
                       CASE WHEN le.direction = 'credit' THEN le.amount ELSE -le.amount END
               END
           ) AS derived
    FROM ledger_entries le
    JOIN accounts a2 ON a2.id = le.account_id
    GROUP BY le.account_id
) d ON d.account_id = a.id
WHERE a.balance <> COALESCE(d.derived, 0);

COMMIT;
