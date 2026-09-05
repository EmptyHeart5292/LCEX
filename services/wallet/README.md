# wallet

钱包服务:托管方案对接层 + 充提闭环。决策见 ADR-002。

## 当前(Phase 2)

- mock Provider 按 (user, currency, network) **确定性派生**充值地址;
- `GET /api/v1/deposit-addresses` 分配/返回地址;
- `POST /internal/chain/deposits` 模拟扫链:确认数未达标只记 pending,达标后 `deposit` 分录入账(`onchain-{txid}-{index}` 幂等);
- `POST /api/v1/withdrawals` 校验最小额,扣 `amount+fee`(fee 入 fee 账户,本金出热钱包),状态 `broadcasting`;
- `POST /internal/chain/withdrawals/{id}/confirm` 记 txid 并终态 `completed`;
- `POST /internal/deposits` 仍保留给做市/测试直接注资。

真托管(Fireblocks/Cobo)只替换 Provider;入账/扣账路径不变。验收:`scripts/e2e-wallet.sh`。

默认 `:8084`。鉴权占位:`X-User-Id`。
