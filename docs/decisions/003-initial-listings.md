# ADR-003:首批交易对与网络

- 状态:已确认(2026-08-30)

## 决策

- **交易对**:BTC/USDT、ETH/USDT、SOL/USDT。USDT 为唯一计价货币。
- **充值/提现网络(MVP)**:Bitcoin、Ethereum(ERC-20)、Solana——与三个币种一一对应,最小集。
- 实际支持的币种/网络清单以托管商(Fireblocks/Cobo)支持的资产为准,配置落 `packages/api-spec/currencies.yaml`。

## 后续

- TRON(USDT)用户量大、手续费低,列为二期网络第一优先;
- 其余网络按需扩展,wallet 服务内通过 ChainAdapter 抽象新增。
