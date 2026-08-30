# LCEX

从 0 构建的中心化加密货币交易所(CEX)。当前状态:骨架阶段(Phase 1 筹备)。

## 已确认的关键决策

| 决策项 | 结论 | 记录 |
|---|---|---|
| 撮合引擎 | Rust,单交易对单线程,事件溯源,可重放 | docs/decisions/001 |
| 钱包 | 托管方案(Fireblocks / Cobo,MPC),不自建私钥体系 | docs/decisions/002 |
| 首批交易对 | BTC/USDT、ETH/USDT、SOL/USDT;MVP 网络:Bitcoin、ERC-20、Solana | docs/decisions/003 |
| 端 | PC 网站先行;移动端 APP 后续,目录已预留 | docs/decisions/004 |
| KYC | MVP 不接入,账户模型预留 kyc_level 字段 | docs/decisions/005 |
| 做市 | 自建内部做市,对齐 Binance/OKX/Bybit/MEXC/Bitget 价格 | docs/decisions/006 |
| MVP 范围 | 仅现货;合约/杠杆为二期 | docs/roadmap.md |

## 目录结构

```
.
├── apps/                  # 前端应用
│   ├── web/               # PC 网站(当前主线,Phase 1 开发)
│   ├── mobile/            # 移动端 APP(预留,暂不开发)
│   └── admin/             # 运营后台(Phase 3)
├── services/              # 后端微服务(Go)
│   ├── api-gateway/       # 接入层:REST/WS、鉴权、限频、推送
│   ├── account/           # 用户、2FA、API Key
│   ├── order/             # 下单校验、资金冻结、订单生命周期
│   ├── clearing/          # 清算、复式记账、手续费
│   ├── market/            # 行情:深度/ticker/K线
│   ├── price-index/       # 外部行情聚合、指数价
│   ├── market-maker/      # 内部做市、外部对冲
│   ├── wallet/            # 托管对接、充值提现、归集、对账
│   └── risk/              # 风控规则、熔断
├── matching/              # 撮合引擎(Rust workspace)
├── packages/
│   ├── api-spec/          # API 契约:REST/WS/错误码/交易对配置(单一事实源)
│   └── exchange-connector/ # 外部交易所连接器库(price-index/market-maker 复用)
├── docs/
│   ├── architecture.md    # 总体架构
│   ├── roadmap.md         # 路线图
│   └── decisions/         # 关键决策记录(ADR)
├── infra/
│   └── docker-compose.yml # 本地开发依赖(Kafka/PostgreSQL/Redis/ClickHouse)
└── scripts/               # 开发运维脚本(待补充)
```

## 本地开发

```bash
docker compose -f infra/docker-compose.yml up -d
```

依赖与端口说明见 `infra/README.md`。

## 当前优先级

1. **PC 网站(apps/web)+ 后端核心交易闭环(services + matching)** —— 见 docs/roadmap.md
2. 移动端暂不开发;后端能力全部经 packages/api-spec 契约暴露,保证移动端后续零改动接入
