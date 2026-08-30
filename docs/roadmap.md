# 路线图

| 阶段 | 周期 | 内容 |
|---|---|---|
| Phase 0 | 1–2 周 | 账本/库表 schema 定稿;撮合引擎 PoC 与压测;API 契约 v1(packages/api-spec) |
| Phase 1 | 4–6 周 | 核心交易闭环:account / order / clearing / market / matching + PC 网站交易页(apps/web)+ price-index(做市数据源就绪) |
| Phase 2 | 4–6 周 | 资金闭环:wallet 托管对接(Fireblocks/Cobo)、Bitcoin/ERC-20/Solana 充提与对账;market-maker 上线(内测流动性) |
| Phase 3 | 3–4 周 | Admin 后台(apps/admin)、risk 规则引擎完善、安全加固 |
| Phase 4 | 2–4 周 | 全链路压测、安全审计、故障演练(撮合重放、账本重放校验)、灰度上线 |
| 二期 | 持续 | 合约/杠杆、TRON(USDT)网络、子账户、移动端 APP(apps/mobile)、更多交易对与链 |

依赖关系:

- Phase 2 的 market-maker 依赖 Phase 1 的 order/matching 闭环与 price-index;
- 公开灰度(Phase 4)前,market-maker 必须稳定运行,否则盘口无流动性;
- 移动端启动的前提是 packages/api-spec 契约冻结,预期零后端改动接入。
