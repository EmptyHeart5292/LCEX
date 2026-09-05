# risk

风控服务:同步规则 + kill switch。

## 当前

- `POST /v1/withdraw/check`:提现地址黑名单(`CEX_RISK_DENY_ADDRESSES`)、UTC 日限额(`CEX_RISK_DAILY_WITHDRAW=BTC:0.002`)、提现暂停。
- `GET /v1/status` / `POST /v1/pause|resume`:withdraw / mm / trading 开关。做市每 tick 拉 status,mmPaused 则撤单停报价。
- wallet 在扣账前调用 check;未配置 `CEX_RISK_URL` 时跳过(本地脚本兼容)。

尚未做:下单限额、Chainalysis、刷量对敲、Admin UI。

默认 `:8086`。验收:`scripts/e2e-risk.sh`。
