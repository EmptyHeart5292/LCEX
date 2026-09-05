# market-maker

内部做市服务:为新平台盘口提供流动性并锚定外部价格。语言决策见 ADR-007(Go)。

## 当前(Phase 2)

- 每交易对一个 goroutine;HTTP 拉指数,经 **order 服务** 挂 post-only 多档限价单。
- `CEX_MM_LEVELS`(默认 3)+ `CEX_MM_HALF_SPREAD_BPS`;库存偏离则整格偏移(`CEX_MM_MAX_SKEW_BPS`)。
- 超过 `CEX_MM_MAX_OFFSET_BPS` 的档不挂。对冲走 `packages/exchange-connector` mock;通道不健康则停报价。
- `GET /status`,`POST /pause` / `POST /resume`。

尚未做:真实所 HMAC 对冲、接 risk。验收:`scripts/e2e-mm.sh`。

## 硬性约束

全部做市单走 order → matching → clearing,不提供旁路。
