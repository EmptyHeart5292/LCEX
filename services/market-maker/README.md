# market-maker

内部做市服务:为新平台盘口提供流动性并锚定外部价格。语言决策见 ADR-007(Go)。

## 当前(Phase 2 起步)

- 每交易对一个 goroutine;HTTP 拉 `price-index`,经 **order 服务** 挂 post-only 双边限价单(不旁路)。
- 价差:`CEX_MM_HALF_SPREAD_BPS`(默认 10bps 每边);指数移动或不成交档位则撤再挂。
- `POST /pause` / `POST /resume` 为本地 kill switch。
- 尚未做:多档、库存偏移、外部所对冲、接 risk。

默认用户 `CEX_MM_USER_ID=9001`,需先经 wallet 入账。验收:`scripts/e2e-mm.sh`。

## 策略(完整目标)

- 以 price-index 指数价为锚,双边挂单(买卖各 N 档);
- 刷新:价格移动超阈值、超时、部分成交后 cancel/replace;
- 库存上限与外部所对冲保持中性。

## 硬性约束

全部做市单走 order → matching → clearing,不提供旁路。
