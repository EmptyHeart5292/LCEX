# 版本与发布规范

## 语义化版本(SemVer):`vMAJOR.MINOR.PATCH`

- 1.0 之前使用 `0.x.y`:允许破坏性变更;MINOR = 里程碑,PATCH = 修复;
- **v1.0.0 = MVP 灰度上线**(Phase 4 验收后);此后 MAJOR = 破坏性变更(API 契约/协议/账本格式),MINOR = 向后兼容新功能,PATCH = 修复。

## Tag 流程

1. 里程碑完成 → 提交推送 main;
2. 打 annotated tag 并推送:`git tag -a v0.x.0 -m "<交付物清单>"` → `git push origin v0.x.0`;
3. tag message 即 release notes:列出交付物、已知限制、破坏性变更;
4. 破坏性变更(API 契约、撮合协议、账本格式)必须在其 ADR 或本文件下记录迁移方式。

## 里程碑与 tag 对照

| Tag | 交付物 | 状态 |
|---|---|---|
| v0.1.0 | Phase 0:账本 schema + 撮合引擎核心(含测试) | 已交付 |
| v0.2.0 | API 契约 v1 + 撮合 Kafka runner | 已交付 |
| v0.3.0 | Phase 1:核心交易闭环(account/order/clearing/market/price-index + web 交易页) | 已交付 |
| v0.3.1 | PATCH:撮合崩溃恢复(先产出再提交 + 指令 log 重放);行情 REST 信封对齐;移除误入库二进制 | 已交付 |
| v0.4.0 | Phase 2:资金闭环(托管钱包、充提、对账)+ market-maker | 规划 |
| v1.0.0 | MVP 灰度上线 | 规划 |

## v0.3.1 迁移

公共行情 REST(`GET /api/v1/depth|tickers|trades|klines`)成功体由裸对象/数组改为统一信封 `{"code":0,"data":...}`(与 `errors.md`、order 服务、交易页 `api()` 对齐)。原先直接解析响应根的客户端改为读 `data`。WebSocket 协议不变。
