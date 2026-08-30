# web — PC 主站

当前实现:**无构建静态 SPA + Go 迷你网关**(ADR-008)。浏览器访问 :8080 单一 origin:

- `/` 静态页(index.html / app.js / style.css,vanilla JS,零依赖)
- `/api/v1/orders*`、`/api/v1/account/*` → 反代 order 服务
- `/api/v1/depth|tickers|trades|klines`、`/stream`(WS)→ 反代 market 服务

迷你网关消除 CORS 并统一入口,未来演进为完整 api-gateway(鉴权/限频在此落地);
UI 迁移 Next.js 时数据契约不变(packages/api-spec)。

## 交易页功能(已实现)

- 订单簿(买卖各 12 档,seq 连续性校验,断档自动重拉快照)
- 1m K线(canvas 绘制)+ 最新成交(taker 方向着色)
- 限价下单(买/卖)、资产总览(可用/冻结)、当前委托与撤单
- WS 断线指数退避重连、心跳回应、symbol 切换重订阅

## 运行

```bash
go build -o /tmp/cex-web ./apps/web
CEX_WEB_STATIC=apps/web/public /tmp/cex-web   # 默认 :8080,上游 8081/8082
```

MVP 鉴权占位:页面 UID 输入框 → `X-User-Id` 请求头;登录态由网关统一签发后移除。
