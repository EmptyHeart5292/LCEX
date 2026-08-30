# ADR-008:web MVP 采用静态 SPA + Go 迷你网关,Next.js 延后

- 状态:已确认(2026-08-31)

## 背景

apps/web 原拟定 Next.js + React 18。核心交易闭环后端已就绪,PC 端需要一个立即可用、
可验收的交易页;Next.js 工程化(构建链、依赖体积、SSR)在当前阶段收益有限。

## 决策

1. MVP 采用**无构建静态 SPA**(vanilla JS + CSS)+ **Go 迷你网关**:
   网关做静态托管 + order/market 反向代理(含 WS),单一 origin 消除 CORS;
2. 所有数据访问严格走 `packages/api-spec` 契约(REST + WS 协议),不引入服务端渲染依赖;
3. UI 复杂化或需要 SSR/组件体系时迁移 Next.js——迁移只动 apps/web 前端层,契约与后端零改动。

## 后果

- 收益:零构建链、秒级改版、验收只依赖 curl/WS 探针;网关即未来 api-gateway 的雏形;
- 代价:无组件化/类型检查,页面逻辑增长后维护成本上升(触发迁移的信号);
- 迁移触发条件(满足其一):页面数 >5、需要 SSR/SEO、团队要求组件体系。
