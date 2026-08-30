# web — PC 主站

当前最高优先级的前端,Phase 1 开始开发。

## 技术栈(拟定)

Next.js(App Router)+ React 18 + TypeScript;状态管理 Zustand;K线图用 TradingView lightweight-charts。

## 页面规划(MVP)

- 首页:市场概览、热门交易对(指数价参考)
- 行情页:交易对列表
- 交易页:K线、盘口、深度图、最新成交、下单面板(限价/市价)、当前委托/历史委托
- 资产:总览、充值、提现、资金流水
- 个人中心:登录注册、2FA、资金密码、API Key、提现地址白名单

## 约定

- 所有接口与 WS 协议以 `packages/api-spec` 为单一事实源,类型自动生成;
- 实时数据:公共 WS(行情)+ 私有 WS(订单/余额),按 sequence 处理断线重连与补齐;
- 盘口过薄时行情展示可回退 price-index 指数数据(可配置,见 docs/architecture.md §2)。

## 目录规划(脚手架时建立)

`src/app` 页面路由、`src/components` 组件、`src/stores` 状态、`src/lib`(WS client、由契约生成的 API client)。
