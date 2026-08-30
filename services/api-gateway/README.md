# api-gateway

统一接入层:面向 apps/web(及未来的 apps/mobile)暴露 REST 与 WebSocket。

## 职责

- REST 路由与转发,统一响应结构与错误码(契约见 `packages/api-spec`);
- 鉴权:Web 登录态(JWT);开放 API 使用 API Key + HMAC-SHA256 签名校验;
- 限频:按 UID / IP / API Key 分级限频(Redis);
- 幂等:接收并透传 clientOrderId;
- 公共 WS:depth / ticker / trades / kline 广播;
- 私有 WS:订单更新、余额变更推送,按全局 sequence 校验连续性,断线按 sequence 补齐。

## 依赖

Redis(限频、会话)、Kafka(消费成交/订单/余额事件用于推送)、各业务服务 gRPC。
