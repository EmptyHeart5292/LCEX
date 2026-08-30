# api-spec

前后端与多端的 API 契约:**单一事实源(single source of truth)**。
apps/web、apps/mobile、apps/admin 与后端服务都以此生成本地类型/客户端代码。

## 内容

- `rest/`:OpenAPI 文件
  - `public.yaml` 行情等公共接口
  - `private.yaml` 交易/资产私有接口(HMAC 签名)
  - `admin.yaml` 运营后台接口
- `ws/`:WebSocket 协议文档
  - 公共频道:depth / ticker / trades / kline
  - 私有频道:order / balance
  - 全局 sequence、断线重连与补齐规则
- `errors.md`:全局错误码表
- `currencies.yaml`:币种精度、充值网络配置
- `markets.yaml`:交易对参数(价格/数量精度、最小名义额、费率)

## 纪律

契约变更走 review,作为前后端协作的接口协议;代码与契约不一致视为 bug。
