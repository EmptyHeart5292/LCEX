# 全局错误码

## 响应结构

```json
{"code": 51001, "message": "insufficient balance", "data": null}
```

HTTP 状态码与业务码同时返回:4xx = 客户端错误,5xx = 服务端错误;
`data` 为可选的结构化补充信息(如限频后的重试等待毫秒数)。

## 通用(1xxxx)

| code | HTTP | message | 说明 |
|---|---|---|---|
| 10001 | 500 | internal error | 服务端内部错误 |
| 10002 | 503 | service unavailable | 依赖服务不可用/降级中 |
| 10003 | 400 | invalid request | 参数缺失或格式非法 |

## 鉴权与限频(2xxxx)

| code | HTTP | message | 说明 |
|---|---|---|---|
| 20001 | 401 | invalid api key | API Key 不存在或已禁用 |
| 20002 | 401 | invalid signature | 签名不匹配 |
| 20003 | 401 | timestamp out of recv window | 时间戳偏差过大 |
| 20004 | 403 | ip not allowed | IP 白名单不通过 |
| 20005 | 403 | permission denied | API Key 权限不足(如 read key 下单) |
| 20101 | 429 | too many requests | 触发限频 |
| 20102 | 400 | duplicate client order id | clientOrderId 已存在且参数不同 |

## 交易(5xxxx)

| code | HTTP | message | 说明 |
|---|---|---|---|
| 50001 | 400 | unknown market | 交易对不存在或已下架 |
| 50002 | 400 | market halted | 交易对暂停交易 |
| 50003 | 400 | invalid price precision | 价格精度超限 |
| 50004 | 400 | invalid qty precision | 数量精度超限 |
| 50005 | 400 | qty below minimum | 数量低于最小值 |
| 50006 | 400 | notional below minimum | 名义额低于最小值 |
| 50007 | 400 | invalid price | 非法价格(≤0 / 限价单缺价格) |
| 50008 | 400 | post only would take | Post-Only 会吃单,拒绝 |
| 50009 | 400 | fok liquidity insufficient | FOK 可成交量不足 |
| 50010 | 404 | order not found | 订单不存在 |
| 50011 | 400 | order not cancelable | 订单已终态,不可撤 |
| 51001 | 400 | insufficient balance | 余额不足(冻结失败) |
| 51002 | 400 | market order qty exceeds limit | 市价单数量超上限 |

## 提现(6xxxx)

| code | HTTP | message | 说明 |
|---|---|---|---|
| 60001 | 400 | amount below minimum withdrawal | 低于最小提现额 |
| 60002 | 400 | address not whitelisted | 提现地址未加入白名单 |
| 60003 | 400 | network unsupported | 网络不支持 |
| 60004 | 400 | withdrawal suspended | 提现暂停(风控/维护) |
| 60005 | 403 | risk rejected | 风控拒绝 |

## WS(7xxxx)

| code | message | 说明 |
|---|---|---|
| 70001 | invalid login | WS 认证失败 |
| 70002 | login expired | 认证重放/过期 |
| 70003 | too many channels | 超出单连接订阅上限 |
| 70004 | invalid channel | 频道不存在 |

## 约定

- 新增错误码必须先在本文件登记再实现;
- 同类错误不得重复占用已分配号段。
