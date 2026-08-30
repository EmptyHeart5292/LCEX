# WebSocket 协议 v0.2.0

端点:`wss://<host>/stream`(本地 `ws://localhost:8080/stream`)。
帧格式为 JSON 文本帧;所有时间戳为毫秒。

## 通用规则

- **seq 连续性**:`trade` / `depth` / 私有 `order` / `balance` 事件携带 `seq`(撮合输出全局序号,单交易对内连续)。客户端发现 `seq` 跳号时:丢弃本地状态 → 重新拉 REST 快照(depth 用 `/api/v1/depth`、私有数据用对应查询接口)→ 重新应用增量;
- **心跳**:服务端每 20s 发 `{"channel":"ping"}`,客户端须在 30s 内回复 `{"op":"pong"}`;客户端亦可主动 ping;
- **订阅配额**:单连接最多 100 个频道;私有频道需先认证。

## 请求(客户端 → 服务端)

```json
{"op": "subscribe",   "args": ["ticker@BTC-USDT", "depth@BTC-USDT@50", "trades@BTC-USDT", "kline@BTC-USDT@1m"]}
{"op": "unsubscribe", "args": ["depth@BTC-USDT@50"]}
{"op": "login", "apiKey": "...", "timestamp": 1756500000000, "signature": "hex(hmac_sha256(secret, \"LCEX-WS\" + timestamp))"}
```

- `login` 成功后自动开通私有频道 `order` / `balance`(无需单独订阅);
- WS 签名不携带路径,固定明文前缀 `LCEX-WS` + timestamp,防重放窗口 30s。

## 应答与事件(服务端 → 客户端)

订阅确认:

```json
{"event": "subscribe", "args": ["depth@BTC-USDT@50"], "code": 0}
{"event": "error", "code": 60007, "message": "invalid channel"}
```

### ticker@{symbol}

```json
{"channel": "ticker", "symbol": "BTC-USDT", "data": {"last": "50000.00000000", "bid": "49999.50000000", "ask": "50000.50000000", "high24h": "51000.00000000", "low24h": "49000.00000000", "volume24h": "123.45678901", "changePct24h": "1.25", "ts": 1756500000000}}
```

### depth@{symbol}@{limit}

首推为快照,其后为增量:

```json
{"channel": "depth", "symbol": "BTC-USDT", "type": "snapshot", "seq": 1000, "bids": [["49999.00000000", "1.50000000"]], "asks": [["50001.00000000", "2.00000000"]]}
{"channel": "depth", "symbol": "BTC-USDT", "type": "update", "seq": 1001, "bids": [["49999.00000000", "0"]], "asks": []}
```

增量合并规则:数量为 `"0"` 表示删除该价位;同价位覆盖。

### trades@{symbol}

```json
{"channel": "trades", "symbol": "BTC-USDT", "data": {"tradeId": 90001, "seq": 1002, "price": "50000.00000000", "qty": "0.10000000", "side": "bid", "ts": 1756500000000}}
```

### kline@{symbol}@{interval}

```json
{"channel": "kline", "symbol": "BTC-USDT", "interval": "1m", "data": {"start": 1756499400000, "end": 1756499459999, "open": "50000.00000000", "high": "50010.00000000", "low": "49995.00000000", "close": "50005.00000000", "volume": "12.34567890"}}
```

### order(私有)

```json
{"channel": "order", "symbol": "BTC-USDT", "data": {"orderId": 100001, "clientOrderId": "web-1", "seq": 1003, "status": "partially_filled", "price": "50000.00000000", "filledQty": "0.05000000", "qty": "0.10000000", "ts": 1756500000000}}
```

### balance(私有)

```json
{"channel": "balance", "data": {"currency": "USDT", "seq": 1004, "available": "999.70000000", "frozen": "30.00000000", "ts": 1756500000000}}
```

## 断线重连

1. 指数退避重连(1s 起,上限 30s);
2. 重连后重新 subscribe 全部频道(私有频道需重新 login);
3. 按 seq 断档规则重新拉快照补齐。
