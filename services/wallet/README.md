# wallet

钱包服务:托管方案对接层 + 充提闭环。决策见 ADR-002。

## 当前(Phase 2 起步)

- **mock Provider**:`POST /internal/deposits` 走账本 `deposit` 分录
  (debit 热钱包 `system` + credit 用户 `available`),`bizId` 幂等。
- 真托管(Fireblocks/Cobo)、扫链确认、提现广播、归集、三方对账尚未接入;
  入账路径不变,后续只替换 Provider。

```bash
curl -X POST http://localhost:8084/internal/deposits \
  -H 'Content-Type: application/json' \
  -d '{"userId":9001,"currency":"USDT","amount":"1000000","bizId":"dep-9001-usdt"}'
```

默认 `:8084`。验收:`scripts/e2e-mm.sh`。

## 职责(完整目标)

- 托管商对接(Fireblocks / Cobo):内部 **Provider 接口**(地址派生、签名广播、回调/扫链、归集);
- 充值:地址生成 → 扫链/回调 → 确认数达标 → 幂等入账;
- 提现:受理 → risk → 大额 4-eyes → 托管签名广播 → 链上确认;
- 归集与每日三方对账。

## MVP 网络范围

Bitcoin、Ethereum(ERC-20)、Solana(见 ADR-003);TRON 二期。
