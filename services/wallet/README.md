# wallet

钱包服务:托管方案对接层 + 充提闭环。决策见 ADR-002。

## 职责

- 托管商对接(Fireblocks / Cobo):内部定义 **Provider 接口**(地址派生、签名广播、回调/扫链、归集),供应商差异不外泄;
- 充值:充值地址生成 → 扫链/回调监听 → 确认数达标 → 幂等入账(UTXO 按 txid+vout,EVM 按 txid+合约转账索引,Solana 按 txid+账户索引);
- 提现:受理 → risk 规则 → 大额人工 4-eyes 审核(Admin)→ 托管签名广播 → 链上确认闭环;
- 归集:热钱包余额超阈值自动归集;
- 对账:每日三方对账——链上余额 vs 内部账本 vs 充提记录,差异触发最高级告警。

## MVP 网络范围

Bitcoin、Ethereum(ERC-20)、Solana(见 ADR-003);TRON 二期。

## 依赖

PostgreSQL、Kafka、托管商 API、risk、Admin 审核操作。
