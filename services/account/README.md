# account

账户服务:用户体系与安全凭证。

## 职责

- 注册/登录(邮箱/手机号)、会话签发;
- 2FA(TOTP)、资金密码、防钓鱼码;
- API Key 管理:生成、权限(read/trade/withdraw)、IP 白名单;
- 用户数据模型预留 `kyc_level` 字段(MVP 恒为 0,见 ADR-005);
- 子账户体系预留(二期)。

## 依赖

PostgreSQL、Redis(会话、2FA 限频)。
