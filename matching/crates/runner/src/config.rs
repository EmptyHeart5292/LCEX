//! 运行配置:全部来自环境变量,便于容器化部署。

use std::env;

#[derive(Debug, Clone)]
pub struct RunnerConfig {
    /// Kafka bootstrap servers,如 "localhost:9092"
    pub brokers: String,
    /// 交易对列表,如 ["BTC-USDT", "ETH-USDT", "SOL-USDT"]
    pub symbols: Vec<String>,
    /// topic 前缀,默认 "cex"
    pub topic_prefix: String,
}

impl RunnerConfig {
    pub fn from_env() -> Self {
        let brokers =
            env::var("CEX_KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9092".to_string());
        let symbols =
            parse_symbols(&env::var("CEX_SYMBOLS").unwrap_or_else(|_| {
                "BTC-USDT,ETH-USDT,SOL-USDT".to_string()
            }));
        let topic_prefix = env::var("CEX_TOPIC_PREFIX").unwrap_or_else(|_| "cex".to_string());
        Self {
            brokers,
            symbols,
            topic_prefix,
        }
    }
}

/// 逗号分隔 → 去空白 → 去空项
pub fn parse_symbols(raw: &str) -> Vec<String> {
    raw.split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}
