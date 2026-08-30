//! cex-runner:撮合引擎 Kafka 接线进程。
//! 每交易对一个 task;SIGINT/SIGTERM 优雅退出。

use std::sync::Arc;

use cex_runner::{config::RunnerConfig, worker};
use tokio::sync::watch;
use tracing::info;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let cfg = Arc::new(RunnerConfig::from_env());
    info!(
        brokers = %cfg.brokers,
        symbols = ?cfg.symbols,
        prefix = %cfg.topic_prefix,
        "cex matching runner starting"
    );

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut handles = Vec::new();
    for symbol in cfg.symbols.clone() {
        let cfg = Arc::clone(&cfg);
        let rx = shutdown_rx.clone();
        handles.push(tokio::spawn(worker::run_symbol(cfg, symbol, rx)));
    }

    wait_for_shutdown().await;
    info!("shutdown signal received, draining workers");
    let _ = shutdown_tx.send(true);
    for h in handles {
        let _ = h.await;
    }
    info!("cex matching runner stopped");
}

#[cfg(unix)]
async fn wait_for_shutdown() {
    use tokio::signal::unix::{signal, SignalKind};
    let mut term = signal(SignalKind::terminate()).expect("install SIGTERM handler");
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = term.recv() => {}
    }
}

#[cfg(not(unix))]
async fn wait_for_shutdown() {
    let _ = tokio::signal::ctrl_c().await;
}
