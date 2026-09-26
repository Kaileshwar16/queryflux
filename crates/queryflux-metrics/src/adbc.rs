//! Process-wide ADBC pool metrics, shared by adapters and the /metrics registry.
use std::sync::LazyLock;

use prometheus::{IntCounter, IntCounterVec, IntGauge, IntGaugeVec, Opts, Registry};

static POOLS: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    IntGaugeVec::new(
        Opts::new(
            "queryflux_adbc_scoped_pools",
            "Cached ADBC scoped sub-pools, including expired entries awaiting cleanup",
        ),
        &["cluster_group", "cluster_name"],
    )
    .expect("valid ADBC gauge descriptor")
});
static EVICTIONS: LazyLock<IntCounterVec> = LazyLock::new(|| {
    IntCounterVec::new(
        Opts::new(
            "queryflux_adbc_scoped_pool_evictions_total",
            "ADBC scoped sub-pool evictions",
        ),
        &["cluster_group", "cluster_name", "reason"],
    )
    .expect("valid ADBC counter descriptor")
});

pub(crate) fn register(registry: &Registry) -> Result<(), prometheus::Error> {
    registry.register(Box::new(POOLS.clone()))?;
    registry.register(Box::new(EVICTIONS.clone()))
}

/// Counts are adjusted by each cache, so overlapping adapter generations during a
/// config reload do not overwrite one another. Labels never contain identity data.
pub struct ScopedPoolMetrics {
    pub pools: IntGauge,
    pub idle_evictions: IntCounter,
    pub lru_evictions: IntCounter,
}

impl ScopedPoolMetrics {
    pub fn new(group: &str, cluster: &str) -> Self {
        Self {
            pools: POOLS.with_label_values(&[group, cluster]),
            idle_evictions: EVICTIONS.with_label_values(&[group, cluster, "idle"]),
            lru_evictions: EVICTIONS.with_label_values(&[group, cluster, "lru"]),
        }
    }
}
