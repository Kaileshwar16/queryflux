use std::collections::HashMap;
use std::sync::{Arc, Barrier};

use queryflux_cluster_manager::{
    cluster_state::ClusterState,
    simple::SimpleClusterGroupManager,
    strategy::{ClusterCandidate, ClusterSelectionStrategy, RoundRobinStrategy},
    ClusterGroupManager,
};
use queryflux_core::query::{ClusterGroupName, ClusterName, EngineType};

const WORKERS: usize = 32;

fn manager(
    cluster_count: usize,
    strategy: Arc<dyn ClusterSelectionStrategy>,
) -> (Arc<SimpleClusterGroupManager>, ClusterGroupName) {
    let group = ClusterGroupName("capacity-test".to_string());
    let clusters = (0..cluster_count)
        .map(|i| {
            Arc::new(ClusterState::new(
                ClusterName(format!("cluster-{i}")),
                group.clone(),
                None,
                None,
                EngineType::Trino,
                None,
                1,
                true,
            ))
        })
        .collect();
    let groups = HashMap::from([(group.clone(), (clusters, strategy))]);
    (Arc::new(SimpleClusterGroupManager::new(groups)), group)
}

async fn contend(
    manager: &Arc<SimpleClusterGroupManager>,
    group: &ClusterGroupName,
) -> Vec<ClusterName> {
    let start = Arc::new(tokio::sync::Barrier::new(WORKERS));
    let mut tasks = Vec::new();
    for _ in 0..WORKERS {
        let manager = Arc::clone(manager);
        let group = group.clone();
        let start = Arc::clone(&start);
        tasks.push(tokio::spawn(async move {
            start.wait().await;
            manager.acquire_cluster(&group).await.unwrap()
        }));
    }

    let mut acquired = Vec::new();
    for task in tasks {
        if let Some(cluster) = task.await.unwrap() {
            acquired.push(cluster);
        }
    }
    // Keep every successful reservation until ALL competing calls have returned.
    acquired
}

/// Exercise the real inline strategy on multiple runtime workers. A start barrier
/// increases contention but does not force the old check/increment interleaving;
/// repeated rounds make this a stress regression, not a proof of atomicity.
#[tokio::test(flavor = "multi_thread", worker_threads = 32)]
async fn concurrent_acquire_respects_capacity_one() {
    let (manager, group) = manager(1, Arc::new(RoundRobinStrategy::new()));
    for round in 0..256 {
        let acquired = contend(&manager, &group).await;
        assert_eq!(acquired.len(), 1, "oversubscribed in round {round}");
        assert_eq!(
            manager.all_cluster_states().await.unwrap()[0].running_queries,
            1
        );
        assert!(manager.acquire_cluster(&group).await.unwrap().is_none());
        manager.release_cluster(&group, &acquired[0]).await.unwrap();
        assert_eq!(
            manager.all_cluster_states().await.unwrap()[0].running_queries,
            0
        );
    }
}

/// Hold every strategy invocation until all callers have snapshotted both
/// clusters. Every caller selects the SECOND cluster; losers must reserve the
/// first cluster or return None, without double-reserving either cluster.
struct ContendedSecondCandidate {
    ready: Barrier,
}

impl ClusterSelectionStrategy for ContendedSecondCandidate {
    fn requires_blocking_dispatch(&self) -> bool {
        true
    }

    fn pick(&self, candidates: &[ClusterCandidate<'_>]) -> Option<usize> {
        assert_eq!(candidates.len(), 2);
        self.ready.wait();
        Some(1)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 32)]
async fn concurrent_acquire_falls_back_after_selected_cluster_fills() {
    let (manager, group) = manager(
        2,
        Arc::new(ContendedSecondCandidate {
            ready: Barrier::new(WORKERS),
        }),
    );
    let mut acquired = contend(&manager, &group).await;
    acquired.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(
        acquired,
        vec![
            ClusterName("cluster-0".to_string()),
            ClusterName("cluster-1".to_string()),
        ]
    );
    let states = manager.all_cluster_states().await.unwrap();
    assert!(states.iter().all(|state| state.running_queries == 1));
    for cluster in &acquired {
        manager.release_cluster(&group, cluster).await.unwrap();
    }
    assert!(manager
        .all_cluster_states()
        .await
        .unwrap()
        .iter()
        .all(|state| state.running_queries == 0));
}
