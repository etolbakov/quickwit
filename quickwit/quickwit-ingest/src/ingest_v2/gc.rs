// Copyright (C) 2023 Quickwit, Inc.
//
// Quickwit is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at hello@quickwit.io.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use mrecordlog::error::DeleteQueueError;
use once_cell::sync::Lazy;
use quickwit_proto::ingest::IngestV2Result;
use quickwit_proto::metastore::{DeleteShardsRequest, DeleteShardsSubrequest};
use quickwit_proto::{split_queue_id, IndexUid, QueueId, ShardId, SourceId};
use tokio::sync::{RwLock, Semaphore};
use tracing::{error, info};

use super::ingest_metastore::IngestMetastore;
use super::ingester::IngesterState;

/// Period of time after which shards candidate for deletion are actually deleted.
///
/// A shard is candidate for deletion when it is closed, its publish position greater or equal than
/// its replication position, and the grace period has elapsed.
const GRACE_PERIOD: Duration = if cfg!(any(test, feature = "testsuite")) {
    Duration::from_millis(10)
} else {
    Duration::from_secs(60 * 10)
};

/// Maximum number of concurrent GC tasks.
const MAX_CONCURRENCY: usize = 3;

/// Limits the number of concurrent GC tasks.
static MAX_CONCURRENCY_SEMAPHORE: Lazy<Arc<Semaphore>> =
    Lazy::new(|| Arc::new(Semaphore::new(MAX_CONCURRENCY)));

/// Deletes shards asynchronously after the grace period has elapsed.
///
/// 1. deletes the shards from the metastore
/// 2. removes the shards from in-memory data structures
/// 3. deletes the associated mrecordlog queues
pub(super) struct GcTask {
    metastore: Arc<dyn IngestMetastore>,
    state: Arc<RwLock<IngesterState>>,
    gc_candidates: Vec<QueueId>,
}

impl GcTask {
    pub fn spawn(
        metastore: Arc<dyn IngestMetastore>,
        state: Arc<RwLock<IngesterState>>,
        gc_candidates: Vec<QueueId>,
    ) {
        if gc_candidates.is_empty() {
            return;
        }
        let gc_task = Self {
            metastore,
            state,
            gc_candidates,
        };
        tokio::spawn(gc_task.run());
    }

    async fn run(self) -> IngestV2Result<()> {
        tokio::time::sleep(GRACE_PERIOD).await;

        let _permit = MAX_CONCURRENCY_SEMAPHORE
            .clone()
            .acquire_owned()
            .await
            .expect("the semaphore should be open");

        let mut per_source_shard_ids: HashMap<(IndexUid, SourceId), Vec<ShardId>> = HashMap::new();

        for gc_candidate in &self.gc_candidates {
            let (index_uid, source_id, shard_id) =
                split_queue_id(gc_candidate).expect("queue ID should be well-formed");
            per_source_shard_ids
                .entry((index_uid, source_id))
                .or_default()
                .push(shard_id);
        }
        let delete_shards_subrequests = per_source_shard_ids
            .into_iter()
            .map(
                |((index_uid, source_id), shard_ids)| DeleteShardsSubrequest {
                    index_uid: index_uid.into(),
                    source_id,
                    shard_ids,
                },
            )
            .collect();
        let delete_shards_request = DeleteShardsRequest {
            subrequests: delete_shards_subrequests,
            force: false,
        };
        if let Err(error) = self.metastore.delete_shards(delete_shards_request).await {
            error!("failed to delete shards: {}", error);
            return Err(error);
        }
        let mut state_guard = self.state.write().await;

        for gc_candidate in &self.gc_candidates {
            if state_guard.primary_shards.remove(gc_candidate).is_none() {
                state_guard.replica_shards.remove(gc_candidate);
            }
        }
        drop(state_guard);

        let mut state_guard = self.state.write().await;

        for gc_candidate in &self.gc_candidates {
            if let Err(DeleteQueueError::IoError(error)) =
                state_guard.mrecordlog.delete_queue(gc_candidate).await
            {
                error!("failed to delete mrecordlog queue: {}", error);
            }
        }
        info!("deleted {} shard(s)", self.gc_candidates.len());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use mrecordlog::MultiRecordLog;
    use quickwit_proto::ingest::ShardState;
    use quickwit_proto::metastore::DeleteShardsResponse;
    use quickwit_proto::queue_id;

    use super::*;
    use crate::ingest_v2::ingest_metastore::MockIngestMetastore;
    use crate::ingest_v2::models::{PrimaryShard, ReplicaShard};

    #[tokio::test]
    async fn test_gc_task() {
        let mut mock_metastore = MockIngestMetastore::default();
        mock_metastore
            .expect_delete_shards()
            .once()
            .returning(|request| {
                assert_eq!(request.subrequests.len(), 1);

                assert_eq!(request.subrequests[0].index_uid, "test-index:0");
                assert_eq!(request.subrequests[0].source_id, "test-source");
                assert_eq!(request.subrequests[0].shard_ids, [0, 1]);

                let response = DeleteShardsResponse {};
                Ok(response)
            });
        let metastore = Arc::new(mock_metastore);

        let queue_id_0 = queue_id("test-index:0", "test-source", 0);
        let queue_id_1 = queue_id("test-index:0", "test-source", 1);

        let tempdir = tempfile::tempdir().unwrap();
        let mut mrecordlog = MultiRecordLog::open(tempdir.path()).await.unwrap();

        for queue_id in [&queue_id_0, &queue_id_1] {
            mrecordlog.create_queue(queue_id).await.unwrap();
        }

        let mut state = IngesterState {
            mrecordlog,
            primary_shards: HashMap::new(),
            replica_shards: HashMap::new(),
            replication_clients: HashMap::new(),
            replication_tasks: HashMap::new(),
        };
        let primary_shard_0 = PrimaryShard::for_test(
            Some("test-ingester-1"),
            ShardState::Closed,
            12,
            12,
            Some(12),
        );
        state
            .primary_shards
            .insert(queue_id_0.clone(), primary_shard_0);

        let replica_shard_1 = ReplicaShard::for_test("test-ingester-1", ShardState::Closed, 42, 42);
        state
            .replica_shards
            .insert(queue_id_1.clone(), replica_shard_1);

        let state = Arc::new(RwLock::new(state));

        let gc_task = GcTask {
            metastore,
            state: state.clone(),
            gc_candidates: vec![queue_id_0, queue_id_1],
        };
        gc_task.run().await.unwrap();

        let state_guard = state.read().await;
        assert!(state_guard.primary_shards.is_empty());
        assert!(state_guard.replica_shards.is_empty());

        let state_guard = state.read().await;
        assert_eq!(state_guard.mrecordlog.list_queues().count(), 0);
    }
}
