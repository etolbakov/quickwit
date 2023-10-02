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

use std::sync::Arc;

use async_trait::async_trait;
use quickwit_metastore::Metastore;
use quickwit_proto::ingest::IngestV2Error;
use quickwit_proto::metastore::{
    CloseShardsRequest, CloseShardsResponse, DeleteShardsRequest, DeleteShardsResponse,
};

// TODO: Remove when the metastore is code generated in `quickwit-proto`.

/// Implementation of the [`quickwit_ingest::IngestMetastore`] trait. See comment in the module
/// where it is defined for more details about why this is required.
#[derive(Clone)]
pub(crate) struct IngestMetastore {
    metastore: Arc<dyn Metastore>,
}

impl IngestMetastore {
    pub fn new(metastore: Arc<dyn Metastore>) -> Self {
        Self { metastore }
    }
}

#[async_trait]
impl quickwit_ingest::IngestMetastore for IngestMetastore {
    async fn close_shards(
        &self,
        request: CloseShardsRequest,
    ) -> quickwit_proto::ingest::IngestV2Result<CloseShardsResponse> {
        self.metastore
            .close_shards(request)
            .await
            .map_err(|error| IngestV2Error::Internal(error.to_string()))
    }

    async fn delete_shards(
        &self,
        request: DeleteShardsRequest,
    ) -> quickwit_proto::ingest::IngestV2Result<DeleteShardsResponse> {
        self.metastore
            .delete_shards(request)
            .await
            .map_err(|error| IngestV2Error::Internal(error.to_string()))
    }
}
