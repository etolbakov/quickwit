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

pub mod file_backed_metastore;
pub(crate) mod index_metadata;
#[cfg(feature = "postgres")]
pub mod postgresql_metastore;
#[cfg(feature = "postgres")]
mod postgresql_model;

use std::ops::{Bound, RangeInclusive};

pub use index_metadata::IndexMetadata;
use itertools::Itertools;
use quickwit_config::{IndexConfig, SourceConfig};
use quickwit_doc_mapper::tag_pruning::TagFilterAst;
use quickwit_proto::metastore::{
    serde_utils, AddSourceRequest, CreateIndexRequest, DeleteTask, IndexMetadataResponse,
    ListIndexesMetadatasRequest, ListIndexesMetadatasResponse, ListSplitsRequest,
    ListSplitsResponse, MetastoreError, MetastoreResult, StageSplitsRequest,
};
use quickwit_proto::IndexUid;
use time::OffsetDateTime;

use crate::{Split, SplitMetadata, SplitState};

/// A query object for listing indexes stored in the metastore.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum ListIndexesQuery {
    /// Matches all indexes.
    All,
    /// List of index ID patterns.
    /// A pattern can contain the wildcard character `*`.
    IndexIdPatterns(Vec<String>),
}

pub trait ListIndexesMetadataRequestExt {
    fn try_from_list_indexes_query(
        list_indexes_query: ListIndexesQuery,
    ) -> MetastoreResult<ListIndexesMetadatasRequest>;

    fn all() -> ListIndexesMetadatasRequest;

    fn deserialize_list_indexes_query(&self) -> MetastoreResult<ListIndexesQuery>;
}

impl ListIndexesMetadataRequestExt for ListIndexesMetadatasRequest {
    fn try_from_list_indexes_query(
        list_indexes_query: ListIndexesQuery,
    ) -> MetastoreResult<ListIndexesMetadatasRequest> {
        let query_json = serde_json::to_string(&list_indexes_query).map_err(|error| {
            MetastoreError::JsonSerializeError {
                struct_name: "ListIndexesQuery".to_string(),
                message: format!("Failed to serialize list indexes query: {error:?}"),
            }
        })?;
        Ok(Self { query_json })
    }

    fn all() -> ListIndexesMetadatasRequest {
        Self::try_from_list_indexes_query(ListIndexesQuery::All).expect("should never fail")
    }

    fn deserialize_list_indexes_query(&self) -> MetastoreResult<ListIndexesQuery> {
        serde_json::from_str(&self.query_json).map_err(|error| {
            MetastoreError::JsonDeserializeError {
                struct_name: "ListIndexesQuery".to_string(),
                message: format!("Failed to deserialize list indexes query: {error:?}"),
            }
        })
    }
}

pub trait CreateIndexRequestExt {
    fn try_from_index_config(index_config: IndexConfig) -> MetastoreResult<CreateIndexRequest>;

    fn deserialize_index_config(&self) -> MetastoreResult<IndexConfig>;
}

impl CreateIndexRequestExt for CreateIndexRequest {
    fn try_from_index_config(index_config: IndexConfig) -> MetastoreResult<CreateIndexRequest> {
        let index_config_json = serde_json::to_string(&index_config).map_err(|error| {
            MetastoreError::JsonSerializeError {
                struct_name: "IndexConfig".to_string(),
                message: format!("Failed to serialize index config: {error:?}"),
            }
        })?;
        let request = Self { index_config_json };
        Ok(request)
    }

    fn deserialize_index_config(&self) -> MetastoreResult<IndexConfig> {
        serde_json::from_str(&self.index_config_json).map_err(|error| {
            MetastoreError::JsonDeserializeError {
                struct_name: "IndexConfig".to_string(),
                message: format!("Failed to deserialize index config: {error:?}"),
            }
        })
    }
}

pub trait IndexMetadataResponseExt {
    fn try_from_index_metadata(
        index_metadata: IndexMetadata,
    ) -> MetastoreResult<IndexMetadataResponse>;

    fn deserialize_index_metadata(&self) -> MetastoreResult<IndexMetadata>;
}

impl IndexMetadataResponseExt for IndexMetadataResponse {
    fn try_from_index_metadata(index_metadata: IndexMetadata) -> MetastoreResult<Self> {
        let index_metadata_serialized_json =
            serde_json::to_string(&index_metadata).map_err(|error| {
                MetastoreError::JsonSerializeError {
                    struct_name: "IndexMetadata".to_string(),
                    message: format!("Failed to serialize index metadata: {error:?}"),
                }
            })?;
        let request = Self {
            index_metadata_serialized_json,
        };
        Ok(request)
    }

    fn deserialize_index_metadata(&self) -> MetastoreResult<IndexMetadata> {
        serde_json::from_str(&self.index_metadata_serialized_json).map_err(|error| {
            MetastoreError::JsonDeserializeError {
                struct_name: "IndexMetadata".to_string(),
                message: format!("Failed to deserialize index metadata: {error:?}"),
            }
        })
    }
}

pub trait ListIndexesResponseExt {
    fn try_from_indexes_metadata(
        indexes_metadata: impl IntoIterator<Item = IndexMetadata>,
    ) -> MetastoreResult<ListIndexesMetadatasResponse>;

    fn deserialize_indexes_metadata(&self) -> MetastoreResult<Vec<IndexMetadata>>;

    fn empty() -> Self;
}

impl ListIndexesResponseExt for ListIndexesMetadatasResponse {
    fn try_from_indexes_metadata(
        indexes_metadata: impl IntoIterator<Item = IndexMetadata>,
    ) -> MetastoreResult<Self> {
        let indexes_metadata: Vec<IndexMetadata> = indexes_metadata.into_iter().collect();
        let indexes_metadatas_serialized_json = serde_utils::to_json_str(&indexes_metadata)?;
        let request = Self {
            indexes_metadatas_serialized_json,
        };
        Ok(request)
    }

    fn empty() -> Self {
        Self::try_from_indexes_metadata(Vec::new()).expect("should never fail")
    }

    fn deserialize_indexes_metadata(&self) -> MetastoreResult<Vec<IndexMetadata>> {
        serde_utils::from_json_str(&self.indexes_metadatas_serialized_json)
    }
}

pub trait AddSourceRequestExt {
    fn try_from_source_config(
        index_uid: impl Into<IndexUid>,
        source_config: SourceConfig,
    ) -> MetastoreResult<AddSourceRequest>;

    fn deserialize_source_config(&self) -> MetastoreResult<SourceConfig>;
}

impl AddSourceRequestExt for AddSourceRequest {
    fn try_from_source_config(
        index_uid: impl Into<IndexUid>,
        source_config: SourceConfig,
    ) -> MetastoreResult<AddSourceRequest> {
        let source_config_json = serde_json::to_string(&source_config).map_err(|error| {
            MetastoreError::JsonSerializeError {
                struct_name: "SourceConfig".to_string(),
                message: format!("Failed to serialize source config: {error:?}"),
            }
        })?;
        let request = Self {
            index_uid: index_uid.into().into(),
            source_config_json,
        };
        Ok(request)
    }

    fn deserialize_source_config(&self) -> MetastoreResult<SourceConfig> {
        serde_json::from_str(&self.source_config_json).map_err(|error| {
            MetastoreError::JsonDeserializeError {
                struct_name: "SourceConfig".to_string(),
                message: format!("Failed to deserialize source config: {error:?}"),
            }
        })
    }
}

pub trait StageSplitsRequestExt {
    fn try_from_split_metadata(
        index_uid: impl Into<IndexUid>,
        split_metadata: SplitMetadata,
    ) -> MetastoreResult<StageSplitsRequest>;

    fn try_from_splits_metadata(
        index_uid: impl Into<IndexUid>,
        splits_metadata: impl IntoIterator<Item = SplitMetadata>,
    ) -> MetastoreResult<StageSplitsRequest>;

    fn deserialize_splits_metadata(&self) -> MetastoreResult<Vec<SplitMetadata>>;
}

impl StageSplitsRequestExt for StageSplitsRequest {
    fn try_from_split_metadata(
        index_uid: impl Into<IndexUid>,
        split_metadata: SplitMetadata,
    ) -> MetastoreResult<StageSplitsRequest> {
        let split_metadata_list_serialized_json = serde_json::to_string(&[split_metadata])
            .map_err(|error| MetastoreError::JsonSerializeError {
                struct_name: "SplitMetadata".to_string(),
                message: format!("Failed to serialize split metadata: {error:?}"),
            })?;
        let request = Self {
            index_uid: index_uid.into().into(),
            split_metadata_list_serialized_json,
        };
        Ok(request)
    }

    fn try_from_splits_metadata(
        index_uid: impl Into<IndexUid>,
        splits_metadata: impl IntoIterator<Item = SplitMetadata>,
    ) -> MetastoreResult<StageSplitsRequest> {
        let splits_metadata: Vec<SplitMetadata> = splits_metadata.into_iter().collect();
        let split_metadata_list_serialized_json = serde_utils::to_json_str(&splits_metadata)?;
        let request = Self {
            index_uid: index_uid.into().into(),
            split_metadata_list_serialized_json,
        };
        Ok(request)
    }

    fn deserialize_splits_metadata(&self) -> MetastoreResult<Vec<SplitMetadata>> {
        serde_utils::from_json_str(&self.split_metadata_list_serialized_json)
    }
}

pub trait ListSplitsRequestExt {
    fn try_from_list_splits_query(
        list_splits_query: ListSplitsQuery,
    ) -> MetastoreResult<ListSplitsRequest>;

    fn deserialize_list_splits_query(&self) -> MetastoreResult<ListSplitsQuery>;
}

impl ListSplitsRequestExt for ListSplitsRequest {
    fn try_from_list_splits_query(
        list_splits_query: ListSplitsQuery,
    ) -> MetastoreResult<ListSplitsRequest> {
        let query_json = serde_utils::to_json_str(&list_splits_query)?;
        let request = Self { query_json };
        Ok(request)
    }

    fn deserialize_list_splits_query(&self) -> MetastoreResult<ListSplitsQuery> {
        let list_splits_query = serde_json::from_str(&self.query_json).map_err(|error| {
            MetastoreError::JsonDeserializeError {
                struct_name: "ListSplitsQuery".to_string(),
                message: format!("Failed to deserialize list splits query: {error:?}"),
            }
        })?;
        Ok(list_splits_query)
    }
}

pub trait ListSplitsResponseExt {
    fn empty() -> Self;
    fn try_from_splits(
        splits: impl IntoIterator<Item = Split>,
    ) -> MetastoreResult<ListSplitsResponse>;

    fn deserialize_splits(&self) -> MetastoreResult<Vec<Split>>;
}

impl ListSplitsResponseExt for ListSplitsResponse {
    fn empty() -> Self {
        Self {
            splits_serialized_json: "[]".to_string(),
        }
    }
    fn try_from_splits(splits: impl IntoIterator<Item = Split>) -> MetastoreResult<Self> {
        let splits_serialized_json = serde_utils::to_json_str(&splits.into_iter().collect_vec())?;
        let request = Self {
            splits_serialized_json,
        };
        Ok(request)
    }

    fn deserialize_splits(&self) -> MetastoreResult<Vec<Split>> {
        serde_utils::from_json_str(&self.splits_serialized_json)
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
/// A query builder for listing splits within the metastore.
pub struct ListSplitsQuery {
    /// A non-empty list of index UIDs to get splits from.
    pub index_uids: Vec<IndexUid>,

    /// The maximum number of splits to retrieve.
    pub limit: Option<usize>,

    /// The number of splits to skip.
    pub offset: Option<usize>,

    /// A specific split state(s) to filter by.
    pub split_states: Vec<SplitState>,

    /// A specific set of tag(s) to filter by.
    pub tags: Option<TagFilterAst>,

    /// The time range to filter by.
    pub time_range: FilterRange<i64>,

    /// The delete opstamp range to filter by.
    pub delete_opstamp: FilterRange<u64>,

    /// The update timestamp range to filter by.
    pub update_timestamp: FilterRange<i64>,

    /// The create timestamp range to filter by.retrying_metastore
    pub create_timestamp: FilterRange<i64>,

    /// The datetime at which you include or exclude mature splits.
    pub mature: Bound<OffsetDateTime>,

    /// Sorts the splits by staleness, i.e. by delete opstamp and publish timestamp in ascending
    /// order.
    pub sort_by_staleness: bool,
}

#[allow(unused_attributes)]
impl ListSplitsQuery {
    /// Creates a new [`ListSplitsQuery`] for the designated index.
    pub fn for_index(index_uid: IndexUid) -> Self {
        Self {
            index_uids: vec![index_uid],
            limit: None,
            offset: None,
            split_states: Vec::new(),
            tags: None,
            time_range: Default::default(),
            delete_opstamp: Default::default(),
            update_timestamp: Default::default(),
            create_timestamp: Default::default(),
            mature: Bound::Unbounded,
            sort_by_staleness: false,
        }
    }

    /// Creates a new [`ListSplitsQuery`] from a non-empty list of index UIDs.
    /// Returns an error if the list is empty.
    pub fn try_from_index_uids(index_uids: Vec<IndexUid>) -> MetastoreResult<Self> {
        if index_uids.is_empty() {
            return Err(MetastoreError::Internal {
                message: "ListSplitQuery should define at least one index uid".to_string(),
                cause: "".to_string(),
            });
        }
        Ok(Self {
            index_uids,
            limit: None,
            offset: None,
            split_states: Vec::new(),
            tags: None,
            time_range: Default::default(),
            delete_opstamp: Default::default(),
            update_timestamp: Default::default(),
            create_timestamp: Default::default(),
            mature: Bound::Unbounded,
            sort_by_staleness: false,
        })
    }

    /// Sets the maximum number of splits to retrieve.
    pub fn with_limit(mut self, n: usize) -> Self {
        self.limit = Some(n);
        self
    }

    /// Sets the number of splits to skip.
    pub fn with_offset(mut self, n: usize) -> Self {
        self.offset = Some(n);
        self
    }

    /// Select splits which have the given split state.
    pub fn with_split_state(mut self, state: SplitState) -> Self {
        self.split_states.push(state);
        self
    }

    /// Select splits which have the any of the following split state.
    pub fn with_split_states(mut self, states: impl AsRef<[SplitState]>) -> Self {
        self.split_states.extend_from_slice(states.as_ref());
        self
    }

    /// Select splits which match the given tag filter.
    pub fn with_tags_filter(mut self, tags: TagFilterAst) -> Self {
        self.tags = Some(tags);
        self
    }

    /// Set the field's lower bound to match values that are
    /// *less than or equal to* the provided value.
    pub fn with_time_range_end_lte(mut self, v: i64) -> Self {
        self.time_range.end = Bound::Included(v);
        self
    }

    /// Set the field's lower bound to match values that are
    /// *less than* the provided value.
    pub fn with_time_range_end_lt(mut self, v: i64) -> Self {
        self.time_range.end = Bound::Excluded(v);
        self
    }

    /// Set the field's upper bound to match values that are
    /// *greater than or equal to* the provided value.
    pub fn with_time_range_start_gte(mut self, v: i64) -> Self {
        self.time_range.start = Bound::Included(v);
        self
    }

    /// Set the field's upper bound to match values that are
    /// *greater than* the provided value.
    pub fn with_time_range_start_gt(mut self, v: i64) -> Self {
        self.time_range.start = Bound::Excluded(v);
        self
    }

    /// Set the field's lower bound to match values that are
    /// *less than or equal to* the provided value.
    pub fn with_delete_opstamp_lte(mut self, v: u64) -> Self {
        self.delete_opstamp.end = Bound::Included(v);
        self
    }

    /// Set the field's lower bound to match values that are
    /// *less than* the provided value.
    pub fn with_delete_opstamp_lt(mut self, v: u64) -> Self {
        self.delete_opstamp.end = Bound::Excluded(v);
        self
    }

    /// Set the field's upper bound to match values that are
    /// *greater than or equal to* the provided value.
    pub fn with_delete_opstamp_gte(mut self, v: u64) -> Self {
        self.delete_opstamp.start = Bound::Included(v);
        self
    }

    /// Set the field's upper bound to match values that are
    /// *greater than* the provided value.
    pub fn with_delete_opstamp_gt(mut self, v: u64) -> Self {
        self.delete_opstamp.start = Bound::Excluded(v);
        self
    }

    /// Set the field's lower bound to match values that are
    /// *less than or equal to* the provided value.
    pub fn with_update_timestamp_lte(mut self, v: i64) -> Self {
        self.update_timestamp.end = Bound::Included(v);
        self
    }

    /// Set the field's lower bound to match values that are
    /// *less than* the provided value.
    pub fn with_update_timestamp_lt(mut self, v: i64) -> Self {
        self.update_timestamp.end = Bound::Excluded(v);
        self
    }

    /// Set the field's upper bound to match values that are
    /// *greater than or equal to* the provided value.
    pub fn with_update_timestamp_gte(mut self, v: i64) -> Self {
        self.update_timestamp.start = Bound::Included(v);
        self
    }

    /// Set the field's upper bound to match values that are
    /// *greater than* the provided value.
    pub fn with_update_timestamp_gt(mut self, v: i64) -> Self {
        self.update_timestamp.start = Bound::Excluded(v);
        self
    }

    /// Set the field's lower bound to match values that are
    /// *less than or equal to* the provided value.
    pub fn with_create_timestamp_lte(mut self, v: i64) -> Self {
        self.create_timestamp.end = Bound::Included(v);
        self
    }

    /// Set the field's lower bound to match values that are
    /// *less than* the provided value.
    pub fn with_create_timestamp_lt(mut self, v: i64) -> Self {
        self.create_timestamp.end = Bound::Excluded(v);
        self
    }

    /// Set the field's upper bound to match values that are
    /// *greater than or equal to* the provided value.
    pub fn with_create_timestamp_gte(mut self, v: i64) -> Self {
        self.create_timestamp.start = Bound::Included(v);
        self
    }

    /// Set the field's upper bound to match values that are
    /// *greater than* the provided value.
    pub fn with_create_timestamp_gt(mut self, v: i64) -> Self {
        self.create_timestamp.start = Bound::Excluded(v);
        self
    }

    /// Retains splits that are mature at the given datetime.
    pub fn retain_mature(mut self, now: OffsetDateTime) -> Self {
        self.mature = Bound::Included(now);
        self
    }

    /// Retains splits that are immature at the given datetime.
    pub fn retain_immature(mut self, now: OffsetDateTime) -> Self {
        self.mature = Bound::Excluded(now);
        self
    }

    /// Sorts the splits by staleness, i.e. by delete opstamp and publish timestamp in ascending
    /// order.
    pub fn sort_by_staleness(mut self) -> Self {
        self.sort_by_staleness = true;
        self
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
/// A range containing the upper and lower bounds to filter documents by.
pub struct FilterRange<T> {
    /// The lower bound of the filter.
    pub start: Bound<T>,
    /// The upper bound of the filter.
    pub end: Bound<T>,
}

impl<T: PartialEq + PartialOrd> FilterRange<T> {
    /// Checks if both the upper and lower bound are `Bound::Unbounded`.
    pub fn is_unbounded(&self) -> bool {
        self.start == Bound::Unbounded && self.end == Bound::Unbounded
    }

    /// Checks if the provided value lies within the upper and lower bounds
    /// of the range.
    pub fn contains(&self, value: &T) -> bool {
        if self.is_unbounded() {
            return true;
        }

        let lower_check = match &self.start {
            Bound::Unbounded => true,
            Bound::Included(left) => left <= value,
            Bound::Excluded(left) => left < value,
        };

        let upper_check = match &self.end {
            Bound::Unbounded => true,
            Bound::Included(left) => left >= value,
            Bound::Excluded(left) => left > value,
        };

        lower_check && upper_check
    }

    /// Checks if the provided range overlaps with the range.
    pub fn overlaps_with(&self, range: RangeInclusive<T>) -> bool {
        if self.is_unbounded() {
            return true;
        }

        let lower_check = match &self.start {
            Bound::Unbounded => true,
            Bound::Included(left) => left <= range.end(),
            Bound::Excluded(left) => left < range.end(),
        };

        let upper_check = match &self.end {
            Bound::Unbounded => true,
            Bound::Included(left) => left >= range.start(),
            Bound::Excluded(left) => left > range.start(),
        };

        lower_check && upper_check
    }
}

// The `Default` derive implementation imposes a restriction
// for `T` to also implement Default when this is not required.
impl<T> Default for FilterRange<T> {
    fn default() -> Self {
        Self {
            start: Bound::Unbounded,
            end: Bound::Unbounded,
        }
    }
}

#[cfg(test)]
mod list_splits_query_tests {
    use super::*;

    #[test]
    fn test_filter_contains() {
        let filter = FilterRange {
            start: Bound::Unbounded,
            end: Bound::Excluded(50),
        };
        assert!(!filter.contains(&50));
        assert!(filter.contains(&0));
        assert!(filter.contains(&49));

        let filter = FilterRange {
            start: Bound::Included(50),
            end: Bound::Unbounded,
        };
        assert!(filter.contains(&50));
        assert!(filter.contains(&51));
        assert!(!filter.contains(&0));

        let filter = FilterRange {
            start: Bound::Included(50),
            end: Bound::Excluded(75),
        };
        assert!(filter.contains(&50));
        assert!(filter.contains(&51));
        assert!(!filter.contains(&0));
        assert!(!filter.contains(&75));
        assert!(filter.contains(&74));
    }

    #[test]
    fn test_overlaps_with() {
        let filter = FilterRange {
            start: Bound::Unbounded,
            end: Bound::Excluded(50),
        };
        assert!(filter.overlaps_with(0..=50));
        assert!(filter.overlaps_with(0..=51));
        assert!(filter.overlaps_with(32..=63));
        assert!(filter.overlaps_with(32..=32));
        assert!(!filter.overlaps_with(51..=76));
        assert!(!filter.overlaps_with(50..=76));

        let filter = FilterRange {
            start: Bound::Unbounded,
            end: Bound::Included(50),
        };
        assert!(filter.overlaps_with(0..=50));
        assert!(filter.overlaps_with(0..=51));
        assert!(filter.overlaps_with(50..=76));
        assert!(!filter.overlaps_with(51..=76));

        let filter = FilterRange {
            start: Bound::Excluded(50),
            end: Bound::Unbounded,
        };
        assert!(filter.overlaps_with(51..=75));
        assert!(filter.overlaps_with(0..=51));
        assert!(filter.overlaps_with(51..=76));
        assert!(filter.overlaps_with(50..=76));
        assert!(!filter.overlaps_with(0..=49));
        assert!(!filter.overlaps_with(0..=50));

        let filter = FilterRange {
            start: Bound::Included(50),
            end: Bound::Unbounded,
        };
        assert!(filter.overlaps_with(51..=75));
        assert!(filter.overlaps_with(0..=51));
        assert!(filter.overlaps_with(51..=76));
        assert!(filter.overlaps_with(50..=76));
        assert!(filter.overlaps_with(0..=50));
        assert!(!filter.overlaps_with(0..=49));

        let filter = FilterRange {
            start: Bound::Included(50),
            end: Bound::Excluded(75),
        };
        assert!(filter.overlaps_with(51..=75));
        assert!(filter.overlaps_with(0..=51));
        assert!(filter.overlaps_with(45..=76));
        assert!(filter.overlaps_with(50..=76));
        assert!(filter.overlaps_with(0..=50));
        assert!(filter.overlaps_with(74..=124));
        assert!(!filter.overlaps_with(0..=49));
        assert!(!filter.overlaps_with(75..=124));
    }
}
