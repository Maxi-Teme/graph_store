use std::fmt::Debug;
use std::hash::Hash;
use std::sync::PoisonError;

use serde::{de::DeserializeOwned, Deserialize, Serialize};

mod client;
mod database;
mod graph;
mod server;
mod store;

pub use database::GraphDatabase;

mod sync_graph {
    tonic::include_proto!("sync_graph");
}

pub trait GraphNode:
    Debug + Default + Clone + Sync + Send + PartialEq + Serialize + DeserializeOwned
{
}

pub trait GraphEdge:
    Debug
    + Default
    + Clone
    + Sync
    + Send
    + PartialOrd
    + Serialize
    + DeserializeOwned
{
}

pub trait GraphNodeIndex:
    Debug + Default + Clone + Sync + Send + Hash + Eq + Serialize + DeserializeOwned
{
}

pub trait MutateGraph<N: GraphNode, E: GraphEdge, I: GraphNodeIndex + From<N>> {
    fn add_edge(&mut self, from: &I, to: &I, edge: E)
        -> Result<(), StoreError>;

    fn remove_edge(&mut self, from: &I, to: &I) -> Result<E, StoreError>;

    fn add_node(&mut self, key: I, node: N) -> Result<N, StoreError>;

    fn remove_node(&mut self, key: I) -> Result<N, StoreError>;
}

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
pub enum StoreError {
    GraphNotFound,
    EdgeNotFound,
    EdgeNotCreated,
    EdgeNotDeleted,
    NodeNotFound,
    NodeNotCreated,
    NodeNotDeleted,
    ConflictDuplicateKey,
    ParseError,
    FileSaveError(String),
    FileLoadError(String),
    FileDecodeError(String),
    PoisonError,
    StoreError,
    SyncError(String),
}

impl<T> From<PoisonError<T>> for StoreError {
    fn from(_: PoisonError<T>) -> Self {
        Self::PoisonError
    }
}
