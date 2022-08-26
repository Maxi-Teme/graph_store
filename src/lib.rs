use std::fmt::Debug;
use std::hash::Hash;
use std::sync::PoisonError;

use actix::{MailboxError, Message};

use serde::{de::DeserializeOwned, Deserialize, Serialize};

use petgraph::stable_graph::{NodeIndex, StableGraph};
use petgraph::Directed;

mod client;
mod database;
mod graph;
mod remotes;
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

#[derive(Debug, Clone)]
pub enum MutatinGraphQuery<N, E, I>
where
    N: GraphNode + 'static,
    E: GraphEdge + 'static,
    I: GraphNodeIndex + From<N> + 'static,
{
    AddEdge((I, I, E)),
    RemoveEdge((I, I)),
    AddNode((I, N)),
    RemoveNode(I),
}

impl<N, E, I> Message for MutatinGraphQuery<N, E, I>
where
    N: GraphNode + 'static,
    E: GraphEdge + 'static,
    I: GraphNodeIndex + From<N> + 'static,
{
    type Result = Result<(), StoreError>;
}

#[derive(Debug, Clone)]
pub enum ReadOnlyGraphQuery<N, E, I>
where
    N: GraphNode + 'static,
    E: GraphEdge + 'static,
    I: GraphNodeIndex + From<N> + 'static,
{
    GetGraph,
    FilterGraph((Option<Vec<N>>, Option<Vec<E>>)),
    RetainNodes(Vec<&'static I>),
    GetNeighbors(&'static I),
    GetEdge((&'static I, &'static I)),
    GetEdges,
    HasNode(&'static I),
    GetNode(&'static I),
    GetNodes,
    GetNodeIndex(&'static I),
    GetSourceNodes,
    GetSinkNodes,
}

#[derive(Debug, Clone)]
pub enum GraphQuery<N, E, I>
where
    N: GraphNode + 'static,
    E: GraphEdge + 'static,
    I: GraphNodeIndex + From<N> + 'static,
{
    Mutating(MutatinGraphQuery<N, E, I>),
    ReadOnly(ReadOnlyGraphQuery<N, E, I>),
}

impl<N, E, I> Message for GraphQuery<N, E, I>
where
    N: GraphNode + 'static,
    E: GraphEdge + 'static,
    I: GraphNodeIndex + From<N> + 'static,
{
    type Result = Result<GraphResponse<N, E, I>, StoreError>;
}

pub enum GraphResponse<N, E, I>
where
    N: GraphNode + 'static,
    E: GraphEdge + 'static,
    I: GraphNodeIndex + From<N> + 'static,
{
    Empty,
    Bool(bool),
    Node(N),
    Nodes(Vec<N>),
    NodeIndex(NodeIndex),
    Edge(E),
    Edges(Vec<E>),
    Key(I),
    Keys(Vec<I>),
    Graph(StableGraph<N, E, Directed>),
}

pub struct RemotesMessage(String);

impl Message for RemotesMessage {
    type Result = Result<(), StoreError>;
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
    ClientSendError,
    ClientError,
    MailboxError,
}

impl<T> From<PoisonError<T>> for StoreError {
    fn from(_: PoisonError<T>) -> Self {
        Self::PoisonError
    }
}

impl From<MailboxError> for StoreError {
    fn from(_: MailboxError) -> Self {
        Self::MailboxError
    }
}
