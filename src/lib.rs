use std::env;
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::PoisonError;
use std::time::{SystemTime, UNIX_EPOCH};

use actix::{MailboxError, Message};

use serde::{de::DeserializeOwned, Deserialize, Serialize};

use petgraph::stable_graph::{NodeIndex, StableGraph};
use petgraph::Directed;

mod client;
mod database;
mod graph;
mod graph_store;
mod mutations_log;
mod mutations_log_store;
mod remotes;
mod server;

pub(crate) use client::GraphClient;
pub use database::GraphDatabase;
pub(crate) use remotes::SyncRemotesMessage;
use url::Url;

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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(bound(deserialize = "N: DeserializeOwned"))]
pub enum GraphMutation<N, E, I>
where
    N: GraphNode + 'static,
    E: GraphEdge + 'static,
    I: GraphNodeIndex + From<N> + 'static,
{
    AddEdge((I, I, E)),
    RemoveEdge((I, I)),
    AddNode(N),
    RemoveNode(I),
}

impl<N, E, I> GraphMutation<N, E, I>
where
    N: GraphNode + 'static,
    E: GraphEdge + 'static,
    I: GraphNodeIndex + From<N> + 'static,
{
    pub(crate) fn get_hash(&self) -> String {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Now should be after UNIX EPOCH")
            .as_nanos();

        format!("{}{:x}", now, md5::compute(format!("{:?}", self)))
    }
}

impl<N, E, I> TryFrom<Vec<u8>> for GraphMutation<N, E, I>
where
    N: GraphNode + 'static,
    E: GraphEdge + 'static,
    I: GraphNodeIndex + From<N> + 'static,
{
    type Error = StoreError;

    fn try_from(bytes: Vec<u8>) -> Result<Self, Self::Error> {
        bincode::deserialize(&bytes)
            .map_err(|_| StoreError::WriteLogError("".to_string()))
    }
}

impl<N, E, I> TryInto<Vec<u8>> for GraphMutation<N, E, I>
where
    N: GraphNode + 'static,
    E: GraphEdge + 'static,
    I: GraphNodeIndex + From<N> + 'static,
{
    type Error = StoreError;

    fn try_into(self) -> Result<Vec<u8>, Self::Error> {
        bincode::serialize(&self)
            .map_err(|_| StoreError::WriteLogError("".to_string()))
    }
}

impl<N, E, I> Message for GraphMutation<N, E, I>
where
    N: GraphNode + 'static,
    E: GraphEdge + 'static,
    I: GraphNodeIndex + From<N> + 'static,
{
    type Result = Result<GraphResponse<N, E, I>, StoreError>;
}

#[derive(Debug, Clone)]
pub enum GraphQuery<N, E, I>
where
    N: GraphNode + 'static,
    E: GraphEdge + 'static,
    I: GraphNodeIndex + From<N> + 'static,
{
    GetGraph,
    FilterGraph((Option<Vec<N>>, Option<Vec<E>>)),
    RetainNodes(Vec<I>),
    GetNeighbors(I),
    GetEdge((I, I)),
    GetEdges,
    HasNode(I),
    GetNode(I),
    GetNodes,
    GetNodeIndex(I),
    GetSourceNodes,
    GetSinkNodes,
}

impl<N, E, I> Message for GraphQuery<N, E, I>
where
    N: GraphNode + 'static,
    E: GraphEdge + 'static,
    I: GraphNodeIndex + From<N> + 'static,
{
    type Result = Result<GraphResponse<N, E, I>, StoreError>;
}

#[derive(Debug)]
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

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
pub enum StoreError {
    // graph
    GraphNotFound,
    EdgeNotFound,
    EdgeNotCreated,
    EdgeNotDeleted,
    NodeNotFound,
    NodeNotCreated,
    NodeNotDeleted,
    ConflictDuplicateNode,
    ConflictDuplicateEdge,
    // store
    StoreError,
    FileSaveError(String),
    FileLoadError(String),
    FileDecodeError(String),
    SqliteError(String),
    WriteLogError(String),
    // sync
    SyncError(String),
    PoisonError(String),
    MailboxError(String),
    // rpc
    ClientSendError,
    ClientError,
    // internal
    ParseError,
    Serde(String),
}

impl<T> From<PoisonError<T>> for StoreError {
    fn from(err: PoisonError<T>) -> Self {
        Self::PoisonError(format!("{err}"))
    }
}

impl From<MailboxError> for StoreError {
    fn from(err: MailboxError) -> Self {
        Self::MailboxError(format!("{err}"))
    }
}

impl From<rusqlite::Error> for StoreError {
    fn from(err: rusqlite::Error) -> Self {
        Self::SqliteError(format!("{err}"))
    }
}

impl ToString for StoreError {
    fn to_string(&self) -> String {
        format!("{:?}", self)
    }
}

#[derive(Debug, Clone)]
pub struct DatabaseConfig {
    server_url: String,
    initial_remote_addresses: Vec<String>,
    store_path: Option<String>,
    sync_with_remotes: usize,
}

impl DatabaseConfig {
    pub fn init() -> Self {
        let mut config = Self::default();

        if let Ok(server_url) = env::var("AGRAPHSTORE_SERVER_URL") {
            Url::parse(&server_url).expect(
                "Configuration error provided AGRAPHSTORE_SERVER_URL \
is not a valid URL.",
            );
            config.server_url = server_url;
        }

        if let Ok(initial_remotes) = env::var("AGRAPHSTORE_INITIAL_REMOTE_URLS")
        {
            let addresses: Vec<String> =
                initial_remotes.split(',').map(String::from).collect();
            for address in addresses.clone().iter() {
                Url::parse(&address).expect(&format!(
                    "Configuration error provided url {} \
of AGRAPHSTORE_INITIAL_REMOTE_URLS is not a valid URL.",
                    address,
                ));
            }

            config.initial_remote_addresses = addresses;
        }

        if let Ok(store_path) = env::var("AGRAPHSTORE_PATH") {
            config.store_path = Some(store_path);
        }

        if let Ok(sync_with_remotes) =
            env::var("AGRAPHSTORE_SYNCH_WITH_REMOTES_N")
        {
            let sync_with_remotes: usize = sync_with_remotes.parse().expect(
                "Configuration error provided AGRAPHSTORE_SYNCH_WITH_REMOTES_N \
is not exactly 8 charackters long."
            );
            config.sync_with_remotes = sync_with_remotes;
        }

        config
    }

    pub fn set_server_url(&mut self, server_url: String) {
        Url::parse(&server_url).expect(
            "Configuration error provided `server_url` \
is not a valid URL.",
        );
        self.server_url = server_url;
    }

    pub fn set_initial_remote_addresses(
        &mut self,
        initial_remote_addresses: Vec<String>,
    ) {
        for address in initial_remote_addresses.clone().iter() {
            Url::parse(&address).expect(&format!(
                "Configuration error provided url {} \
of `initial_remote_addresses` is not a valid URL.",
                address,
            ));
        }
        self.initial_remote_addresses = initial_remote_addresses;
    }

    pub fn set_store_path(&mut self, store_path: String) {
        self.store_path = Some(store_path);
    }
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            server_url: String::default(),
            initial_remote_addresses: Vec::default(),
            store_path: Option::default(),
            sync_with_remotes: 2,
        }
    }
}
