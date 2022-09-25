use serde::{Deserialize, Serialize};

use uuid::Uuid;

use graph_db::{
    DatabaseConfig, GraphDatabase, GraphEdge, GraphNode, GraphNodeIndex,
};

#[derive(
    Debug,
    Default,
    Clone,
    Copy,
    Hash,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
)]
pub struct Edge(pub usize);
impl GraphEdge for Edge {}

#[derive(
    Debug,
    Default,
    Clone,
    Copy,
    Hash,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
)]
pub struct Node {
    pub id: Uuid,
}

impl Node {
    pub fn new(id: Uuid) -> Self {
        Self { id }
    }
}

impl GraphNode for Node {}

#[derive(
    Debug,
    Default,
    Clone,
    Copy,
    Hash,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
)]
pub struct NodeId(pub Uuid);

impl GraphNodeIndex for NodeId {}

impl From<Node> for NodeId {
    fn from(node: Node) -> Self {
        Self(node.id)
    }
}

pub type SimpleDatabase = GraphDatabase<Node, Edge, NodeId>;

pub async fn setup(
    store_path: &'static str,
    port: u16,
    initial_remotes: Vec<String>,
) -> SimpleDatabase {
    if std::path::Path::new(store_path).is_dir() {
        std::fs::remove_dir_all(store_path).unwrap();
    }

    let server_address = format!("http://127.0.0.1:{}", port);
    let mut database_config = DatabaseConfig::init();
    database_config.set_store_path(store_path.to_string());
    database_config.set_server_url(server_address);
    database_config.set_initial_remote_addresses(initial_remotes);

    GraphDatabase::run(database_config).await.unwrap()
}
