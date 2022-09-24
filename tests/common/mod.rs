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
pub struct SimpleEdgeType(pub usize);
impl GraphEdge for SimpleEdgeType {}

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
pub struct SimpleNodeType {
    pub id: Uuid,
}

impl SimpleNodeType {
    pub fn new(id: Uuid) -> Self {
        Self { id }
    }
}

impl GraphNode for SimpleNodeType {}

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
pub struct SimpleNodeWeightIndex(pub Uuid);

impl GraphNodeIndex for SimpleNodeWeightIndex {}

impl From<SimpleNodeType> for SimpleNodeWeightIndex {
    fn from(node: SimpleNodeType) -> Self {
        Self(node.id)
    }
}

pub type SimpleDatabase =
    GraphDatabase<SimpleNodeType, SimpleEdgeType, SimpleNodeWeightIndex>;

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
