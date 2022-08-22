use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::{Arc, RwLock},
};

use serde::{Deserialize, Serialize};

use uuid::Uuid;

use graph_db::{GraphDatabase, GraphEdge, GraphNode, GraphNodeIndex};

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
) -> (Arc<RwLock<SimpleDatabase>>, impl Fn() -> ()) {
    let server_address =
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port);
    let database = GraphDatabase::run(
        Some(store_path.to_string()),
        server_address,
        vec![],
    )
    .await
    .unwrap();

    let teardown = move || {
        std::fs::remove_dir_all(store_path).unwrap();
    };

    (database, teardown)
}
