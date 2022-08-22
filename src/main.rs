use std::env;
use std::net::SocketAddr;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use graph_db::{
    GraphDatabase, GraphEdge, GraphNode, GraphNodeIndex, MutateGraph,
};

#[derive(
    Debug,
    Clone,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
)]
pub struct EdgeType(usize);
impl GraphEdge for EdgeType {}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct NodeTypes {
    id: usize,
    name: String,
}

impl GraphNode for NodeTypes {}

#[derive(
    Debug, Clone, Default, Hash, PartialEq, Eq, Serialize, Deserialize,
)]
pub struct NodeKey(usize);
impl GraphNodeIndex for NodeKey {}
impl From<NodeTypes> for NodeKey {
    fn from(node: NodeTypes) -> Self {
        Self(node.id)
    }
}

type Database = GraphDatabase<NodeTypes, EdgeType, NodeKey>;

#[tokio::main]
async fn main() -> Result<(), std::io::Error> {
    env_logger::init();

    let store_path: String =
        env::var("DATA_STORE_PATH").expect("DATA_STORE_PATH");

    let server_address = env::var("DATABASE_SERVER_URL").expect("DATABASE_URL");
    let server_address = SocketAddr::from_str(&server_address)
        .expect("provided DATABASE_URL was not valid");

    let remote_addresses: Vec<String> = env::var("DATABASE_REMOTE_ADDRESSES")
        .expect("DATABASE_REMOTE_ADDRESSES")
        .split(',')
        .map(String::from)
        .collect();

    let database =
        Database::run(Some(store_path), server_address, remote_addresses)
            .await
            .unwrap();
    {
        let added_node = database
            .write()
            .unwrap()
            .add_node(
                NodeKey(1),
                NodeTypes {
                    id: 1,
                    name: "First Node".to_string(),
                },
            )
            .unwrap();

        log::info!("Added node: {added_node:#?}");
    }
    {
        let graph = database.read().unwrap().get_graph().unwrap();

        log::info!("Got graph: {graph:#?}");
    }

    Ok(())
}
