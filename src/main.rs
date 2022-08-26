use std::env;

use serde::{Deserialize, Serialize};

use graph_db::{GraphDatabase, GraphEdge, GraphNode, GraphNodeIndex};

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

#[actix_rt::main]
async fn main() -> Result<(), std::io::Error> {
    env_logger::init();

    let store_path: String =
        env::var("AGRAPHSTORE_PATH").expect("AGRAPHSTORE_PATH");

    let server_address =
        env::var("AGRAPHSTORE_SERVER_URL").expect("AGRAPHSTORE_SERVER_URL");

    let initial_remote_addresses: Vec<String> =
        env::var("AGRAPHSTORE_INITIAL_REMOTE_URLS")
            .expect("AGRAPHSTORE_INITIAL_REMOTE_URLS")
            .split(',')
            .map(String::from)
            .collect();

    log::info!(
        "AGRAPHSTORE_PATH: {:?}, AGRAPHSTORE_SERVER_URL: {:?}, \
AGRAPHSTORE_INITIAL_REMOTE_URLS: {:?}",
        store_path,
        server_address,
        initial_remote_addresses
    );

    let database = Database::run(
        Some(store_path),
        server_address,
        initial_remote_addresses,
    )
    .await
    .unwrap()
    .recv()
    .unwrap();

    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    {
        if env::var("MAIN").is_ok() {
            database
                .write()
                .unwrap()
                .add_node(
                    NodeKey(1),
                    NodeTypes {
                        id: 1,
                        name: "First Node".to_string(),
                    },
                )
                .await
                .unwrap();
        } else {
            database
                .write()
                .unwrap()
                .add_node(
                    NodeKey(2),
                    NodeTypes {
                        id: 2,
                        name: "Second Node".to_string(),
                    },
                )
                .await
                .unwrap();
        }
    }

    tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
    log::info!("UPDATING GRAPH SECOND TIME");
    {
        if env::var("MAIN").is_ok() {
            database
                .write()
                .unwrap()
                .add_node(
                    NodeKey(3),
                    NodeTypes {
                        id: 3,
                        name: "Third Node".to_string(),
                    },
                )
                .await
                .unwrap();
        } else {
            database
                .write()
                .unwrap()
                .add_node(
                    NodeKey(4),
                    NodeTypes {
                        id: 4,
                        name: "Fourth Node".to_string(),
                    },
                )
                .await
                .unwrap();
        }
    }

    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    let graph = database.read().unwrap().get_graph().await.unwrap();
    log::info!("Resulting Graph: '{:?}'", graph);

    tokio::time::sleep(tokio::time::Duration::from_secs(60 * 5)).await;

    Ok(())
}
