use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::RwLock;
use std::{env, sync::Arc};

use tokio::sync::oneshot;

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
        env::var("DATA_STORE_PATH").expect("DATA_STORE_PATH");

    let server_address = env::var("DATABASE_SERVER_URL").expect("DATABASE_URL");
    let server_address = SocketAddr::from_str(&server_address)
        .expect("provided DATABASE_URL was not valid");

    let initial_remote_addresses: Vec<String> =
        env::var("DATABASE_INITIAL_REMOTE_ADDRESSES")
            .expect("DATABASE_INITIAL_REMOTE_ADDRESSES")
            .split(',')
            .map(String::from)
            .collect();

    let (tx, rx) = oneshot::channel::<Arc<RwLock<Database>>>();

    // tokio::spawn(async move {
    Database::run(
        Some(store_path),
        server_address,
        initial_remote_addresses,
        tx,
    )
    .await
    .unwrap();
    // });

    let database = rx.await.unwrap();

    tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;

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

    let graph = database.read().unwrap().get_graph().await.unwrap();
    log::debug!("Resulting Graph: '{:?}'", graph);

    tokio::time::sleep(tokio::time::Duration::from_secs(20)).await;

    Ok(())
}
