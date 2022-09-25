use std::env;

use serde::{Deserialize, Serialize};

use graph_db::{
    DatabaseConfig, GraphDatabase, GraphEdge, GraphNode, GraphNodeIndex,
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

#[actix_rt::main]
async fn main() -> Result<(), std::io::Error> {
    env_logger::init();

    let database_config = DatabaseConfig::init();
    log::debug!("Database config: {:#?}", database_config);
    let database = Database::run(database_config).await.unwrap();

    tokio::time::sleep(tokio::time::Duration::from_secs(4)).await;

    {
        if env::var("MAIN").is_ok() {
            database
                .add_node(NodeTypes {
                    id: 1,
                    name: "First Node".to_string(),
                })
                .await
                .unwrap();
        } else {
            database
                .add_node(NodeTypes {
                    id: 2,
                    name: "Second Node".to_string(),
                })
                .await
                .unwrap();
        }
    }

    tokio::time::sleep(tokio::time::Duration::from_secs(20)).await;
    log::info!("UPDATING GRAPH SECOND TIME");
    {
        if env::var("MAIN").is_ok() {
            database
                .add_node(NodeTypes {
                    id: 3,
                    name: "Third Node".to_string(),
                })
                .await
                .unwrap();
        } else {
            database
                .add_node(NodeTypes {
                    id: 4,
                    name: "Fourth Node".to_string(),
                })
                .await
                .unwrap();
        }
    }

    tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;

    let graph = database.get_graph().await.unwrap();
    log::info!("Resulting Graph: '{:?}'", graph);

    tokio::time::sleep(tokio::time::Duration::from_secs(60 * 5)).await;

    Ok(())
}
