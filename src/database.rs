use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::mpsc::{sync_channel, Receiver};
use std::sync::{Arc, RwLock};

use actix::{Actor, Addr};

use petgraph::stable_graph::{NodeIndex, StableGraph};
use petgraph::Directed;
use url::Url;

use crate::graph::Graph;
use crate::remotes::Remotes;
use crate::server::GraphServer;

use crate::{
    GraphEdge, GraphMutation, GraphNode, GraphNodeIndex, GraphQuery,
    GraphResponse, StoreError,
};

#[derive(Debug)]
pub struct GraphDatabase<N, E, I>
where
    N: GraphNode + Unpin + 'static,
    E: GraphEdge + Unpin + 'static,
    I: GraphNodeIndex + From<N> + Unpin + 'static,
{
    graph: Addr<Graph<N, E, I>>,
    remotes: Addr<Remotes>,
}

impl<N, E, I> GraphDatabase<N, E, I>
where
    N: GraphNode + Unpin + 'static,
    E: GraphEdge + Unpin + 'static,
    I: GraphNodeIndex + From<N> + Unpin + 'static,
{
    // constructors
    //
    pub async fn run(
        store_path: Option<String>,
        server_url: String,
        initial_remote_addresses: Vec<String>,
    ) -> Result<Receiver<Arc<RwLock<Self>>>, StoreError> {
        let graph = Graph::<N, E, I>::new(store_path)?.start();
        let graph2 = graph.clone();

        let remotes =
            Remotes::new(server_url.clone(), initial_remote_addresses)
                .await
                .start();
        let remotes2 = remotes.clone();

        let server_address = match Url::parse(&server_url) {
            Ok(url) => match (url.host_str(), url.port()) {
                (Some(host), Some(port)) => format!("{}:{}", host, port),
                _ => return Err(StoreError::ParseError),
            },
            Err(err) => {
                log::error!(
                    "Error while parsing server url. \
Error: '{err}'"
                );
                return Err(StoreError::ParseError);
            }
        };

        let server_address = match SocketAddr::from_str(&server_address) {
            Ok(address) => address,
            Err(err) => {
                log::error!(
                    "Error while formatting server url to socket address. \
Error: '{err}'"
                );
                return Err(StoreError::ParseError);
            }
        };

        actix_rt::spawn(async move {
            if let Err(err) =
                GraphServer::run(server_address, graph2, remotes2).await
            {
                log::error!(
                    "Error while starting GraphServer. Error: '{err:?}'"
                );
            };
        });

        let (tx, rx) = sync_channel::<Arc<RwLock<Self>>>(1);

        tx.send(Arc::new(RwLock::new(Self { graph, remotes })))
            .unwrap();

        Ok(rx)
    }

    //
    // public interface
    //
    // mutating queries
    pub async fn add_edge(
        &mut self,
        from: I,
        to: I,
        edge: E,
    ) -> Result<(), StoreError> {
        let query = GraphMutation::AddEdge((from, to, edge));

        self.graph.send(query.clone()).await??;
        self.remotes.do_send(query);

        Ok(())
    }

    pub async fn remove_edge(
        &mut self,
        from: I,
        to: I,
    ) -> Result<E, StoreError> {
        let query = GraphMutation::RemoveEdge((from, to));

        if let GraphResponse::Edge(edge) =
            self.graph.send(query.clone()).await??
        {
            self.remotes.do_send(query);

            Ok(edge)
        } else {
            Err(StoreError::EdgeNotDeleted)
        }
    }

    pub async fn add_node(&mut self, key: I, node: N) -> Result<N, StoreError> {
        let query = GraphMutation::AddNode((key, node));

        if let GraphResponse::Node(node) =
            self.graph.send(query.clone()).await??
        {
            self.remotes.do_send(query);

            Ok(node)
        } else {
            Err(StoreError::NodeNotCreated)
        }
    }

    pub async fn remove_node(&mut self, key: I) -> Result<N, StoreError> {
        let query = GraphMutation::RemoveNode(key);

        if let GraphResponse::Node(node) =
            self.graph.send(query.clone()).await??
        {
            self.remotes.do_send(query);

            Ok(node)
        } else {
            Err(StoreError::NodeNotDeleted)
        }
    }

    // read only
    pub async fn get_graph(
        &self,
    ) -> Result<StableGraph<N, E, Directed>, StoreError> {
        let query = GraphQuery::GetGraph;

        if let GraphResponse::Graph(graph) = self.graph.send(query).await?? {
            Ok(graph)
        } else {
            Err(StoreError::GraphNotFound)
        }
    }

    pub async fn filter_graph(
        &self,
        include_nodes: Option<Vec<N>>,
        include_edges: Option<Vec<E>>,
    ) -> Result<StableGraph<N, E, Directed>, StoreError> {
        let query = GraphQuery::FilterGraph((include_nodes, include_edges));

        if let GraphResponse::Graph(graph) = self.graph.send(query).await?? {
            Ok(graph)
        } else {
            Err(StoreError::GraphNotFound)
        }
    }

    pub async fn retain_nodes(
        &self,
        nodes_to_retain: Vec<&'static I>,
    ) -> Result<StableGraph<N, E, Directed>, StoreError> {
        let query = GraphQuery::RetainNodes(nodes_to_retain);

        if let GraphResponse::Graph(graph) = self.graph.send(query).await?? {
            Ok(graph)
        } else {
            Err(StoreError::GraphNotFound)
        }
    }

    pub async fn get_neighbors(
        &self,
        key: &'static I,
    ) -> Result<Vec<N>, StoreError> {
        let query = GraphQuery::GetNeighbors(key);

        if let GraphResponse::Nodes(nodes) = self.graph.send(query).await?? {
            Ok(nodes)
        } else {
            Err(StoreError::NodeNotFound)
        }
    }

    pub async fn get_edge(
        &self,
        from: &'static I,
        to: &'static I,
    ) -> Result<E, StoreError> {
        let query = GraphQuery::GetEdge((from, to));

        if let GraphResponse::Edge(edge) = self.graph.send(query).await?? {
            Ok(edge)
        } else {
            Err(StoreError::EdgeNotFound)
        }
    }

    pub async fn get_edges(&self) -> Result<Vec<E>, StoreError> {
        let query = GraphQuery::GetEdges;

        if let GraphResponse::Edges(edges) = self.graph.send(query).await?? {
            Ok(edges)
        } else {
            Err(StoreError::EdgeNotFound)
        }
    }

    pub async fn has_node(&self, key: &'static I) -> Result<bool, StoreError> {
        let query = GraphQuery::HasNode(key);

        if let GraphResponse::Bool(has) = self.graph.send(query).await?? {
            Ok(has)
        } else {
            Err(StoreError::NodeNotFound)
        }
    }

    pub async fn get_node(&self, key: &'static I) -> Result<N, StoreError> {
        let query = GraphQuery::GetNode(key);

        if let GraphResponse::Node(node) = self.graph.send(query).await?? {
            Ok(node)
        } else {
            Err(StoreError::NodeNotFound)
        }
    }

    pub async fn get_nodes(&self) -> Result<Vec<N>, StoreError> {
        let query = GraphQuery::GetNodes;

        if let GraphResponse::Nodes(nodes) = self.graph.send(query).await?? {
            Ok(nodes)
        } else {
            Err(StoreError::NodeNotFound)
        }
    }

    pub async fn get_node_index(
        &self,
        key: &'static I,
    ) -> Result<NodeIndex, StoreError> {
        let query = GraphQuery::GetNodeIndex(key);

        if let GraphResponse::NodeIndex(node_index) =
            self.graph.send(query).await??
        {
            Ok(node_index)
        } else {
            Err(StoreError::NodeNotFound)
        }
    }

    pub async fn get_source_nodes(&self) -> Result<Vec<N>, StoreError> {
        let query = GraphQuery::GetSourceNodes;

        if let GraphResponse::Nodes(nodes) = self.graph.send(query).await?? {
            Ok(nodes)
        } else {
            Err(StoreError::NodeNotFound)
        }
    }

    pub async fn get_sink_nodes(&self) -> Result<Vec<N>, StoreError> {
        let query = GraphQuery::GetSinkNodes;

        if let GraphResponse::Nodes(nodes) = self.graph.send(query).await?? {
            Ok(nodes)
        } else {
            Err(StoreError::NodeNotFound)
        }
    }
}
