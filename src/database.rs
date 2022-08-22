use std::net::SocketAddr;
use std::sync::{Arc, RwLock};

use petgraph::stable_graph::{EdgeIndex, NodeIndex, StableGraph};
use petgraph::Directed;

use crate::client::GraphClient;
use crate::graph::Graph;
use crate::server::GraphServer;
use crate::{GraphEdge, GraphNode, GraphNodeIndex, MutateGraph, StoreError};

pub struct GraphDatabase<N, E, I>
where
    N: GraphNode,
    E: GraphEdge,
    I: GraphNodeIndex + From<N>,
{
    graph: Arc<RwLock<Graph<N, E, I>>>,
    client: GraphClient<N, E, I>,
    _server: GraphServer<N, E, I>,
}

impl<N, E, I> GraphDatabase<N, E, I>
where
    N: GraphNode + 'static,
    E: GraphEdge + 'static,
    I: GraphNodeIndex + From<N> + 'static,
{
    // constructors
    //
    pub async fn run(
        store_path: Option<String>,
        server_address: SocketAddr,
        remote_addresses: Vec<String>,
    ) -> Result<Arc<RwLock<Self>>, StoreError> {
        let graph = Arc::new(RwLock::new(Graph::new(store_path)?));
        log::debug!("Initialized graph");

        let (client, _server) = tokio::join!(
            GraphClient::new(remote_addresses),
            GraphServer::new(Arc::clone(&graph), server_address)
        );

        // TODO: no need to clone the whole database
        // a handler to use the public interface would be enought
        Ok(Arc::new(RwLock::new(Self {
            graph,
            client: client?,
            _server,
        })))
    }

    //
    // public interface
    //

    pub fn get_graph(&self) -> Result<StableGraph<N, E, Directed>, StoreError> {
        self.graph.read()?.get_graph()
    }

    pub fn filter_graph(
        &self,
        include_nodes: Option<Vec<N>>,
        include_edges: Option<Vec<E>>,
    ) -> Result<StableGraph<N, E, Directed>, StoreError> {
        self.graph
            .read()?
            .filter_graph(include_nodes, include_edges)
    }

    pub fn retain_nodes(
        &self,
        nodes_to_retain: Vec<&I>,
    ) -> Result<StableGraph<N, E, Directed>, StoreError> {
        self.graph.read()?.retain_nodes(nodes_to_retain)
    }

    pub fn get_neighbors(&self, key: &I) -> Result<Vec<N>, StoreError> {
        self.graph.read()?.get_neighbors(key)
    }

    pub fn get_edge(&self, from: &I, to: &I) -> Result<EdgeIndex, StoreError> {
        self.graph.read()?.get_edge(from, to)
    }

    pub fn get_edges(&self) -> Result<Vec<E>, StoreError> {
        self.graph.read()?.get_edges()
    }

    pub fn has_node(&self, key: &I) -> Result<bool, StoreError> {
        let graph = self.graph.read()?;

        Ok(graph.has_node(key))
    }

    pub fn get_node(&self, key: &I) -> Result<N, StoreError> {
        self.graph.read()?.get_node(key)
    }

    pub fn get_nodes(&self) -> Result<Vec<N>, StoreError> {
        self.graph.read()?.get_nodes()
    }

    pub fn get_node_index(&self, key: &I) -> Result<NodeIndex, StoreError> {
        self.graph.read()?.get_node_index(key)
    }

    pub fn get_source_nodes(&self) -> Result<Vec<N>, StoreError> {
        self.graph.read()?.get_source_nodes()
    }

    pub fn get_sink_nodes(&self) -> Result<Vec<N>, StoreError> {
        self.graph.read()?.get_sink_nodes()
    }
}

impl<N, E, I> MutateGraph<N, E, I> for GraphDatabase<N, E, I>
where
    N: GraphNode,
    E: GraphEdge,
    I: GraphNodeIndex + From<N>,
{
    fn add_edge(
        &mut self,
        from: &I,
        to: &I,
        edge: E,
    ) -> Result<(), StoreError> {
        let result = self.graph.write()?.add_edge(from, to, edge.clone());
        self.client.add_edge(from, to, edge)?;
        result
    }

    fn remove_edge(&mut self, from: &I, to: &I) -> Result<E, StoreError> {
        let result = self.graph.write()?.remove_edge(from, to);
        self.client.remove_edge(from, to)?;
        result
    }

    fn add_node(&mut self, key: I, node: N) -> Result<N, StoreError> {
        let result = self.graph.write()?.add_node(key.clone(), node.clone());
        self.client.add_node(key, node)?;
        result
    }

    fn remove_node(&mut self, key: I) -> Result<N, StoreError> {
        let result = self.graph.write()?.remove_node(key.clone());
        self.client.remove_node(key)?;
        result
    }
}
