use std::marker::PhantomData;

use tokio::sync::broadcast::{self, Receiver, Sender};
use tonic::transport::Channel;
use tonic::Request;

use futures_util::stream::{self, StreamExt};

use crate::sync_graph::sync_graph_client::SyncGraphClient;
use crate::sync_graph::{
    AddEdgeRequest, AddNodeRequest, RemoveEdgeRequest, RemoveNodeRequest,
};
use crate::{GraphEdge, GraphNode, GraphNodeIndex, StoreError};

#[derive(Debug, Clone)]
enum Requests {
    AddEdge(AddEdgeRequest),
    RemoveEdge(RemoveEdgeRequest),
    AddNode(AddNodeRequest),
    RemoveNode(RemoveNodeRequest),
}

#[derive(Debug)]
struct Connection {
    client: SyncGraphClient<Channel>,
    rx: Receiver<Requests>,
}

pub struct GraphClient<N, E, I> {
    // connections: Vec<Connection>,
    tx: Sender<Requests>,
    phantom_n: PhantomData<N>,
    phantom_e: PhantomData<E>,
    phantom_i: PhantomData<I>,
}

impl<N, E, I> GraphClient<N, E, I>
where
    N: GraphNode,
    E: GraphEdge,
    I: GraphNodeIndex + From<N>,
{
    // constructors
    //

    pub async fn new(
        remote_addresses: Vec<String>,
    ) -> Result<Self, StoreError> {
        let (tx, _) = broadcast::channel(128);

        {
            let mut connections = Vec::new();

            for address in remote_addresses {
                if let Ok(client) = SyncGraphClient::connect(address).await {
                    let connection = Connection {
                        client,
                        rx: tx.subscribe(),
                    };
                    connections.push(connection);
                }
            }

            tokio::spawn(async move {
                Self::handle_connections(&mut connections).await;
            });

            log::debug!("Initialized client");
        }

        Ok(Self {
            tx,
            phantom_n: PhantomData,
            phantom_e: PhantomData,
            phantom_i: PhantomData,
        })
    }

    async fn handle_connections(connections: &mut [Connection]) {
        loop {
            stream::iter(connections.iter_mut())
                .for_each_concurrent(None, Self::handle_connection)
                .await;

            log::debug!("Waiting a bit then handle next connections");
            tokio::time::sleep(tokio::time::Duration::from_secs(4)).await;
        }
    }

    async fn handle_connection(connection: &mut Connection) {
        let request = match connection.rx.recv().await {
            Ok(request) => request,
            Err(err) => {
                return log::error!(
                    "Error receiving message from broadcast. Error: '{}'",
                    err
                )
            }
        };

        match request {
            Requests::AddEdge(add_edge_request) => {
                let req = Request::new(add_edge_request);
                if let Err(err) = connection.client.add_edge(req).await {
                    log::error!(
                        "Error while sending 'add_edge' request. Error: {err}"
                    );
                };
            }
            Requests::RemoveEdge(remove_edge_request) => {
                let req = Request::new(remove_edge_request);
                if let Err(err) = connection.client.remove_edge(req).await {
                    log::error!("Error while sending 'remove_edge' request. Error: {err}");
                };
            }
            Requests::AddNode(add_node_request) => {
                let req = Request::new(add_node_request);
                if let Err(err) = connection.client.add_node(req).await {
                    log::error!(
                        "Error while sending 'add_node' request. Error: {err}"
                    );
                };
            }
            Requests::RemoveNode(remove_node_request) => {
                let req = Request::new(remove_node_request);
                if let Err(err) = connection.client.remove_node(req).await {
                    log::error!("Error while sending 'remove_node' request. Error: {err}");
                };
            }
        }
    }

    pub fn add_edge(
        &mut self,
        from: &I,
        to: &I,
        edge: E,
    ) -> Result<(), StoreError> {
        let (from, to, edge) = match (
            bincode::serialize(&from),
            bincode::serialize(&to),
            bincode::serialize(&edge),
        ) {
            (Ok(from), Ok(to), Ok(edge)) => (from, to, edge),
            _ => return Err(StoreError::ParseError),
        };

        let request = Requests::AddEdge(AddEdgeRequest { from, to, edge });

        if let Err(err) = self.tx.send(request) {
            log::error!("Error while sending changes. Error: '{err}'");
        };

        Ok(())
    }

    pub fn remove_edge(&mut self, from: &I, to: &I) -> Result<(), StoreError> {
        let (from, to) =
            match (bincode::serialize(&from), bincode::serialize(&to)) {
                (Ok(from), Ok(to)) => (from, to),
                _ => return Err(StoreError::ParseError),
            };

        let request = Requests::RemoveEdge(RemoveEdgeRequest { from, to });

        if let Err(err) = self.tx.send(request) {
            log::error!("Error while sending changes. Error: '{err}'");
        };

        Ok(())
    }

    pub fn add_node(&mut self, key: I, node: N) -> Result<(), StoreError> {
        let (key, node) =
            match (bincode::serialize(&key), bincode::serialize(&node)) {
                (Ok(key), Ok(node)) => (key, node),
                _ => return Err(StoreError::ParseError),
            };

        let request = Requests::AddNode(AddNodeRequest { key, node });

        if let Err(err) = self.tx.send(request) {
            log::error!("Error while sending changes. Error: '{err}'");
        };

        Ok(())
    }

    pub fn remove_node(&mut self, key: I) -> Result<(), StoreError> {
        let key = match bincode::serialize(&key) {
            Ok(key) => key,
            _ => return Err(StoreError::ParseError),
        };

        let request = Requests::RemoveNode(RemoveNodeRequest { key });

        if let Err(err) = self.tx.send(request) {
            log::error!("Error while sending changes. Error: '{err}'");
        };

        Ok(())
    }
}
