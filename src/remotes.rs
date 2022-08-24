use actix::{Actor, Addr, Context, Handler, Message};

use crate::{
    client::GraphClient, GraphEdge, GraphNode, GraphNodeIndex,
    MutatinGraphQuery, StoreError,
};

pub enum RemotesTasks<N, E, I>
where
    N: GraphNode + Unpin + 'static,
    E: GraphEdge + Unpin + 'static,
    I: GraphNodeIndex + From<N> + Unpin + 'static,
{
    AddRemote(String),
    RemoveRemote(String),
    Broadcast(MutatinGraphQuery<N, E, I>),
}

impl<N, E, I> Message for RemotesTasks<N, E, I>
where
    N: GraphNode + Unpin + 'static,
    E: GraphEdge + Unpin + 'static,
    I: GraphNodeIndex + From<N> + Unpin + 'static,
{
    type Result = Result<(), StoreError>;
}

pub struct Remotes {
    client_addresses: Vec<Addr<GraphClient>>,
}

impl Remotes {
    pub async fn new(initial_client_addresses: Vec<String>) -> Self {
        let mut client_addresses = Vec::new();

        for address in initial_client_addresses {
            if let Ok(client) = GraphClient::new(address).await {
                let client = client.start();
                client_addresses.push(client);
            }
        }

        log::debug!(
            "Remotes initialized with initial clients: '{:#?}'",
            client_addresses
        );

        Self { client_addresses }
    }
}

impl Actor for Remotes {
    type Context = Context<Self>;
}

impl<N, E, I> Handler<RemotesTasks<N, E, I>> for Remotes
where
    N: GraphNode + Unpin + 'static,
    E: GraphEdge + Unpin + 'static,
    I: GraphNodeIndex + From<N> + Unpin + 'static,
{
    type Result = Result<(), StoreError>;

    fn handle(
        &mut self,
        msg: RemotesTasks<N, E, I>,
        _: &mut Self::Context,
    ) -> Self::Result {
        match msg {
            RemotesTasks::AddRemote(_) => {
                unimplemented!()
            }
            RemotesTasks::RemoveRemote(_) => {
                unimplemented!()
            }
            RemotesTasks::Broadcast(requests) => match requests {
                MutatinGraphQuery::AddEdge(query) => {
                    let msg = MutatinGraphQuery::AddEdge(query);

                    for address in &self.client_addresses {
                        address.do_send(msg.clone())
                    }
                }
                MutatinGraphQuery::RemoveEdge(query) => {
                    let msg = MutatinGraphQuery::<N, E, I>::RemoveEdge(query);

                    for address in &self.client_addresses {
                        address.do_send(msg.clone())
                    }
                }
                MutatinGraphQuery::AddNode(query) => {
                    let msg = MutatinGraphQuery::<N, E, I>::AddNode(query);

                    for address in &self.client_addresses {
                        address.do_send(msg.clone())
                    }
                }
                MutatinGraphQuery::RemoveNode(query) => {
                    let msg = MutatinGraphQuery::<N, E, I>::RemoveNode(query);

                    for address in &self.client_addresses {
                        address.do_send(msg.clone())
                    }
                }
            },
        };

        Ok(())
    }
}
