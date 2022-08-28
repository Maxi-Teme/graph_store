use actix::{
    Actor, ActorFutureExt, Addr, Context, Handler,
    ResponseActFuture, WrapFuture,
};
use tonic::transport::Endpoint;

use crate::{
    client::GraphClient, GraphEdge, GraphNode, GraphNodeIndex,
    GraphMutation, RemotesMessage, StoreError, GraphResponse,
};

pub struct Remotes {
    remote_addresses: Vec<Addr<GraphClient>>,
}

impl Remotes {
    pub async fn new(
        server_address: String,
        initial_addresses: Vec<String>,
    ) -> Self {
        let mut remote_addresses = Vec::new();

        for address in initial_addresses {
            let endpoint: Result<Endpoint, _> = address.try_into();

            if let Ok(endpoint) = endpoint {
                if let Ok(client) = GraphClient::new(endpoint.clone()).await {
                    let client = client.start();
                    client.do_send(RemotesMessage(server_address.clone()));
                    remote_addresses.push(client);
                }
            }
        }

        log::info!(
            "Remotes initialized with initial clients: '{:?}'",
            remote_addresses
        );

        Self { remote_addresses }
    }
}

impl Actor for Remotes {
    type Context = Context<Self>;
}

impl Handler<RemotesMessage> for Remotes {
    // type Result = Result<(), StoreError>;
    type Result = ResponseActFuture<Self, Result<(), StoreError>>;

    fn handle(
        &mut self,
        msg: RemotesMessage,
        _: &mut Self::Context,
    ) -> Self::Result {
        let future = Box::pin(async move {
            let endpoint: Endpoint = match msg.0.try_into() {
                Ok(endpoint) => endpoint,
                Err(err) => {
                    log::error!(
                        "Error while parsing address from \
message to Endpoint. Error: '{err}'",
                    );
                    return Err(StoreError::ClientError);
                }
            };

            let graph_client = match GraphClient::new(endpoint.clone()).await {
                Ok(client) => client,
                Err(err) => {
                    log::error!(
                        "Could not connect to new remote: '{}'. Error: '{:?}'",
                        endpoint.uri().to_string(),
                        err
                    );
                    return Err(StoreError::ClientError);
                }
            };

            let client_address = graph_client.start();

            Ok(client_address)
        });

        let actor_future = future.into_actor(self);

        let add_remote = actor_future.map(|result, actor, _ctx| match result {
            Ok(client) => {
                actor.remote_addresses.push(client);
                Ok(())
            }
            Err(err) => {
                log::error!(
                    "Error resolving add_remote future. Error: '{:?}'",
                    err
                );
                Err(StoreError::ClientError)
            }
        });

        Box::pin(add_remote)
    }
}

impl<N, E, I> Handler<GraphMutation<N, E, I>> for Remotes
where
    N: GraphNode + Unpin + 'static,
    E: GraphEdge + Unpin + 'static,
    I: GraphNodeIndex + From<N> + Unpin + 'static,
{
    type Result = Result<GraphResponse<N, E, I>, StoreError>;

    fn handle(
        &mut self,
        msg: GraphMutation<N, E, I>,
        _: &mut Self::Context,
    ) -> Self::Result {
        match msg {
            GraphMutation::AddEdge(query) => {
                let msg = GraphMutation::AddEdge(query);

                for address in self.remote_addresses.iter() {
                    address.do_send(msg.clone())
                }
            }
            GraphMutation::RemoveEdge(query) => {
                let msg = GraphMutation::<N, E, I>::RemoveEdge(query);

                for address in self.remote_addresses.iter() {
                    address.do_send(msg.clone())
                }
            }
            GraphMutation::AddNode(query) => {
                let msg = GraphMutation::<N, E, I>::AddNode(query);

                for address in self.remote_addresses.iter() {
                    address.do_send(msg.clone())
                }
            }
            GraphMutation::RemoveNode(query) => {
                let msg = GraphMutation::<N, E, I>::RemoveNode(query);

                for address in self.remote_addresses.iter() {
                    address.do_send(msg.clone())
                }
            }
        };

        Ok(GraphResponse::Empty)
    }
}
