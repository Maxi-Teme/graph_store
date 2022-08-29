use std::marker::PhantomData;

use actix::{
    Actor, ActorFutureExt, Addr, Context, Handler, ResponseActFuture,
    WrapFuture, Message,
};
use rand::seq::SliceRandom;
use tonic::transport::Endpoint;

use crate::{
    client::GraphClient, GraphEdge, GraphMutation, GraphNode, GraphNodeIndex,
    GraphResponse, StoreError,
};

pub struct AddRemoteMessage(pub String);

impl Message for AddRemoteMessage {
    type Result = Result<(), StoreError>;
}

pub struct SendToNMessage<N, E, I>(pub GraphMutation<N, E, I>)
where
    N: GraphNode + 'static,
    E: GraphEdge + 'static,
    I: GraphNodeIndex + From<N> + 'static;

impl<N, E, I> Message for SendToNMessage<N, E, I>
where
    N: GraphNode + 'static,
    E: GraphEdge + 'static,
    I: GraphNodeIndex + From<N> + 'static,
{
    type Result = Result<(), StoreError>;
}

#[derive(Debug, Clone)]
pub struct Remotes<N, E, I>
where
    N: GraphNode + Unpin + 'static,
    E: GraphEdge + Unpin + 'static,
    I: GraphNodeIndex + From<N> + Unpin + 'static,
{
    remote_addresses: Vec<Addr<GraphClient<N, E, I>>>,
    phantom_n: PhantomData<N>,
    phantom_e: PhantomData<E>,
    phantom_i: PhantomData<I>,
}

impl<N, E, I> Remotes<N, E, I>
where
    N: GraphNode + Unpin + 'static,
    E: GraphEdge + Unpin + 'static,
    I: GraphNodeIndex + From<N> + Unpin + 'static,
{
    const SYNC_WITH_N: u8 = 2;

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
                    client.do_send(AddRemoteMessage(server_address.clone()));
                    remote_addresses.push(client);
                }
            }
        }

        log::info!(
            "Remotes initialized with initial clients: '{:?}'",
            remote_addresses
        );

        Self {
            remote_addresses,
            phantom_n: PhantomData,
            phantom_e: PhantomData,
            phantom_i: PhantomData,
        }
    }
}

impl<N, E, I> Actor for Remotes<N, E, I>
where
    N: GraphNode + Unpin + 'static,
    E: GraphEdge + Unpin + 'static,
    I: GraphNodeIndex + From<N> + Unpin + 'static,
{
    type Context = Context<Self>;
}

impl<N, E, I> Handler<AddRemoteMessage> for Remotes<N, E, I>
where
    N: GraphNode + Unpin + 'static,
    E: GraphEdge + Unpin + 'static,
    I: GraphNodeIndex + From<N> + Unpin + 'static,
{
    // type Result = Result<(), StoreError>;
    type Result = ResponseActFuture<Self, Result<(), StoreError>>;

    fn handle(
        &mut self,
        msg: AddRemoteMessage,
        _: &mut Self::Context,
    ) -> Self::Result {
        let address = msg.0;

        let future = Box::pin(async move {
            let endpoint: Endpoint = match address.try_into() {
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

        let actor_future =
            future
                .into_actor(self)
                .map(|result, actor, _ctx| match result {
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

        Box::pin(actor_future)
    }
}

impl<N, E, I> Handler<SendToNMessage<N, E, I>> for Remotes<N, E, I>
where
    N: GraphNode + Unpin + 'static,
    E: GraphEdge + Unpin + 'static,
    I: GraphNodeIndex + From<N> + Unpin + 'static,
{
    // type Result = Result<(), StoreError>;
    type Result = ResponseActFuture<Self, Result<(), StoreError>>;

    fn handle(
        &mut self,
        msg: SendToNMessage<N, E, I>,
        _: &mut Self::Context,
    ) -> Self::Result {
        let graph_mutation = msg.0;

        let mut rng = rand::thread_rng();
        let random_remotes = self.remote_addresses.clone();

        let future = Box::pin(async move {
            let random_remotes = random_remotes
                .choose_multiple(&mut rng, Self::SYNC_WITH_N.into());

            for address in random_remotes {
                address.send(graph_mutation.clone()).await??;
            }
            Ok(())
        });
        let actor_future = future.into_actor(self);

        Box::pin(actor_future)
    }
}

impl<N, E, I> Handler<GraphMutation<N, E, I>> for Remotes<N, E, I>
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
        for address in self.remote_addresses.iter() {
            address.do_send(msg.clone())
        }

        Ok(GraphResponse::Empty)
    }
}
