use std::collections::HashMap;
use std::error::Error;
use std::marker::PhantomData;

use actix::{Actor, Context, Handler, ResponseActFuture};
use actix_interop::FutureInterop;
use futures_util::TryFutureExt;
use tonic::transport::{Channel, Endpoint};
use tonic::Request;

use crate::mutations_log::{MutationsLogMutation, MutationsLogQuery};
use crate::sync_graph::sync_graph_client::SyncGraphClient;
use crate::sync_graph::{
    GraphMutationRequest, MutationsLogRequest, MutationsLogResponse,
    RemotesLogRequest,
};
use crate::{
    GraphEdge, GraphNode, GraphNodeIndex, StoreError, SyncRemotesMessage,
};

#[derive(Debug, Clone)]
pub struct GraphClient<N, E, I>
where
    N: GraphNode + Unpin + 'static,
    E: GraphEdge + Unpin + 'static,
    I: GraphNodeIndex + From<N> + Unpin + 'static,
{
    client: SyncGraphClient<Channel>,
    phantom_n: PhantomData<N>,
    phantom_e: PhantomData<E>,
    phantom_i: PhantomData<I>,
}

impl<N, E, I> Actor for GraphClient<N, E, I>
where
    N: GraphNode + Unpin + 'static,
    E: GraphEdge + Unpin + 'static,
    I: GraphNodeIndex + From<N> + Unpin + 'static,
{
    type Context = Context<Self>;
}

impl<N, E, I> GraphClient<N, E, I>
where
    N: GraphNode + Unpin + 'static,
    E: GraphEdge + Unpin + 'static,
    I: GraphNodeIndex + From<N> + Unpin + 'static,
{
    pub async fn new(address: String) -> Result<Self, StoreError> {
        let endpoint = Endpoint::try_from(address)
            .map_err(|_err| StoreError::ParseError)?;

        let client = SyncGraphClient::connect(endpoint.clone())
            .map_err(|err| {
                log::warn!(
                    "Could not connect client at {:?}. Error: '{}' source: '{:?}'",
                    endpoint.uri(),
                    err,
                    err.source(),
                );
                StoreError::ClientError
            })
            .await?;

        Ok(Self {
            client,
            phantom_n: PhantomData,
            phantom_e: PhantomData,
            phantom_i: PhantomData,
        })
    }
}

impl<N, E, I> Handler<MutationsLogMutation<N, E, I>> for GraphClient<N, E, I>
where
    N: GraphNode + Unpin + 'static,
    E: GraphEdge + Unpin + 'static,
    I: GraphNodeIndex + From<N> + Unpin + 'static,
{
    type Result = ResponseActFuture<Self, Result<(), StoreError>>;

    fn handle(
        &mut self,
        msg: MutationsLogMutation<N, E, I>,
        _ctx: &mut Self::Context,
    ) -> Self::Result {
        let mut client = self.client.clone();

        async move {
            let request: GraphMutationRequest = msg.try_into()?;

            if let Err(err) = client.graph_mutation(request).await {
                log::error!(
                    "Error while sending 'AddEdgeRequest' request. Error: {err}"
                );
            };

            Ok(())
        }
        .interop_actor_boxed(self)
    }
}

impl<N, E, I> Handler<SyncRemotesMessage> for GraphClient<N, E, I>
where
    N: GraphNode + Unpin + 'static,
    E: GraphEdge + Unpin + 'static,
    I: GraphNodeIndex + From<N> + Unpin + 'static,
{
    type Result =
        ResponseActFuture<Self, Result<HashMap<String, bool>, StoreError>>;

    fn handle(
        &mut self,
        msg: SyncRemotesMessage,
        _ctx: &mut Self::Context,
    ) -> Self::Result {
        let SyncRemotesMessage {
            from,
            flat_remotes_log,
        } = msg;

        let request = Request::new(RemotesLogRequest {
            from_server: from,
            remotes_log: flat_remotes_log,
        });

        let mut client = self.client.clone();

        async move {
            match client.sync_remotes(request).await {
                Ok(response) => {
                    let response = response.into_inner();
                    Ok(response.remotes_log)
                }
                Err(err) => {
                    log::error!(
                        "Error sending add_remote request. Error: '{err}'"
                    );
                    Err(StoreError::ClientError)
                }
            }
        }
        .interop_actor_boxed(self)
    }
}

impl<N, E, I> Handler<MutationsLogQuery<N, E, I>> for GraphClient<N, E, I>
where
    N: GraphNode + Unpin + 'static,
    E: GraphEdge + Unpin + 'static,
    I: GraphNodeIndex + From<N> + Unpin + 'static,
{
    type Result = ResponseActFuture<
        Self,
        Result<Vec<MutationsLogMutation<N, E, I>>, StoreError>,
    >;

    fn handle(
        &mut self,
        _msg: MutationsLogQuery<N, E, I>,
        _ctx: &mut Self::Context,
    ) -> Self::Result {
        let mut client = self.client.clone();

        async move {
            match client
                .sync_mutations_log(Request::new(MutationsLogRequest {}))
                .await
            {
                Ok(response) => {
                    let MutationsLogResponse { mutations_log } =
                        response.into_inner();

                    let mutations_log = bincode::deserialize(&mutations_log)
                        .map_err(|_err| StoreError::ParseError)?;

                    Ok(mutations_log)
                }
                Err(err) => {
                    log::error!(
                        "Error getting MutationsLog from remote. Error: '{err:?}'",
                    );

                    Err(StoreError::ParseError)
                },
            }
        }
        .interop_actor_boxed(self)
    }
}
