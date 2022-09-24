use std::collections::HashMap;
use std::marker::PhantomData;

use actix::{
    Actor, ActorFutureExt, Addr, Context, Handler, Message, ResponseActFuture,
    WrapFuture,
};

use actix_interop::FutureInterop;

use crate::graph::Graph;
use crate::mutations_log_store::MutationsLogStore;
use crate::remotes::Remotes;
use crate::sync_graph::GraphMutationRequest;
use crate::{
    GraphEdge, GraphMutation, GraphNode, GraphNodeIndex, GraphResponse,
    StoreError,
};

pub struct MutationsLog<N, E, I>
where
    N: GraphNode + Unpin + 'static,
    E: GraphEdge + Unpin + 'static,
    I: GraphNodeIndex + From<N> + Unpin + 'static,
{
    node_id: String,
    graph: Addr<Graph<N, E, I>>,
    remotes: Addr<Remotes<N, E, I>>,
    mutations_log_store: Addr<MutationsLogStore<N, E, I>>,
    pending_mutations_log: HashMap<String, GraphMutation<N, E, I>>,
}

impl<N, E, I> Actor for MutationsLog<N, E, I>
where
    N: GraphNode + Unpin + 'static,
    E: GraphEdge + Unpin + 'static,
    I: GraphNodeIndex + From<N> + Unpin + 'static,
{
    type Context = Context<Self>;
}

impl<N, E, I> MutationsLog<N, E, I>
where
    N: GraphNode + Unpin + 'static,
    E: GraphEdge + Unpin + 'static,
    I: GraphNodeIndex + From<N> + Unpin + 'static,
{
    //
    // constuctor

    pub async fn new(
        node_id: String,
        graph: Addr<Graph<N, E, I>>,
        remotes: Addr<Remotes<N, E, I>>,
        store_path: Option<String>,
    ) -> Result<Self, StoreError> {
        let mutations_log_store = MutationsLogStore::new(store_path).start();
        let pending_mutations_log = HashMap::new();

        Ok(Self {
            node_id,
            graph,
            remotes,
            mutations_log_store,
            pending_mutations_log,
        })
    }

    pub async fn commit_mutation(
        mutations_log_store_addr: &Addr<MutationsLogStore<N, E, I>>,
        graph_addr: &Addr<Graph<N, E, I>>,
        graph_mutation_log_entry: MutationsLogMutation<N, E, I>,
    ) -> Result<GraphResponse<N, E, I>, StoreError> {
        mutations_log_store_addr
            .send(graph_mutation_log_entry.clone())
            .await??;

        graph_addr.send(graph_mutation_log_entry.mutation).await?
    }
}

impl<N, E, I> Handler<GraphMutation<N, E, I>> for MutationsLog<N, E, I>
where
    N: GraphNode + Unpin + 'static,
    E: GraphEdge + Unpin + 'static,
    I: GraphNodeIndex + From<N> + Unpin + 'static,
{
    type Result =
        ResponseActFuture<Self, Result<GraphResponse<N, E, I>, StoreError>>;

    fn handle(
        &mut self,
        msg: GraphMutation<N, E, I>,
        _ctx: &mut Self::Context,
    ) -> Self::Result {
        let remotes = self.remotes.clone();
        let graph = self.graph.clone();
        let mutations_log_store = self.mutations_log_store.clone();
        let node_id = self.node_id.clone();
        let mut pending_mutations_log = self.pending_mutations_log.clone();

        async move {
            let hash = msg.get_hash(node_id.clone());
            pending_mutations_log.insert(hash, msg.clone());

            let query = MutationsLogMutation {
                hash,
                mutation: msg,
            };

            // includes also remotes to which we send
            // synchronous mutations
            remotes.do_send(query.clone());

            remotes.send(query.clone()).await??;

            Self::commit_mutation(&mutations_log_store, &graph, query).await
        }
        .interop_actor_boxed(self)
    }
}

#[derive(Debug, Clone)]
pub struct MutationsLogMutation<N, E, I>
where
    N: GraphNode + 'static,
    E: GraphEdge + 'static,
    I: GraphNodeIndex + From<N> + 'static,
{
    pub hash: String,
    pub mutation: GraphMutation<N, E, I>,
}

impl<N, E, I> TryFrom<GraphMutationRequest> for MutationsLogMutation<N, E, I>
where
    N: GraphNode + Unpin + 'static,
    E: GraphEdge + Unpin + 'static,
    I: GraphNodeIndex + From<N> + Unpin + 'static,
{
    type Error = StoreError;

    fn try_from(request: GraphMutationRequest) -> Result<Self, Self::Error> {
        let GraphMutationRequest {
            graph_mutation,
            hash,
        } = request;

        let mutation: GraphMutation<N, E, I> =
            bincode::deserialize(&graph_mutation)
                .map_err(|err| StoreError::Serde(err.to_string()))?;

        Ok(Self { hash, mutation })
    }
}

impl<N, E, I> TryInto<GraphMutationRequest> for MutationsLogMutation<N, E, I>
where
    N: GraphNode + Unpin + 'static,
    E: GraphEdge + Unpin + 'static,
    I: GraphNodeIndex + From<N> + Unpin + 'static,
{
    type Error = StoreError;

    fn try_into(self) -> Result<GraphMutationRequest, Self::Error> {
        let graph_mutation = bincode::serialize(&self.mutation)
            .map_err(|err| StoreError::Serde(err.to_string()))?;

        Ok(GraphMutationRequest {
            hash: self.hash,
            graph_mutation,
        })
    }
}

impl<N, E, I> Message for MutationsLogMutation<N, E, I>
where
    N: GraphNode + Unpin + 'static,
    E: GraphEdge + Unpin + 'static,
    I: GraphNodeIndex + From<N> + Unpin + 'static,
{
    type Result = Result<GraphResponse<N, E, I>, StoreError>;
}

impl<N, E, I> Handler<MutationsLogMutation<N, E, I>> for MutationsLog<N, E, I>
where
    N: GraphNode + Unpin + 'static,
    E: GraphEdge + Unpin + 'static,
    I: GraphNodeIndex + From<N> + Unpin + 'static,
{
    type Result =
        ResponseActFuture<Self, Result<GraphResponse<N, E, I>, StoreError>>;

    fn handle(
        &mut self,
        msg: MutationsLogMutation<N, E, I>,
        _ctx: &mut Self::Context,
    ) -> Self::Result {
        let mutations_log_store = self.mutations_log_store.clone();
        let graph = self.graph.clone();

        async move {
            Self::commit_mutation(&mutations_log_store, &graph, msg)
                .await
        }
        .interop_actor_boxed(self)
    }
}

pub enum MutationsLogQuery<N, E, I> {
    Full((PhantomData<N>, PhantomData<E>, PhantomData<I>)),
}

impl<N, E, I> MutationsLogQuery<N, E, I> {
    pub fn full() -> Self {
        Self::Full((PhantomData, PhantomData, PhantomData))
    }
}

impl<N, E, I> Message for MutationsLogQuery<N, E, I>
where
    N: GraphNode + 'static,
    E: GraphEdge + 'static,
    I: GraphNodeIndex + From<N> + 'static,
{
    type Result = Result<Vec<MutationsLogMutation<N, E, I>>, StoreError>;
}

impl<N, E, I> Handler<MutationsLogQuery<N, E, I>> for MutationsLog<N, E, I>
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
        msg: MutationsLogQuery<N, E, I>,
        _ctx: &mut Self::Context,
    ) -> Self::Result {
        let mutations_log_store = self.mutations_log_store.clone();

        async move { mutations_log_store.send(msg).await? }
            .interop_actor_boxed(self)
    }
}

pub struct InitializeMutationsLog;

impl Message for InitializeMutationsLog {
    type Result = Result<(), StoreError>;
}

impl<N, E, I> Handler<InitializeMutationsLog> for MutationsLog<N, E, I>
where
    N: GraphNode + Unpin + 'static,
    E: GraphEdge + Unpin + 'static,
    I: GraphNodeIndex + From<N> + Unpin + 'static,
{
    type Result = ResponseActFuture<Self, Result<(), StoreError>>;

    fn handle(
        &mut self,
        _msg: InitializeMutationsLog,
        _ctx: &mut Self::Context,
    ) -> Self::Result {
        let remotes = self.remotes.clone();
        let graph = self.graph.clone();
        let mutations_log_store = self.mutations_log_store.clone();
        let mut pending_mutations_log = self.pending_mutations_log.clone();
        let node_id = self.node_id.clone();

        let future = async move {
            let mutations_log_mutations =
                remotes.send(MutationsLogQuery::full()).await??;

            for mutation_log_mutation in mutations_log_mutations.into_iter() {
                if let Err(err) = Self::commit_mutation(
                    &mutations_log_store,
                    &graph,
                    mutation_log_mutation,
                )
                .await
                {
                    log::warn!(
                        "Error committing GraphMutations \
while synchronizing. Error: '{:?}'",
                        err
                    );

                    pending_mutations_log.insert(
                        mutation_log_mutation.hash,
                        mutation_log_mutation.mutation,
                    );
                }
            }

            Ok(pending_mutations_log)
        };

        let actor_future = future.into_actor(self).map(
            |pending_mutations: Result<
                HashMap<std::string::String, GraphMutation<N, E, I>>,
                StoreError,
            >,
             actor,
             _ctx| {
                actor.pending_mutations_log = pending_mutations?;
                Ok(())
            },
        );

        Box::pin(actor_future)
    }
}
