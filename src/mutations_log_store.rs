use std::marker::PhantomData;

use actix::{Actor, Context, Handler};
use rusqlite::{params, Connection, Error as SqliteError};

use crate::{
    mutations_log::{MutationsLogMutation, MutationsLogQuery},
    GraphEdge, GraphMutation, GraphNode, GraphNodeIndex, StoreError,
};

const DEFAULT_GRAPH_LOG_PATH: &str = "data/graph_store/log";

pub struct MutationsLogStore<N, E, I>
where
    N: GraphNode + Unpin + 'static,
    E: GraphEdge + Unpin + 'static,
    I: GraphNodeIndex + From<N> + Unpin + 'static,
{
    conn: Connection,
    phantom_n: PhantomData<N>,
    phantom_e: PhantomData<E>,
    phantom_i: PhantomData<I>,
}

impl<N, E, I> MutationsLogStore<N, E, I>
where
    N: GraphNode + Unpin + 'static,
    E: GraphEdge + Unpin + 'static,
    I: GraphNodeIndex + From<N> + Unpin + 'static,
{
    pub fn new(store_path: Option<String>) -> Self {
        let mutations_log_path = match store_path {
            Some(path) => format!("{}/log.sqlite", path),
            None => DEFAULT_GRAPH_LOG_PATH.to_string(),
        };

        let conn = Connection::open(mutations_log_path).unwrap();
        Self::init_log_table(&conn).unwrap();

        Self {
            conn,
            phantom_n: PhantomData,
            phantom_e: PhantomData,
            phantom_i: PhantomData,
        }
    }

    fn init_log_table(conn: &Connection) -> Result<(), rusqlite::Error> {
        conn.execute(
            "CREATE TABLE IF NOT EXISTS mutations_log (
                id CHAR(40) PRIMARY KEY NOT NULL UNIQUE,
                mutation BLOB NOT NULL
            )",
            (),
        )?;
        Ok(())
    }
}

impl<N, E, I> Actor for MutationsLogStore<N, E, I>
where
    N: GraphNode + Unpin + 'static,
    E: GraphEdge + Unpin + 'static,
    I: GraphNodeIndex + From<N> + Unpin + 'static,
{
    type Context = Context<Self>;
}

impl<N, E, I> Handler<MutationsLogMutation<N, E, I>>
    for MutationsLogStore<N, E, I>
where
    N: GraphNode + Unpin + 'static,
    E: GraphEdge + Unpin + 'static,
    I: GraphNodeIndex + From<N> + Unpin + 'static,
{
    type Result = Result<(), StoreError>;

    fn handle(
        &mut self,
        msg: MutationsLogMutation<N, E, I>,
        _ctx: &mut Self::Context,
    ) -> Self::Result {
        let mutation: Vec<u8> = msg.mutation.try_into()?;

        self.conn
            .execute(
                "INSERT INTO mutations_log (id, mutation)
                            VALUES (?1, ?2)",
                params![msg.hash, mutation],
            )
            .map_err(|err| StoreError::WriteLogError(err.to_string()))?;

        Ok(())
    }
}

impl<N, E, I> Handler<MutationsLogQuery<N, E, I>> for MutationsLogStore<N, E, I>
where
    N: GraphNode + Unpin + 'static,
    E: GraphEdge + Unpin + 'static,
    I: GraphNodeIndex + From<N> + Unpin + 'static,
{
    type Result = Result<Vec<MutationsLogMutation<N, E, I>>, StoreError>;

    fn handle(
        &mut self,
        _msg: MutationsLogQuery<N, E, I>,
        _ctx: &mut Self::Context,
    ) -> Self::Result {
        let mut statement = self
            .conn
            .prepare("SELECT id, mutation FROM mutations_log")?;

        let mutations_log_iter = statement
            .query_map([], |row| {
                let hash: String = row.get(0)?;
                let mutation: Vec<u8> = row.get(1)?;
                let mutation: GraphMutation<N, E, I> = mutation
                    .try_into()
                    .map_err(|_| SqliteError::ExecuteReturnedResults)?;

                Ok((hash, mutation))
            })
            .map_err(|err| StoreError::SqliteError(err.to_string()))?;

        let mut mutations_log = Vec::new();

        for row in mutations_log_iter {
            match row {
                Ok(mutation_entry) => {
                    mutations_log.push(MutationsLogMutation {
                        hash: mutation_entry.0,
                        mutation: mutation_entry.1,
                    });
                }
                Err(err) => log::error!(
                    "Error while building MutationsLog \
from sqlite file. Error: '{}'",
                    err
                ),
            }
        }

        Ok(mutations_log)
    }
}
