use std::fmt::{Debug, Display};
use std::fs;
use std::marker::PhantomData;
use std::path::Path;

use petgraph::stable_graph::StableGraph;
use petgraph::Directed;

use crate::{GraphEdge, GraphNode, StoreError};

const DEFAULT_STORE_PATH: &str = "data/graph_store";

#[derive(Debug, Clone)]
pub struct GraphStore<N, E> {
    store_path: String,
    phantom_n: PhantomData<N>,
    phantom_e: PhantomData<E>,
}

impl<N, E> GraphStore<N, E>
where
    N: Debug + GraphNode,
    E: Debug + GraphEdge,
{
    // constructors
    //

    pub fn new(store_path: Option<String>) -> Result<Self, StoreError> {
        let store_path = match store_path {
            Some(path) => path,
            None => DEFAULT_STORE_PATH.to_string(),
        };

        fs::create_dir_all(store_path.clone()).map_err(|err| {
            log::error!(
                "Error creating store data directories. Error: '{}'",
                err
            );
            StoreError::StoreError
        })?;

        Ok(Self {
            store_path,
            phantom_n: PhantomData,
            phantom_e: PhantomData,
        })
    }

    //
    // public interface
    //

    pub fn save_to_file(
        &self,
        data: StableGraph<N, E, Directed>,
    ) -> Result<StableGraph<N, E, Directed>, StoreError> {
        let store_filepath = Self::get_filepath(self.store_path.clone());

        let encoded = bincode::serialize(&data)
            .map_err(|err| StoreError::FileSaveError(err.to_string()))?;

        savefile::save_file(store_filepath, 0, &encoded)
            .map_err(|err| StoreError::FileSaveError(err.to_string()))?;

        Ok(data)
    }

    pub fn load_from_file(
        &self,
    ) -> Result<StableGraph<N, E, Directed>, StoreError> {
        let store_filepath = Self::get_filepath(self.store_path.clone());

        if !Path::new(&store_filepath).exists() {
            let default = StableGraph::default();
            return self.save_to_file(default);
        }

        let loaded_bincode: Vec<u8> = savefile::load_file(store_filepath, 0)
            .map_err(|err| StoreError::FileLoadError(err.to_string()))?;

        let decoded = bincode::deserialize::<StableGraph<N, E, Directed>>(
            &loaded_bincode[..],
        )
        .map_err(|err| StoreError::FileDecodeError(err.to_string()))?;

        Ok(decoded)
    }

    //
    // private methods
    //

    fn get_filepath<S: Display>(store_path: S) -> String {
        format!("{}/data.bin", store_path)
    }
}

impl<N, E> Default for GraphStore<N, E> {
    fn default() -> Self {
        let store_path = DEFAULT_STORE_PATH.to_string();
        Self {
            store_path,
            phantom_n: PhantomData,
            phantom_e: PhantomData,
        }
    }
}
