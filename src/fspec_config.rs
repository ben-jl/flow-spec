use std::path::{PathBuf};
use serde::{Serialize, Deserialize};

#[derive(Debug,Clone,PartialEq,Serialize,Deserialize)]
pub struct SpecEnvironmentConfiguration {
    fspec_directory: PathBuf
}

impl SpecEnvironmentConfiguration {
    pub fn default() -> SpecEnvironmentConfiguration {
        let fspec_directory = PathBuf::from(".fspec");
        SpecEnvironmentConfiguration { fspec_directory }
    }  

    pub fn fspec_directory(&self) -> &PathBuf {
        &self.fspec_directory
    }
}
