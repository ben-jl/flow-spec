pub mod fspec_config;
pub mod fspec_db;
use std::hash::Hash;
use log::{trace,debug,info,error,warn};
use url::{Url};
use serde::{Serialize,Deserialize};
use std::iter::{IntoIterator};

#[derive(Debug,Copy,Clone,PartialEq,PartialOrd,Eq,Hash)]
pub enum FspecCommand {
    Initialize,
    ListAvailableTypes
}

impl PipelineTypeSpecification {
    pub fn from_name(name: &str) -> anyhow::Result<PipelineTypeSpecification> {
        let ptype = match name{
            "HttpWebEndpoint" => PipelineType::HttpWebEndpoint,
            "StaticList" => PipelineType::StaticList,
            _ => return Err(anyhow::Error::from(std::io::Error::from(std::io::ErrorKind::InvalidInput)))
        };

        let pspec = PipelineTypeSpecification { parameter_types: vec![], pipeline_type: ptype };
        Ok(pspec)
    }

    pub fn with_parameter_types(&mut self, configured_types: Vec<PipelineTypeParameterTypeSchema>) -> anyhow::Result<&mut Self> {
        for pt in configured_types.iter() {
            self.parameter_types.push(pt.clone());
        }
        Ok(self)
    }
}



#[derive(Debug,Clone,PartialEq,Serialize,Deserialize)]
pub struct PipelineTypeSpecification {
    parameter_types: Vec<PipelineTypeParameterTypeSchema>,
    pipeline_type: PipelineType
}

#[derive(Debug,Clone,PartialEq,Serialize,Deserialize)]
pub enum PipelineType {
    HttpWebEndpoint,
    StaticList
}

impl PipelineTypeParameterTypeSchema {
    pub fn from_raw<B : ToString>(param_name: B, param_type_name: B, required: bool) -> anyhow::Result<PipelineTypeParameterTypeSchema>{
        trace!("Creating schema from raw parts param_name={} param_type_name={} required={}", &param_name.to_string(), &param_type_name.to_string(), &required);
        let pname = param_name.to_string();
        let ptype = param_type_name.to_string();
        let param_type = match ptype.as_str() {
            "ValidUrl" => FspecParameterType::ValidUrl(FspecValidUrlParameterTypeS {}),
            "StaticList" => FspecParameterType::StaticList(FspecStaticListParameterTypeS {}),
            _ => return Err(anyhow::Error::from(std::io::Error::from(std::io::ErrorKind::InvalidInput)))
        };

        Ok(PipelineTypeParameterTypeSchema { name: pname, param_type, required})

    }
}

#[derive(Debug,Clone,PartialEq,Serialize,Deserialize)]
pub struct PipelineTypeParameterTypeSchema {
    param_type: FspecParameterType,
    required: bool,
    name: String
}

#[derive(Debug,Clone,PartialEq,Serialize,Deserialize)]
pub enum FspecParameterType {
    ValidUrl(FspecValidUrlParameterTypeS),
    StaticList(FspecStaticListParameterTypeS)
}

#[derive(Debug,Clone,PartialEq,Serialize,Deserialize)]
pub struct FspecValidUrlParameterTypeS {

}


#[derive(Debug,Clone,PartialEq,Serialize,Deserialize)]
pub struct FspecStaticListParameterTypeS {
}

pub fn execute_fspec_command(command: FspecCommand, config: fspec_config::SpecEnvironmentConfiguration) -> anyhow::Result<()> {
    match command {
        FspecCommand::Initialize => {
            info!("Initializing fspec at {}", config.fspec_directory().display());
            let fspec_db_context = fspec_db::FspecContextDb::initialize(config)?;
            
            Ok(())
        },
        FspecCommand::ListAvailableTypes => {
            let fspec_db_context = fspec_db::FspecContextDb::initialize(config)?;
            let types = fspec_db_context.get_available_types()?;
            for t in types {
                println!("{}", t);
            }
            Ok(())
        }
    }
}

impl std::fmt::Display for PipelineTypeSpecification {
    
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> { 
        write!(f, "{:?} ({} params)", self.pipeline_type, &self.parameter_types.len())?;
        for pt in &self.parameter_types {
            write!(f, "\n    {} (required={}, type={:?})", pt.name, &pt.required, &pt.param_type)?;
        }
        Ok(())
     }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
