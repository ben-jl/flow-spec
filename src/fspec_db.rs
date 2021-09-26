use rusqlite::{Connection, params};
use crate::fspec_config::SpecEnvironmentConfiguration;
use log::{info,trace,debug,warn,error};
use anyhow::Error;
use crate::PipelineTypeSpecification;
use crate::PipelineTypeParameterTypeSchema;
pub struct FspecContextDb {
    fspec_config: SpecEnvironmentConfiguration,
    sqlite_conn: Connection
}

impl FspecContextDb {
    pub fn initialize(config: SpecEnvironmentConfiguration) -> anyhow::Result<FspecContextDb> {
        if !config.fspec_directory().exists() {
            info!("Fspec directory not found, attempting to create at {}", config.fspec_directory().display());
            std::fs::create_dir_all(config.fspec_directory())?;
        } 
        let db_path = config.fspec_directory().join("fspec.db");
        if !db_path.exists() {
            info!("Fspec database not found at {}, creating", db_path.display());
        } else {
            warn!("Fspec database already initialized, skipping create");
        }

        let sqlite_conn = Connection::open(config.fspec_directory().join("fspec.db"))?;
        

        let dbctx = FspecContextDb { fspec_config: config, sqlite_conn };
        dbctx.initialize_config_table()?;
        dbctx.initialize_pipeline_type_tables()?;
        dbctx.initialize_pipeline_tables()?;
        Ok(dbctx)
    }

    pub fn get_available_types(&self) -> anyhow::Result<Vec<PipelineTypeSpecification>> {
        let mut stmt = self.sqlite_conn.prepare("SELECT id, name FROM fspec_pipeline_type")?;
        let types = stmt.query_map([], |e| {
            let id : i32 = e.get(0)?;
            let name  : String = e.get(1)?;
           Ok((id,name))
        })?;

        let mut param_quer = self.sqlite_conn.prepare("SELECT id, parameter_name, required, parameter_type FROM fspec_pipeline_parameter_type WHERE pipeline_type_id = ?1")?;

        let mut resolved_types : Vec<PipelineTypeSpecification> = Vec::new();
        for t in types {
            let (id, name) = t?;
            trace!("Resolving parameters for type {} with id {}", &name, &id);
            let mut tparam : Vec<PipelineTypeParameterTypeSchema> = Vec::new();
            let ptypes = param_quer.query_map([&id], |pt| {
                let pid : i32 = pt.get(0)?;
                let pname : String = pt.get(1)?;
                let required : bool = pt.get(2)?;
                let param_type : String = pt.get(3)?;
                trace!("Resolved {} parameter (id={}, required={}, param_type={})", &pname, &pid, &required, &param_type);
                let resolved = PipelineTypeParameterTypeSchema::from_raw(pname, param_type, required).unwrap();
                Ok(resolved)
            })?;
            for r in ptypes {
                let unwrapped = r?;
                tparam.push(unwrapped);
            }
            let mut spec = PipelineTypeSpecification::from_name(&name)?;
            spec.with_parameter_types(tparam)?;
            resolved_types.push(spec);
        }
        Ok(resolved_types)
    }

    fn initialize_config_table(&self) -> anyhow::Result<()> {
        trace!("Ensuring configuration table");
        self.sqlite_conn.execute(r"
            CREATE TABLE IF NOT EXISTS fspec_config (
                fspec_directory TEXT UNIQUE NOT NULL,
                created_at TEXT NOT NULL
            )
        ", [])?;
        
        trace!("Ensuring context metadata exists in configuration table");
        self.sqlite_conn.execute(r"
        INSERT OR IGNORE INTO fspec_config (fspec_directory, created_at) VALUES (?1, datetime('now'))", params![self.fspec_config.fspec_directory().to_string_lossy()])?;

        let mut stmt = self.sqlite_conn.prepare("SELECT fspec_directory, created_at FROM fspec_config")?;
        
        let  config_table_rows = stmt.query_map([], |e| {
            let cfg_dir : String= e.get(0)?;
            let created_at : String = e.get(1)?;
            Ok((cfg_dir, created_at))
        })?;
        
        for r in config_table_rows.filter(|e| e.is_ok()) {
            let (cfg_dir,created_time) = r.unwrap();
            trace!("Fspec database has config dir {} and creation time {}", cfg_dir, created_time);
        }
        Ok(())
    }

    fn initialize_pipeline_type_tables(&self) -> anyhow::Result<()> {
        trace!("Ensuring pipeline type tables created and populated");
        self.sqlite_conn.execute(r"
            CREATE TABLE IF NOT EXISTS fspec_pipeline_type (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name VARCHAR(100) UNIQUE NOT NULL
            )
        ", [])?;
        trace!("Ensured fspec_pipeline_type exists");
        self.sqlite_conn.execute(r"
            CREATE TABLE IF NOT EXISTS fspec_pipeline_parameter_type (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pipeline_type_id INT NOT NULL,
                parameter_name VARCHAR(1024) UNIQUE NOT NULL,
                required INT NOT NULL,
                parameter_type VARCHAR(100) NOT NULL,
                FOREIGN KEY (pipeline_type_id) REFERENCES fspec_pipeline_type(id)
            )
        ", [])?;
        trace!("Ensured fspec_pipeline_parameter_type exists");

        let mut type_insert_stmt = self.sqlite_conn.prepare(r"
            INSERT OR IGNORE INTO fspec_pipeline_type (name) VALUES (?1)
        ")?;
        type_insert_stmt.execute(["HttpWebEndpoint"])?;
        type_insert_stmt.execute(["StaticList"])?;

        let mut select_stmt = self.sqlite_conn.prepare("SELECT id, name FROM fspec_pipeline_type ORDER BY name")?;
        let rs = select_stmt.query_map([],|r| {
            
            let n : String = r.get(1)?;
            let i : i32 = r.get(0)?;
            trace!("Pipeline type {} (ctx id {}) found", &n, &i);
            Ok((i,n))
        })?;

        let mut param_type_insert_stmt = self.sqlite_conn.prepare(r"
            INSERT OR IGNORE INTO fspec_pipeline_parameter_type (pipeline_type_id, parameter_name, required, parameter_type) VALUES (?1,?2,?3,?4)
        ")?;
        
        for res in rs {
            let (i,n) = res?;
            trace!("Loaded pipeline type {} (ctx id {}), ensuring parameter types configured", &n, &i);
            let ps = match n.as_str() {
                "HttpWebEndpoint" => Ok(vec![(i, "Url", 1, "ValidUrl")]),
                "StaticList" => Ok(vec![(i, "Values", 1, "StaticList")]),
                c => Err(std::io::Error::new(std::io::ErrorKind::NotFound, format!("Unrecognized pipeline type {}", c)))
            }?;
            for p in ps {
                trace!("Ensuring parameter type {} configured for pipeline type {}", p.1, &n);
                param_type_insert_stmt.execute(params!(p.0,p.1,p.2,p.3))?;
            }


        }
        Ok(())
    }

    fn initialize_pipeline_tables(&self) -> anyhow::Result<()> {
        trace!("Ensuring fspec_pipeline created");
        self.sqlite_conn.execute(r"
            CREATE TABLE IF NOT EXISTS fspec_pipeline (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name VARCHAR(1024),
                pipeline_type_id INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (pipeline_type_id) REFERENCES fspec_pipeline_type(id)
            )
        ", [])?;

        trace!("Ensuring fspec_pipeline_parameter_value created");
        self.sqlite_conn.execute(r"
            CREATE TABLE IF NOT EXISTS fspec_pipeline_parameter_value (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pipeline_id INTEGER NOT NULL,
                parameter_type_id INTEGER NOT NULL,
                parameter_value TEXT,
                FOREIGN KEY (pipeline_id) REFERENCES fspec_pipeline(id),
                FOREIGN KEY (parameter_type_id) REFERENCES fspec_pipeline_parameter_type(id),
                UNIQUE(pipeline_id,parameter_type_id)
                
            )
        ", [])?;
        Ok(())
    }
}