use raphtory::{
    graph_loader::source::csv_loader::CsvLoader, 
    prelude::*,
};
use serde_derive::Deserialize;
use std::{
    env,
    path::{Path, PathBuf},
    time::Instant,
};

#[derive(Deserialize, std::fmt::Debug)]
pub struct CallGraphRow {
    timestamp: i64,
    um: String,
    dm: String,
}

fn main(){
    let args: Vec<String> = env::args().collect();

    let default_data_dir: PathBuf = [env!("CARGO_MANIFEST_DIR"), "C:/Users/maruf/OneDrive/Desktop/CallingGraph"]
        .iter()
        .collect();

    let data_dir = if args.len() < 2 {
        &default_data_dir
    } else {
        Path::new(args.get(1).unwrap())
    };

    if !data_dir.exists() {
        panic!("Missing data dir = {}", data_dir.to_str().unwrap())
    }

    let encoded_data_dir = data_dir.join("graphdb.bincode");

    let graph = if encoded_data_dir.exists() {
        let now = Instant::now();
        let g = Graph::load_from_file(encoded_data_dir.as_path())
            .expect("Failed to load graph from encoded data files");

        println!(
            "Loaded graph from encoded data files {} with {} nodes, {} edges which took {} seconds",
            encoded_data_dir.to_str().unwrap(),
            g.count_nodes(),
            g.count_edges(),
            now.elapsed().as_secs()
        );

        g

    }else{
        let g = Graph::new();
        let now = Instant::now();

        CsvLoader::new(data_dir)
            .set_header(true)
            .load_into_graph(&g, |row: CallGraphRow, g: &Graph| {
                g.add_edge(
                    row.timestamp,
                    row.um.clone(),
                    row.dm.clone(),
                    NO_PROPS,
                    None
                )
                .expect("Failed to add edge");
            })
            .expect("Failed to load graph from CSV data files");

        println!(
            "Loaded graph from CSV data files {} with {} nodes, {} edges which took {} seconds",
            data_dir.to_str().unwrap(),
            g.count_nodes(),
            g.count_edges(),
            now.elapsed().as_secs()
        );

        g.save_to_file(encoded_data_dir)
            .expect("Failed to save graph");

        g
    
    };
}