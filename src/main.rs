use std::fs;
use std::fs::File;
use csv::ReaderBuilder;
use flate2::read::GzDecoder;
use tar::Archive;
use rayon::prelude::*;
use rayon::ThreadPoolBuilder;
use csv::WriterBuilder;
use csv::Trim;
use serde_derive::Serialize;
use serde_derive::Deserialize;
//use std::io::{self, BufReader, Read, Write};
//use std::path::Path;
//use std::time::Instant;

#[derive(Serialize, Deserialize)]
struct CallGraphRow {
    timestamp: i64,
    um: String,
    dm: String,
}


fn preprocess_and_load_data_in_memory(file_path: &str) -> Result<(), csv::Error> {
    let mut rdr = ReaderBuilder::new()
        .trim(Trim::All)
        .from_path(file_path)?;

    let unwanted_values = vec!["UNKNOWN".to_string(), "UNAVAILABLE".to_string()];
    
    // Read the entire file into memory
    let records: Vec<CallGraphRow> = rdr.deserialize()
        .par_bridge()
        .filter_map(Result::ok)
        .filter(|row: &CallGraphRow| {
            row.timestamp != 0
                && !row.um.is_empty()
                && !row.dm.is_empty()
                && !unwanted_values.contains(&row.um)
                && !unwanted_values.contains(&row.dm)
        })
        .collect();

    // Overwrite the original file with processed data
    let file = File::create(file_path)?;
    let mut wtr = WriterBuilder::new().from_writer(file);

    for record in records {
        wtr.serialize(record)?;
    }

    Ok(())
}

fn extract_tar_gz(tar_path: &str, output_dir: &str) -> std::io::Result<()> {
    let tar_gz = File::open(tar_path)?;
    let tar = GzDecoder::new(tar_gz);
    let mut archive = Archive::new(tar);
    archive.unpack(output_dir)?;
    Ok(())
}


fn main() {
    // Configure Rayon to use 4 global threads
    let _pool = ThreadPoolBuilder::new().num_threads(4).build().unwrap();

    // Define base path
    let base_path = "C:/Users/maruf/Downloads/Alibaba-clusterData-master/cluster-trace-microservices-v2022/data/CallGraph/";

    // Generates paths dynamically along with their index
    let file_names: Vec<_> = (6..21)
    .map(|i| (format!("{}/CallGraph_{}.tar.gz", base_path, i), i))
    .collect();

    // Use parallel iterator to process files in parallel
    file_names.into_par_iter().for_each(|(file_path,i)| {

        // Attempt to extract the .tar.gz file
        if let Err(e) = extract_tar_gz(&file_path, &base_path) {
            eprintln!("Failed to extract file {}: {}", i, e);
        }

        // Remove the .tar.gz file after processing
        if let Err(e) = fs::remove_file(&file_path) {
            eprintln!("Failed to remove file {}: {}", i, e);
        }

        // Create a new file path with files ending now with .csv
        let csv_file_path = format!("{}/CallGraph_{}.csv", base_path, i);

        // Preprocess the data csv files
        match preprocess_and_load_data_in_memory(&csv_file_path) {
            Ok(_) => println!("Successfully preprocessed {}", csv_file_path),
            Err(e) => eprintln!("Failed to preprocess data from {}: {}", csv_file_path, e),
        }
    });

    println!("All files processed.");
}


/* 
fn main() {
    if let Err(e) = extract_tar_gz("C:/Users/maruf/Downloads/Alibaba-clusterData-master/cluster-trace-microservices-v2022/data/CallGraph/CallGraph_5.tar.gz",
                                   "C:/Users/maruf/Downloads/Alibaba-clusterData-master/cluster-trace-microservices-v2022/data/CallGraph"){
        eprintln!("Failed to extract: {}", e);
    }

    preprocess_and_load_data_in_memory("C:/Users/maruf/Downloads/Alibaba-clusterData-master/cluster-trace-microservices-v2022/data/CallGraph/CallGraph_5.csv");

    fs::remove_file("C:/Users/maruf/Downloads/Alibaba-clusterData-master/cluster-trace-microservices-v2022/data/CallGraph/CallGraph_5.tar.gz");
}

fn main(){
    let start = Instant::now();

    let csv_files = vec![
        "C:/Users/maruf/Downloads/Alibaba-clusterData-master/cluster-trace-microservices-v2022/data/CallGraph/CallGraph_0.csv",
        "C:/Users/maruf/Downloads/Alibaba-clusterData-master/cluster-trace-microservices-v2022/data/CallGraph/CallGraph_1.csv",
        "C:/Users/maruf/Downloads/Alibaba-clusterData-master/cluster-trace-microservices-v2022/data/CallGraph/CallGraph_2.csv",
        "C:/Users/maruf/Downloads/Alibaba-clusterData-master/cluster-trace-microservices-v2022/data/CallGraph/CallGraph_3.csv"
        // Add more paths as needed
    ];

    // Process files in parallel
    csv_files.par_iter().for_each(|file| {
        preprocess_and_load_data_in_memory(file);
    });

    let duration = start.elapsed();
    println!("Time elapsed in the function with parallelism is: {:?}", duration);
}*/