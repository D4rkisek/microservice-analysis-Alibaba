use std::fs;
use std::fs::File;
use std::io::{self, BufReader, Read, Write};
use std::path::Path;
use std::time::Instant;
use csv::ReaderBuilder;
use flate2::read::GzDecoder;
use tar::Archive;
use rayon::prelude::*;
use serde::{Serialize, Deserialize};
use csv::WriterBuilder;
use csv::Trim;
use serde_derive::Serialize;
use serde_derive::Deserialize;


#[derive(Serialize, Deserialize)]
struct CallGraphRow {
    timestamp: i64,
    um: String,
    dm: String,
}


fn parallel_extract_tar_gz(tar_path: &str, output_dir: &str) -> std::io::Result<()> {
    let tar_gz = File::open(tar_path)?;
    let tar = GzDecoder::new(tar_gz);
    let mut archive = Archive::new(tar);
    let entries = archive.entries()?;

    // Collect file data into a Vec to allow for parallel processing
    let file_data: Result<Vec<(Vec<u8>, String)>, io::Error> = entries
        .map(|file| {
            let mut file = file?;
            let mut contents = Vec::new();
            file.read_to_end(&mut contents)?;
            let path = file.path()?.into_owned().display().to_string();
            Ok((contents, path))
        })
        .collect();

    let file_data = file_data?;

    // Process each file in parallel
    file_data.into_par_iter().try_for_each(|(contents, path)| {
        let output_file_path = Path::new(output_dir).join(path);
        let mut output_file = File::create(output_file_path)?;

        // Write the contents of the file
        output_file.write_all(&contents)?;
        Ok(())
    })

    //archive.unpack(output_dir)?;
    //Ok(())
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

fn delete_tar_gz_file(tar_path: &str) -> std::io::Result<()> {
    fs::remove_file(tar_path)
}

fn extract_tar_gz(tar_path: &str, output_dir: &str) -> std::io::Result<()> {
    let tar_gz = File::open(tar_path)?;
    let tar = GzDecoder::new(tar_gz);
    let mut archive = Archive::new(tar);
    archive.unpack(output_dir)?;
    Ok(())
}


fn main() {
    let start = Instant::now(); // Capture the start time
    if let Err(e) = extract_tar_gz("C:/Users/maruf/Downloads/Alibaba-clusterData-master/cluster-trace-microservices-v2022/data/CallGraph/CallGraph_4.tar.gz",
                                   "C:/Users/maruf/Downloads/Alibaba-clusterData-master/cluster-trace-microservices-v2022/data/CallGraph"){
        eprintln!("Failed to extract: {}", e);
    }

    preprocess_and_load_data_in_memory("C:/Users/maruf/Downloads/Alibaba-clusterData-master/cluster-trace-microservices-v2022/data/CallGraph/CallGraph_4.csv");

    delete_tar_gz_file("C:/Users/maruf/Downloads/Alibaba-clusterData-master/cluster-trace-microservices-v2022/data/CallGraph/CallGraph_4.tar.gz");

    let duration = start.elapsed(); // Calculate the duration since the start time
    println!("Time elapsed in the function with parallelism is: {:?}", duration);
}

/*
fn main(){
    let start = Instant::now(); // Capture the start time

    parallel_extract_tar_gz("C:/Users/maruf/Downloads/Alibaba-clusterData-master/cluster-trace-microservices-v2022/data/CallGraph/CallGraph_2.tar.gz",
                   "C:/Users/maruf/Downloads/Alibaba-clusterData-master/cluster-trace-microservices-v2022/data/CallGraph");

    let duration = start.elapsed(); // Calculate the duration since the start time

    println!("Time elapsed in the function with parallelism is: {:?}", duration);

}


fn main() {
    let file_path = "C:/Users/maruf/Downloads/Alibaba-clusterData-master/cluster-trace-microservices-v2022/data/CallGraph/CallGraph_4.tar.gz";
    let path = Path::new(file_path);
    let absolute_path = path.canonicalize().expect("Failed to get absolute path");
    println!("Attempting to open file at: {:?}", absolute_path);

    if !path.exists() {
        println!("File does not exist at the specified path.");
    } else {
        // proceed with file processing
    }
}
*/