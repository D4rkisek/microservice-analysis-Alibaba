use std::fs;
use std::fs::File;
use std::io::{self, BufReader, Read, Write};
use std::path::Path;
use std::time::Instant;
use csv::ReaderBuilder;
use flate2::read::GzDecoder;
use tar::Archive;
use rayon::prelude::*;
use serde_derive::Deserialize;

#[derive(Deserialize)]
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
/*
fn preprocess_and_load_data(file_path: &str) -> Vec<CallGraphRow> {
    let tar_gz = File::open(file_path).expect("Failed to open tar.gz file");
    let tar = GzDecoder::new(BufReader::new(tar_gz));
    let mut archive = Archive::new(tar);

    // Parallel processing of each file in the archive
    archive.entries().expect("Failed to read entries")
        .filter_map(Result::ok)
        .par_bridge() // Parallel iterator
        .filter_map(|file| {
            let mut rdr = ReaderBuilder::new().from_reader(file);
            rdr.deserialize::<CallGraphRow>()
                .filter_map(Result::ok)
                .filter(|row| row.timestamp != 0 && !row.um.is_empty() && !row.dm.is_empty()) // Apply filters similar to Python code
                .collect::<Vec<_>>() // Collect into Vec<CallGraphRow>
        })
        .flatten()
        .collect()
}
*/
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
    if let Err(e) = extract_tar_gz("C:/Users/maruf/Downloads/Alibaba-clusterData-master/cluster-trace-microservices-v2022/data/CallGraph/CallGraph_3.tar.gz",
                                   "C:/Users/maruf/Downloads/Alibaba-clusterData-master/cluster-trace-microservices-v2022/data/CallGraph"){
        eprintln!("Failed to extract: {}", e);
    }
    let _ = delete_tar_gz_file("C:/Users/maruf/Downloads/Alibaba-clusterData-master/cluster-trace-microservices-v2022/data/CallGraph/CallGraph_3.tar.gz");
}

/*
fn main(){
    let start = Instant::now(); // Capture the start time

    parallel_extract_tar_gz("C:/Users/maruf/Downloads/Alibaba-clusterData-master/cluster-trace-microservices-v2022/data/CallGraph/CallGraph_2.tar.gz",
                   "C:/Users/maruf/Downloads/Alibaba-clusterData-master/cluster-trace-microservices-v2022/data/CallGraph");

    let duration = start.elapsed(); // Calculate the duration since the start time

    println!("Time elapsed in the function with parallelism is: {:?}", duration);

}
*/