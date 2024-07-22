This research aims to conduct a complex systems analysis of Alibaba’s microservices deployment. By leveraging
data from [Alibaba’s microarchitectural traces from 2022](https://github.com/alibaba/clusterdata/tree/master/cluster-trace-microservices-v2022), particularly the MSCallGraph dataset.
The primary problem addressed in this research is the understanding patterns in the system.
One of the key aspects of this research involves employing [Raphtory](https://github.com/pometry/raphtory) to determine its effectiveness as a
tool in this context. The ultimate goal is to fill a gap in current research - by seeing whether there is a primary contributing factor responsible for observed system behaviours.

* Implemented a monitoring tool for large-scale microservice systems using Rust and Python.
* Developed a program in Rust that leverages parallelisation to efficiently process terabytes of data on a single laptop, achieving a performance 4x faster.
* Used Raphtory, an open-source software for temporal graph analysis & Pandas DataFrames.


First please use main rust file to preprocess data. Secondly, load the data to raphtory either via get_data_to_raphtory rust file or use testing_raphtory python file. Both options are as equally good.
