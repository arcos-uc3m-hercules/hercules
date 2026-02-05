<!-- Last modification 02/02/2026 
Jesús Carretero
Javier Garcia Blas
Genaro Sánchez-Gallegos
Cosmin Petre
-->

![Screenshot](hercules.png)

# Table of Contents

- [Main references](#main-references)
- [Design summary](#design-summary)
- [Use cases](#use-cases)
- [Download and installation](#download-and-installation)
  - [Method 1: Manual installation](#method-1-manual-installation)
  - [Method 2: Spack module](#method-2-spack-module)
- [Usage](#usage)
  - [Writing a configuration file](#writing-a-configuration-file)
  - [Option 1. Running with Slurm](#option-1-running-with-slurm)
    - [1. Deploying Metadata and Data storage servers](#1-deploying-metadata-and-data-storage-servers)
      - [1.1 Slurm script](#11-slurm-script)
      - [1.2 Directly from the terminal](#12-directly-from-the-terminal)
    - [2. Intercepting I/O calls](#2-intercepting-io-calls)
    - [3. Stopping Metadata and Data storage servers](#3-stopping-metadata-and-data-storage-servers)
  - [Without Slurm](#without-slurm)
  - [Configuration file](#configuration-file)
    - [Malleability parameters](#optional-malleability-parameters)
    - [Fault tolerance parameters](#optional-fault-tolerance-parameters)
    - [Debug parameters](#optional-debug-parameters)

# Main references
- [HERCULES: A scalable and elastic ad-hoc file system for large-scale computing systems](https://doi.org/10.1016/j.future.2025.108350) in Future Generation Computer Systems
- [Efficient Data Elasticity for HPC: A Malleable Ad-hoc In-memory File System for Ephemeral Data.
](https://doi.org/10.1145/3784828.3785165) in Proceedings of the Supercomputing Asia and International Conference on High Performance Computing in Asia Pacific Region Workshops (pp. 226-232).
- [
Hercules: Scalable and Network Portable In-Memory Ad-Hoc File System for Data-Centric and High-Performance Applications](https://doi.org/10.1007/978-3-031-39698-4_46) in Euro-Par 2023: Parallel Processing.
- [Hercules web page](https://arcos-uc3m-hercules.github.io/).


# Design summary

The architectural design of Hercules follows a client-server design model where the client itself will be responsible for the server entities deployment. We propose an application-attached deployment constrained to application's nodes and an application-detached considering offshore nodes. 

The development of the present work was strictly conditioned by a set of well-defined objectives. Firstly, Hercules should provide flexibility in terms of deployment. To achieve this, the Hercules API provides a set of deployment methods where the number of servers conforming the instance, as well as their locations, buffer sizes, and their coupled or decoupled nature, can be specified. Second, parallelism should be maximised. To achieve this, Hercules follows a multi-threaded design architecture. Each server in an instance has a dispatcher thread and a pool of worker threads. The dispatcher thread distributes the incoming workload between the worker threads with the aim of balancing the workload in a multi-threaded scenario. Main entities conforming the architectural design are Hercules clients (front-end), Hercules server (back-end), and Hercules metadata server. Addressing the interaction between these components, the Hercules client will exclusively communicate with the Hercules metadata server whenever a metadata-related operation is performed, such as: *create_dataset* and *open_imss*. Data-related operations (*get_data* & *set_data*) will be handled directly by the corresponding storage server. Finally, Hercules offers to the application a set of distribution policies at dataset level increasing the application's awareness about the location of the data. As a result, the storage system will increase awareness in terms of data distribution at the client side, providing benefits such as data locality exploitation and load balancing.

Hercules takes advantage of UCX in order to handle communications between the different entities conforming an Hercules instance. UCX has been qualified as one of the most efficient libraries for creating distributed applications. UCX provides multiple communication patterns across various transport layers, such as inter-threaded, inter-process, TCP, UDP, and multicast. 

Furthermore, to deal with the Hercules dynamic nature, a distributed metadata server, resembling CEPH model, was included in the design step. The metadata server is in charge of storing the structures representing each Hercules and dataset instances. Consequently, clients are able to join an already created Hercules as well as accessing an existing dataset among other operations. 

# Use cases


Two strategies were considered so as to adapt the storage system to the application's requirements. On the one hand, the *application-detached* strategy, consisting of deploying Hercules clients and servers as process entities on decoupled nodes. Hercules clients will be deployed in the same computing nodes as the application, using them to take advantage of all available computing resources within an HPC cluster, while Hercules servers will be in charge of storing the application datasets and enabling the storage's execution in application's offshore nodes. In this strategy, Hercules clients do not store data locally, as this deployment was thought to provide an application-detached possibility. In this way, persistent Hercules storage servers could be created by the system and would be executed longer than a specific application, so as to avoid additional storage initialisation overheads in execution time. Figure \ref{Deployments} (left) illustrates the topology of an Hercules application-detached deployment over a set of compute and/or storage nodes where the Hercules instance does not belong to the application context nor its nodes.


On the other hand, the *application-attached* deployment strategy seeks empowering locality exploitation constraining deployment possibilities to the set of nodes where the application is running, so that each application node will also include an Hercules client and an Hercules server, deployed as a thread within the application. Consequently, data could be forced to be sent and retrieved from the same node, thus maximising locality possibilities for data. In this approach each process conforming the application will invoke a method initialising certain in-memory store resources preparing for future deployments. However, as the attached deployment executes in the applications machine, the amount of memory used by the storage system turns into a matter of concern. Considering that unexpectedly bigger memory buffers may harm the applications performance, we took the decision of letting the application determine the memory space that a set of servers (storage and metadata) executing in the same machine shall use through a parameter in the previous method. This decision was made because the final user is the only one conscious about the execution environment as well as the applications memory requirements. Flexibility aside, as main memory will be used as storage device, an in-memory store will be implemented so as to achieve faster data-related request management. Figure \ref{Deployments} (right) displays the topology of an Hercules application-attached deployment where the Hercules instance is contained within the application.



# Download and installation

## Method 1: Manual installation

Clone the project from the project GIT repository:

```
git clone https://github.com/arcos-uc3m-hercules/hercules.git
```

The following software packages are required for the compilation of Hercules:

- CMake >= 3.5
- UCX >= 1.14
- Glib >= 2.78.3
- (optional) MPI (MPICH or OpenMPI) 


We have prepared two scripts to install and load Hercules's dependencies.
```
# Install missing dependencies.
bash scripts/c3/c3-install-dependencies.sh
# Load Hercules dependencies.
source scripts/c3/c3-load-dependencies.sh
```
Hercules is a CMAKE-based project, so the compilation process is quite simple. 

**Option 1**.  If **Slurm** is available, run the following script (in the root project) to allocate a node, load the dependencies and install Hercules:
```
sbatch scripts/c3/c3-slurm-hercules-compilation.sh
```

**Option 2**. If Slurm is not avaible, run the following commands:
```
cmake --preset default
cmake --build --preset default --target install
```

As a result the project generates the following outputs:
<!-- - mount.imss: run as daemons the necessary instances for Hercules. Later, it enables the usage of the interception library with execution persistency. -->
<!-- - umount.imss: umount the file system by killing the deployed processes. -->
- build: the build directory.
- install: the installation directory.
- **install/bin/hercules**: Hercules's deployment script.
- **install/bin/hercules_server**: binary to run a data or metadata storage server.
- install/conf/hercules.conf.sample: an example of the Hercules's configuration file (must be modified according to your needs).
- **install/lib/libhercules_posix.so**: dynamic library for intercepting I/O calls.
- install/lib/libhercules_shared.so: dynamic library of Hercules's API.
- install/lib/libhercules_static.a: static library of Hercules's API.
<!-- - imfssfs: application for mounting Hercules at user space by using FUSE engine. -->
    
## Method 2: Spack module

Clone the project from the project GIT repository:

```
git clone https://github.com/arcos-uc3m-hercules/hercules.git
```

Now, you can add repositories under the admire namespace:

```
cd hercules
spack repo add spack
spack install hercules
spack load hercules
```

# Usage

The current prototype of Hercules enables the access to the storage infrastructure in two different ways: API library and LD_PRELOAD by overriding symbols. In the following subsection we describe the characteristics of the preferred option.

## Writing a configuration file
Before running Hercules, you have to define a configuration file, please refer to [Configuration file](#configuration-file) section for more details.

A template configuration file can be found in [conf/hercules-template.conf](conf/hercules-template.conf)

## Option 1. Running with Slurm 
If available, Hercules can be run with Slurm. 
### 1. Deploying Metadata and Data storage servers
We provide a script that launches an Hercules deployment script (_scripts/hercules_). This script reads all initialization parameters from a provided configuration file and creates the metadata and data hostfiles.

Custom configuration files can be specified launching Hercules in this manner, where "CONF_PATH" is the path to the **configuration file** :
```
install/bin/hercules start -f <CONF_PATH>
```
> Note: If no configuration file is provided, Hercules looks for default files _/etc/hercules.conf_, _./hercules.conf_, _hercules.conf_, or _<PROJECT_PATH>/conf/hercules.conf_ in that order.

The Hercules deployment script can be called from 1) a Slurm script or 2) directly from the terminal. 
### 1.1 Slurm script
This is the easiest way to run your tests with Hercules, as you only have to write a Slurm script:
```
#!/bin/bash
#SBATCH --job-name=hercules    # Job name
#SBATCH --output=logs/hercules/%j-ior.log   # Standard output and error log
#SBATCH --time=1:00:00               # Time limit hrs:min:sec
#SBATCH --nodes=3
#SBATCH --cpus-per-task=4
#SBATCH --ntasks-per-node=32 # fix the mpirun error: "There are not enough slots available in the system."

CONFIG_PATH=$1
echo "Configuration file content:"
echo "------------------"
cat "${CONFIG_PATH}"
echo "------------------"

## load packages.
source "scripts/c3/c3-load-dependencies.sh"

echo "Starting Hercules Servers"
start_=$(date +%s.%N)
if [ -z "$CONFIG_PATH" ]; then
   echo "No configuration file"
   source hercules start
else
   echo "Configuration file pass $CONFIG_PATH"
   source /home/user/hercules/scripts/hercules start \
   -f "$CONFIG_PATH"
fi
end_=$(date +%s.%N)
runtime=$(echo "$end_ - $start_" | bc -l)
echo "Hercules started in $runtime seconds, start=$start_, end=$end_"
...
```
We provide some [examples](scripts/c3/examples/) of scripts to run multiple Hercules deployments without the need to manually modify the configuration file. 

#### 1.2 Directly from the terminal
1. First, you have to allocate the number of nodes equals to the sum of the number of data storage servers + the number of metadata storage servers + the number of nodes needed by your application. For example, if you want to run 1 data storage server, 1 metadata storage server and your application only needs 1 node, you have to allocate 3 nodes:
```
salloc -N3 --cpus-per-task=16
```
The parameters of this command must be modified according to the Slurm configuration of your enviroment.
Since Hercules is a multi-threading application and the Hercules deployment script launches multiple tasks on the same node, it is sometimes required to set the --cpus-per-task parameter. Otherwise, the Slurm error "More processors requested than permitted" could be produced.

2. Now, you can run the Hercules deployment script:
```
 hercules start -f hercules.conf
```
This will perform the following actions:
* To create a copy of the configuration file by concatenating the Slurm job ID to the name and replacing the paths with the temporary hostfiles that will be created in the [tmp/](tmp/) directory of Hercules.
* To create the metadata and data hostfiles in the [tmp/](tmp/) directory of Hercules. Hercules automatically detects and assigns the allocated nodes to the hostfiles.
* To launch all the back-end services on the nodes specified by the hostfiles.
 <!-- following a Round-Robin distribution, by first deploying the metadata storage servers and then the data storage servers. -->


### 2. Intercepting I/O calls 
Hercules can override I/O calls of your application by using the LD_PRELOAD environment variable. Both data and metadata calls are currently intercepted by the implemented dynamic library. As example:

```
export HERCULES_CONF=<CONF_PATH> 
```
```
LD_PRELOAD=install/lib/libhercules_posix.so ls <hercules_mount_point>
```
```
LD_PRELOAD=install/lib/libhercules_posix.so cp myfile <hercules_mount_point>/copied_file
```
```
LD_PRELOAD=install/lib/libhercules_posix.so ./myapp
```
where "hercules_mount_point" is the MOUNT_POINT defined in the configuration file (CONF_PATH).

**Remember that the paths where "myapp" writes and reads have to point to the "hercules_mount_point".** In other case, they will not be intercepted.


### 3. Stopping Metadata and Data storage servers
To stop a Hercules deployment:
```
hercules stop -f <CONF_PATH>
```

## Without Slurm
To run Hercules in a non-Slurm environment, the script need additional parameters:

```
hercules start -m <meta_server_hostfile> -d <data_server_hostfile> -f <CONF_PATH>

meta_server_hostfile: file containing hostnames of metadata servers
data_server_hostfile: file containing hostnames of data servers
```

#### Hostfile Example

```
node1
node2
node3
```


## Configuration file
<!-- Configuration File (_hercules.conf_) -->
Here we briefly explain each field of the configuration file.

You can overwrite an option at runtime by placing HERCULES_ in front of it. For example, 
```
export HERCULES_BLOCK_SIZE=256
export HERCULES_METADATA_HOSTFILE=/tmp/hercules/my_meta_hostfile
```

A template configuration file can be found in [conf/hercules-template.conf](conf/hercules-template.conf)

<!-- <details><summary>Click to expand</summary> -->
### URI used for internal items definition
> URI = imss://

### Block size (in KBytes)
> BLOCK_SIZE = 512

### Used mount point in the client side
> MOUNT_POINT = /mnt/hercules/

### Path to the Hercules project
> HERCULES_PATH = /home/hercules/

### Port listening in the metadata node service
> METADATA_PORT = 7500

### Port listening in the data node service
> DATA_PORT = 8500

### [>= 1]: Total number of metadata nodes
> NUM_META_SERVERS = 1

### [>= 1]: Total number of data nodes
> NUM_DATA_SERVERS = 1 

### File containing a list of nodes serving as metadata nodes (one per line)
> METADATA_HOSTFILE = /home/hercules/bash/meta_hostfile

### File containing a list of nodes serving as data nodes (one per line)
> DATA_HOSTFILE = /home/hercules/bash/data_hostfile

### [>= 1] Number of threads attending data requests
> THREAD_POOL = 1

### [>= 0] Maximum size in GBytes used by the data nodes
#### **0** = 75% of avaiable memory
> STORAGE_SIZE = 0

### [1, 0]: client process will share resources with data nodes
> ATTACHED = 0

### Data distribution policy (RR, BUCKETS, HASH, CRC16, CRC64, or LOCAL)
> POLICY = RR

## Optional: Malleability parameters
### [1, 0]: to enable malleability functions
> MALLEABILITY = 0      
### [>= 0] Windows size; it defines how many records are used to check the performance status.
> MALLEABILITY_WSIZE = 20
### [>= 0] Performance threshold in MBytes
> MALLEABILITY_THRESHOLD = 2500  
### [>= 0] If the performance threshold is not met, malleability is not triggered until this occurs “MALLEABILITY_TOLERANCE” times
> MALLEABILITY_TOLERANCE = 100


## Optional: Fault tolerance parameters
### [1, 2, 3] Replication factor
> REPL_FACTOR = 1

### Replication type
#### **0**: SYNC (all replicas are written synchronously)
#### **1**: ASYNC (first replica is written synchronously, the rest asynchronously)
> REPL_TYPE = 1

### Checkpoint
### Optional Path where Hercules is going to copy data from memory to disk
#### For example: ./HerculesCheckpoint/
> HERCULES_CHECKPOINT_PATH = # (Leave empty if unused)

### Snapshot
### Path where Hercules is going to do a persistent copy of the files
#### For example: ./HerculesSnapshot/
> HERCULES_SNAPSHOT_PATH = 

### Colon-separated list of directories to be copy from Hercules to disk
#### For example: /mnt/hercules/directory1/:/mnt/hercules/directory2/
#### This value is only used if HERCULES_SNAPSHOT_PATH is defined.
> SNAPSHOT_PATHS_LIST= /mnt/hercules/

### Colon-separated list of directories to be excluded from the HERCULES_SNAPSHOT_PATH.
> IGNORE_PATHS_LIST=

## Optional: Debug parameters
### [none, SLOG_LIVE, SLOG_DEBUG, SLOG_WARN, SLOG_INFO, SLOG_ERROR, SLOG_FATAL, SLOG_PANIC, SLOG_TIME, SLOG_FULL, SLOG_READ, all] Debug mode level
#### **none**: it does not make log files (better performance, but not useful for debugging)
#### **all**: each service makes log files containing all debugging information 
#### **other valid value**: each service makes a log file containing the debugging information according to the logging level selected.
> DEBUG_LEVEL = none

</details>