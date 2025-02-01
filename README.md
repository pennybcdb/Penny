# Penny: A Cost-Efficient Hybrid Storage Solution for Blockchain

**Penny** is a hybrid storage management system designed to optimize SSD usage while maintaining high performance in blockchain systems. It efficiently classifies and migrates key-value data between SSD and HDD based on predefined heuristics and access patterns.

## Architecture Overview

Penny consists of three main components:

- **KV Thermometer**:  
  This component classifies key-value requests into four categories: **hot, cold, deterministic-gray, and nondeterministic-gray**, based on the characteristics of each key-value type, as detailed in Table~\ref{table_types}.  
  The classifications by KV Thermometer serve as indicators for fine-grained data placement policies.  
  To reduce SSD storage usage, **nondeterministic-gray data** is treated as cold data and stored accordingly.

- **Hytorage**:  
  Penny features a **hybrid storage architecture** that combines **SSD for hot data** and **HDD for cold data**, leveraging the strengths of both storage types to balance performance and cost efficiency.  
  Unlike conventional blockchain storage approaches, the cold store is specifically optimized for HDD performance.  
  It incorporates a **write-optimized key-value store with HDD-friendly configuration** and a **read-optimized file-based indexed store** to ensure efficient data storage and retrieval.

- **KV Migrator**:  
  This component manages the **migration of cold and gray data to the cold store in the background** while ensuring minimal impact on system performance.  
  It selectively migrates **deterministic-gray data** and determines whether it should be placed on HDD or SSD based on blockchain-aware policies.  
  Additionally, it **buffers and smooths write operations** to prevent burst write requests from overwhelming the HDD, thereby avoiding performance bottlenecks.

Penny is designed to enhance storage efficiency in blockchain systems by intelligently managing data placement while maintaining high performance.

For a detailed explanation, refer to our paper

## Installation

### Prerequisites
For prerequisites and detailed build instructions please read the [Installation Instructions](https://geth.ethereum.org/docs/getting-started/installing-geth).

- Linux-based OS (Ubuntu 22.04+ recommended)
- Go 1.21+  


### Build and Run

```bash 
# Clone the repository
git clone https://github.com/vldbpenny/Penny.git
cd Penny
```

```bash 
# Build the project
make geth
```

```bash 
# Run archive sync with Penny 
./geth --mainnet --http --http.api eth,net,engine,admin --authrpc.jwtsecret=$JWT_PATH --syncmode full --datadir=$DATA_PATH --gcmode=archive 
```



## License

The go-ethereum library (i.e. all code outside of the `cmd` directory) is licensed under the
[GNU Lesser General Public License v3.0](https://www.gnu.org/licenses/lgpl-3.0.en.html),
also included in our repository in the `COPYING.LESSER` file.

The go-ethereum binaries (i.e. all code inside of the `cmd` directory) are licensed under the
[GNU General Public License v3.0](https://www.gnu.org/licenses/gpl-3.0.en.html), also
included in our repository in the `COPYING` file.
