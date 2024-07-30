# Skew handling in distributed joins: Implementation of a Flow-Join

This project implements a flow join algorithm, following the paper of Roediger W. [1].
It includes scripts and C++ code for generating and processing data, partitioning, and performing the join. Here we implemented the flow join as well as a hash join to compare the performance.
The project also evaluates the efficiency of different data structures for tracking frequent elements using the Space-Saving algorithm.

Flow-Join includes several key components:

- **Heavy Hitter Detection**: Uses small, approximate histograms to detect skewed data during the initial phases of data partitioning.
- **Adaptive Redistribution**: Adjusts the data redistribution strategy in real-time to balance the load across servers, minimizing the impact of skew on join performance.

## Table of Contents
1. Overview
2. Prerequisites
3. Run
4. Scripts and Files
5. Contributors
6. Literature

## Overview

This project focuses on:

- Generating Zipf-distributed data.
- Partitioning data into files for distributed processing.
- Implementing the flow join algorithm.
- Implementing the hash join algorithm to compare.
- Evaluating data structures for tracking heavy hitters using the Space-Saving algorithm.

## Prerequisites
- C++ compiler with C++20 support (Tested with g++)
- ZeroMQ networking library. On Linux, use: sudo apt install libzmq3-dev

## Run
### Generate and partition data
To generate and partition data, use the ``create_R_S.sh`` script. Run the following argument:

```
./create_R_S.sh <seed> <alpha> <n_unique> <n_rows> <n_servers>
```

- ``<seed>``: Random seed for data generation.
- ``<alpha>``: Zipf distribution parameter.
- ``<n_unique>``: Number of unique values (Number of tuples in R table)
- ``<n_rows>``: Number of rows to generate (Number of tuples in S table)
- ``<n_servers>``: Number of servers for data partitioning.

Note: Remember to compile the given scripts in ``create_R_S.sh`` (in total 4) beforehand (See Scripts and Files).

### Run joins
After generating and partitioning data, you can run the join algorithms. The provided executables for ``flow_join_local`` and ``hash_join_local`` can be used as follows:

```
./flow_join_local <n_servers> <num_r_tuples> <num_s_tuples> <R_folder> <S_folder>
```


```
./hash_join_local <n_servers> <num_r_tuples> <num_s_tuples> <R_folder> <S_folder>
```

- ``<n_servers>``: Number of servers.
- ``<num_r_tuples>``: Number of tuples in R data.
- ``<num_s_tuples>``: Number of tuples in S data.
- ``<R_folder>``: Folder containing R data files.
- ``<S_folder>``: Folder containing S data files.

## Scripts and Files
- ``create_R_S.sh``: Script to generate and partition Zipf-distributed data.
- ``helper_functions.cpp``: C++ helper functions for file operations and joins.
- ``helper_functions.h``: Header file for helper functions.
- ``SpaceSaving.h``: Header file for the Space-Saving algorithm.
- ``SpacesSaving_update_rates.cpp``: C++ code to evaluate update rates of different data structures.
```
g++ -std=c++20 SpaceSaving_update_rates.cpp helper_functions.cpp -o SpaceSaving_update_rates
```
- ``flow_join_local.cpp``: C++ code for distributed flow join implementation. Compile with
```
g++ -std=c++20 flow_join_local.cpp utils/helper_functions.cpp -o flow_join_local
```
- ``hash_join_local.cpp``: C++ code for distributed hash join implementation.
```
g++ -std=c++20 hash_join_local.cpp utils/helper_functions.cpp -o hash_join_local
```
- ``hash_join_distributed.cpp``: C++ code for distributed hash join implementation.
```
g++ -std=c++20 hash_join_distributed.cpp utils/helper_functions.cpp -o hash_join_distributed -lzmq
```
- Helper files for ``create_R_S.sh``: ``split_file.cpp``, ``add_row_numbers.cpp``, ``gen_zipf.cpp``, ``gen_R.cpp``
```
g++ file.cpp -o file
```

## Contributors
Collaborators: Irene Santana Martin, Luca Heller and Timothy

UTN - CloudDB Project SS24

## Literature
[1] Roediger W. et al. Flow-Join: Adaptive Skew Handling for Distributed Joins over High-Speed Networks. 2016. https://chatgpt.com/c/512a1c73-b87a-49cf-8b67-4926d3653485.
