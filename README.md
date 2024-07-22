# Skew handling in distributed joins: Implementation of a Flow-Join
UTN - CloudDB Project SS24

Collaborators: Irene Santana Martin, Luca Heller and Timothy

## Description
...

### Flow-Join Algorithm
Flow-Join includes several key components:

- **Heavy Hitter Detection**: Uses small, approximate histograms to detect skewed data during the initial phases of data partitioning.
- **Adaptive Redistribution**: Adjusts the data redistribution strategy in real-time to balance the load across servers, minimizing the impact of skew on join performance.
- **Integration with High-Speed Networks**: Utilizes RDMA for efficient data transfer, achieving near-theoretical network throughput with minimal CPU overhead

...

## Literature
[1] Roediger W. et al. Flow-Join: Adaptive Skew Handling for Distributed Joins over High-Speed Networks. 2016. https://chatgpt.com/c/512a1c73-b87a-49cf-8b67-4926d3653485.
