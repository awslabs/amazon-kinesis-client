## Introduction
This document explains the architectural changes in KCL 3.x regarding leader election and workload distribution, highlighting how KCL 3.x efficiently rebalances leases across workers using throughput metrics and CPU utilization.

- [Overview: Lease reassignments in KCL 3.x](#overview-lease-reassignments-in-kcl-3x)
- [Motivation for moving from distributed to leader-based protocol](#motivation-for-moving-from-distributed-to-leader-based-protocol)
- [How KCL 3.x manages leader election](#how-kcl-3x-manages-leader-election)
- [How KCL 3.x captures throughput per shard](#how-kcl-3x-captures-throughput-per-shard)
- [How KCL 3.x captures CPU utilization of worker hosts](#how-kcl-3x-captures-cpu-utilization-of-worker-hosts)
- [How KCL 3.x performs lease assignments](#how-kcl-3x-performs-lease-assignments)

## Overview: Lease reassignments in KCL 3.x

KCL 3.x rebalances leases based on the load (CPU utilization) on each worker with a goal of even distribution of load across workers. Unlike KCL 2.x where all workers scan the entire lease table to identify the lease assignment status and pick up unassigned leases or steal leases from existing workers, KCL 3.x achieves it by electing a leader worker scanning the entire lease table and do the lease assignment for the entire KCL workers. The following is the key differences in KCL 3.x.  

- **Centralized (leader-based) lease assignment**  
    KCL 3.x introduces a leader-based approach for assigning and rebalancing leases. Instead of each worker scanning the lease table and autonomously deciding how to pick up or steal leases (KCL 2.x approach), a single worker is elected as **leader**. This leader scans the DynamoDB tables to gather global state, decides on an optimal assignment, and updates the leases accordingly.
- **Throughput-aware rebalancing**  
    KCL 3.x tracks the actual throughput (bytes processed) of each shard. During lease assignment, the leader considers shard-level throughput, not just the shard count. This ensures to distribute “hot shards” more evenly and minimize the risk of overloading a single worker.
- **CPU utilization metrics (worker metrics)**  
    KCL 3.x introduces the concept of **worker metrics**, primarily CPU utilization at the launch of KCL 3.x, to guide the rebalancing decisions. KCL 3.x tries to keep each worker’s CPU utilization within a specified threshold range by redistributing leases away from overloaded workers. This ensures even CPU utilization across workers.
- **Reduced DynamoDB usage**  
    By centralizing the lease assignment logic in a single leader, KCL 3.x significantly reduces the number of scan operations on the DynamoDB lease table. Only the leader does a full table scan at a set frequency rather than all workers doing so simultaneously. Once the leader worker updates the lease assignments result in the lease table, other workers are reading the global secondary index on the lease table for an efficient lease discovery. Global secondary index mirrors the `leaseKey` attribute from the base lease table with the partition key of `leaseOwner`.
- **Graceful Lease Handover**  
    KCL 3.x introduces a more graceful approach to handing off leases during rebalancing. When a shard must be reassigned, KCL 3.x can minimize duplicated processing and reduce the need to reprocess records unnecessarily by checkpointing the last processed record in the worker that relinquishes the lease and resuming the processing in the new worker picking up the lease.

## Motivation for moving from distributed to leader-based protocol

There were several challenges in the distributed protocol in KCL 2.x:  

1. **Suboptimal decisions and duplicate work:** In KCL 2.x, each worker does the duplicate work of listing all leases, computing target and expired leases and the performs assignment looking at only its current state without knowledge of what other works are doing. This leads to high convergence time (in terms of getting to same no. of leases per worker) and also leads to wasted/unoptimized usage of compute on all workers where each worker spends CPU cycle going through same code path)
2. **Lease churn and high MillisBehindLatest:** When stream scales (new leases added or leases are removed) or workers dies (due to failures or deployment) there is chaos/lease churn in the application due to lease stealing, where workers are free to steal any no. of leases (not just the leases that lost its owner). e.g., W0 worker has lease1-10 assigned to it and goes down, W1 picks all 10 leases 1-10 left by W0 and W2 then steals assigned leases from W1 randomly and the once which W1 already have with it, leading to chaos and increase in reading delays.
3. **Lack of load based assignment and global optimal assignment:** Lease count based assignment works good for workload with equal throughput on all leases and predictable/smooth traffic, however for workloads with different throughput on different shards the mere lease count based assignment causes difference in the resource consumption on different workers and thus requiring application to scale for worst case assignment and over-provision compute.
4. **High DynamoDB RCU usage:** As KCL 2.x lease assignment require each worker to list leases periodically to identify new leases and workers, workers need to make scan of whole lease table which adds to the RCU consumption.
5. **High races assignment of new leases (leading to wasted DynamoDB WCU):** KCL 2.x lease assignment works on work stealing principal, several workers race for assigning same lease to itself, this creates the race in assignment where only single worker wins the lease while others fail and waste RCU, CPU cycle, and time.

KCL 3.x solves challenges above by having a leader worker performing the assignment by looking into the global state of application and performs the assignment and load balancing of leases based on the utilization of each worker.  

## How KCL 3.x manages leader election

### **Leader decider**  
KCL has a specific worker that is chosen as a leader and does following tasks.  

1. Periodically ensures the lease table matches the active shards in the stream (`PeriodicShardSync`)
2. Discover new streams that needs processing/consumption and creates the shards for them (in [multi-stream processing](https://docs.aws.amazon.com/streams/latest/dev/kcl-multi-stream.html) mode)
3. Performs lease assignment (in KCL 3.x)

KCL 2.x used a leader decider class called `DeterministicShuffleShardSyncLeaderDecider` ([GitHub link](https://github.com/awslabs/amazon-kinesis-client/blob/master/amazon-kinesis-client/src/main/java/software/amazon/kinesis/coordinator/DeterministicShuffleShardSyncLeaderDecider.java)). In KCL 3.x, the leader decider has been updated to `DynamoDBLockBasedLeaderDecider` ([GitHub link](https://github.com/awslabs/amazon-kinesis-client/blob/master/amazon-kinesis-client/src/main/java/software/amazon/kinesis/leader/DynamoDBLockBasedLeaderDecider.java)).  
  
### **Why KCL 3.x stepped away from DeterministicShuffleShardSyncLeaderDecider?**  
`DeterministicShuffleShardSyncLeaderDecider` is a component within KCL 2.x that handles leader election among workers. The component operates by scanning all `leaseOwners` (workers) within the KCL application, sorting them by worker ID, shuffling them using a constant seed, and designating the top worker as leader. This process informs each worker whether it should perform leader duties or operate as a standard worker.  
  
The implementation relies on DynamoDB table scans to gather a complete list of `leaseOwners`. Every five minutes, each worker initiates a `listLeases` call to DynamoDB, scanning the entire lease table to compile a list of unique lease owners. While this approach provides a straightforward solution to leader election, it introduces following challenges:  

- **1) High DynamoDB RCU Usage**: Full table scans performed every five minutes lead to high RCU consumption in the DynamoDB lease table. This consumption scales proportionally with the application size – more workers and leases result in higher RCU usage, creating potential performance bottlenecks and increased operational costs.
- **2) High Convergence Time:** During worker failures or deployments, the five-minute interval forces the cluster to wait at least five minutes before recognizing and establishing a new leader. This delay significantly impacts system availability during critical operational periods.
- **3) Slow Failure Handling:** Newly elected leader may remain unaware of their leadership status for up to five minutes after selection. This delay creates a gap in leadership coverage and could impact the application's coordination capabilities during transition periods.
- **4) Low Consistency:** The five-minute heartbeat interval in leadership detection can create race conditions where multiple workers claim leadership simultaneously or no worker assumes leadership. This leads to redundant operations and wasted resources when multiple leaders exist, or delay in critical tasks like shard synchronization and stream discovery when leadership gaps exist.

  
### **Leader election in KCL 3.x**  
KCL 3.x implements leader election using `DynamoDBLockClient` library ([GitHub link](https://github.com/awslabs/amazon-dynamodb-lock-client/)). Instead of performing full table scans of lease entries and finding the first worker in the sorted list of leases, this implementation uses a lock-based mechanism stored in the DynamoDB coordinator state table. Each worker in KCL 3.x follows this process:  

- 1) Reads a single lock entry from the DynamoDB lock table
- 2) Validates the active status of the current lock owner by checking if the lock item gets constant heartbeat
- 3) Claims leadership if the previous lock owner's heartbeat has expired, following a first-worker-wins model

### **Leader failure handling in KCL 3.x**  
The `DynamoDBLockClient` library provides the core failure handling capabilities. It monitors and responds to various failure scenarios including network partitions, worker shutdowns, and overloaded workers. When a worker fails to maintain its heartbeat on the lock item, KCL 3.x automatically enables another worker to claim leadership.  
  
KCL 3.x extends the base failure handling with additional safeguards for critical operations such as lease assignments. If there are three consecutive failures in lease assignment by a leader ([GitHub code ref](https://github.com/awslabs/amazon-kinesis-client/blob/68a7a9bf53e03bd9177bbf2fd234aca103d7f5dc/amazon-kinesis-client/src/main/java/software/amazon/kinesis/coordinator/assignment/LeaseAssignmentManager.java#L81)), KCL 3.x detects and releases the leadership to enable other workers to take the leadership and perform lease assignments. This dual-layer failure handling mechanism ensures both infrastructure-level and application-level failures are handled effectively.  

## How KCL 3.x captures throughput per shard

### **Shard throughput calculation**  
Every time KCL 3.x retrieves a batch of records from a Kinesis data stream and deliver to the record processor, it computes the total bytes that was fetched from a specific shard and records it in the `LeaseStatsRecorder` (GitHub – [class](https://github.com/awslabs/amazon-kinesis-client/blob/ae9a433ebd1b0bef74e643b205f8d4759a126e65/amazon-kinesis-client/src/main/java/software/amazon/kinesis/leases/LeaseStatsRecorder.java#L45), [code ref](https://github.com/awslabs/amazon-kinesis-client/blob/ae9a433ebd1b0bef74e643b205f8d4759a126e65/amazon-kinesis-client/src/main/java/software/amazon/kinesis/lifecycle/ProcessTask.java#L180)). These stats are accumulated for the `leaseRenewerFrequency` (`failoverTimeMillis/3`). During `leaseRenewal` (which runs at `leaseRenewerFrequency`), the shard throughput is computed and updated in the KCL lease’s throughput attribute. The final shard throughput is calculated as following:  

- Calculate the shard throughput in the last lease renewal interval (bytes delivered to record processor in the last interval / `leaseRenewerFrequency`)
- To avoid the swing from the short-term traffic surges, take an exponential moving average (EMA) with an alpha of 0.5 on the previous and current value ([GitHub code ref](https://github.com/awslabs/amazon-kinesis-client/blob/master/amazon-kinesis-client/src/main/java/software/amazon/kinesis/utils/ExponentialMovingAverage.java#L45)). This smoothed stat is used for the lease assignment in KCL 3.x.

## How KCL 3.x captures CPU utilization of worker hosts

### **Amazon Elastic Compute Cloud (EC2)**

KCL collects CPU utilization on Amazon EC2 instances by reading system statistics from the `/proc/stat` file. This file provides kernel/system statistics, from which KCL calculates the CPU utilization in percentage ([GitHub code ref](https://github.com/awslabs/amazon-kinesis-client/blob/master/amazon-kinesis-client/src/main/java/software/amazon/kinesis/worker/metric/impl/linux/LinuxCpuWorkerMetric.java)) for the worker host.  

### Amazon Elastic Container Service (ECS)

KCL 3.x retrieves container-level metrics through the ECS task metadata endpoint, accessible within the container via the `${ECS_CONTAINER_METADATA_URI_V4}` environment variable. The ECS container agent provides a local endpoint reachable in the container by the task to retrieve task metadata and Docker stats, known as the task metadata endpoint ([ref](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-metadata-endpoint.html)). The URI is injected into the container with the system variable `${ECS_CONTAINER_METADATA_URI_V4}`.  
  
This endpoint provides CPU, memory, and network statistics on a per task granularity. Even when multiple KCL applications run in separate containers on the same EC2 host, these statistics would not aggregate for the whole host ([GitHub code ref](https://github.com/awslabs/amazon-kinesis-client/blob/master/amazon-kinesis-client/src/main/java/software/amazon/kinesis/worker/metric/impl/container/EcsCpuWorkerMetric.java)).  
  
The CPU utilization percentage is calculated with the following formula ([ref](https://docs.docker.com/engine/api/v1.45/#tag/Container/operation/ContainerStats)):  

```
cpuUtilization = (cpuDelta / SystemDelta) * online_cpus * 100.0
```
  
where:  
```
cpuDelta = total_usage - prev_total_usage  
systemDelta = system_cpu_usage - prev_system_cpu_usage
```

This gets the value in CPU cores. For example, if there is a limit of 3 CPUs given to a task and the KCL is the only container, this value can be up to 3. So, to get percent utilization, we must determine the maximum number of CPUs the KCL container can use.  
  
Getting that value requires a couple of steps. The hard limit on a task is the [CPU task size](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task_definition_parameters.html#task_size). This is the maximum CPU time that all containers in the task can use. The second limit is the [container CPU size](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task_definition_parameters.html#container_definitions). This is a relative amount to other containers in the task. For example, if a task CPU size is 3 CPUs, and there are two containers with 256 and 512 CPU size, then the first container would guarantee 1 CPU and the second container is guaranteed 2 CPUs. However, each container can use more than the minimum up to the CPU task size if the other container is not using all of its CPU. Though if both containers use 100% of their CPU all the time, only the guaranteed CPUs would be used.  
  
So, to get the CPUs guaranteed to the container, we first get the value of the CPU task size. Then, we get the CPU shares of the current container. Divide those shares by the total number of shares from all containers and multiply by the CPU task size to get the max amount of CPUs available to the container.  

### Elastic Kubernetes Service (EKS) containers running on Linux instances with cgroupv1 or cgroupv2

KCL 3.x utilizes Linux Control Groups to extract CPU information about the container. KCL 3.x reads CPU time and available CPU from cgroup directory. Amazon Linux 2 uses cgroupv1 and Amazon Linux 2023 uses cgroupv2. CPU time is measured in CPU cores time. A container is limited by amount of CPU core time it is allocated. So, if over a second the container uses 0.5 CPU core time and is allocated 2 CPU cores, the CPU utilization would be 25%. When this is invoked for the first time, the value returned is always 0 as the previous values are not available to calculate the diff ([GitHub code ref 1](https://github.com/awslabs/amazon-kinesis-client/blob/master/amazon-kinesis-client/src/main/java/software/amazon/kinesis/worker/metric/impl/container/Cgroupv1CpuWorkerMetric.java), [GitHub code ref 2](https://github.com/awslabs/amazon-kinesis-client/blob/master/amazon-kinesis-client/src/main/java/software/amazon/kinesis/worker/metric/impl/container/Cgroupv2CpuWorkerMetric.java)).  
  
The file `/sys/fs/cgroup/cpu/cpuacct.usage` contains a value of the CPU time used by the container in nanoseconds. To calculate the CPU utilization percent, we can get the maximum amount of CPU cores allocated to the container by reading the files `/sys/fs/cgroup/cpu/cpu.cfs_quota_u`s and `/sys/fs/cgroup/cpu/cpu.cfs_period_us`. the quota divided by the period will give the max amount of CPU core time given to the container. If the value of the quota is -1, this means that the container is not limited in CPU time by the cgroup and can utilize all the cores on the node. We can get this value in java by calling `Runtime._getRuntime_().availableProcessors()`. The file paths and format of some data are the difference between cgroupv1 and cgroupv2, and the process describes is the same besides that.  

## How KCL 3.x performs lease assignments

Like mentioned in the previous sections, only the leader worker regularly scans the entire lease table and assign leases in KCL 3.x. `LeaseAssignmentManager` class ([GitHub link](https://github.com/awslabs/amazon-kinesis-client/blob/68a7a9bf53e03bd9177bbf2fd234aca103d7f5dc/amazon-kinesis-client/src/main/java/software/amazon/kinesis/coordinator/assignment/LeaseAssignmentManager.java#L81)) performs two operations related to the lease assignment:  

- **1) Priority lease assignments:** assigning unassigned leases (lease without owners) and expired leases (leases where the lease duration has elapsed).
- **2) Lease rebalancing:** shuffling leases to equalize worker’s utilization to achieve even distribution of load.

For non-leader workers, `LeaseAssignmentManager` will do nothing as the assignment will be done on the leader ([GitHub code ref](https://github.com/awslabs/amazon-kinesis-client/blob/68a7a9bf53e03bd9177bbf2fd234aca103d7f5dc/amazon-kinesis-client/src/main/java/software/amazon/kinesis/coordinator/assignment/LeaseAssignmentManager.java#L181)). Lease assignment follows a greedy algorithm, prioritizing workers with the lowest utilization. To prevent over-allocation, the assignment process considers the following:  

- Worker's current utilization
- Existing lease throughput
- Projected load from potential new leases
- Number of leases held by worker

`LeaseAssignmentManager` iteratively assigns multiple leases per worker while maintaining balanced resource distribution based on these metrics. Throughput serves as a proxy metric to estimate the impact of lease changes on worker’s load (i.e., workers’ CPU utilization). When assigning or removing leases, KCL 3.x uses throughput data to predict how these operations will affect the overall worker utilization and use that to influence the assignment decision. The following is the detailed rebalancing process ([GitHub code ref](https://github.com/awslabs/amazon-kinesis-client/blob/68a7a9bf53e03bd9177bbf2fd234aca103d7f5dc/amazon-kinesis-client/src/main/java/software/amazon/kinesis/coordinator/assignment/VarianceBasedLeaseAssignmentDecider.java)).  

1. Calculate the average worker metrics value (CPU utilization) across all workers.
2. Compute the upper and lower limit thresholds for triggering the rebalancing. The upper and lower limits are calculated based on the average worker metrics value and `reBalanceThresholdPercentage` (10 by default; [GitHub code ref](https://github.com/awslabs/amazon-kinesis-client/blob/68a7a9bf53e03bd9177bbf2fd234aca103d7f5dc/amazon-kinesis-client/src/main/java/software/amazon/kinesis/leases/LeaseManagementConfig.java#L541)) configuration parameter.

  - Upper limit: average worker metric * (1 + `reBalanceThresholdPercentage`/100).
  - Lower limit: average worker metric * (1 + `reBalanceThresholdPercentage`/100).

3. Trigger a rebalancing if any worker’s worker metric value falls outside these limits.

  - For example, let’s assume that worker A has 70% CPU utilization and worker B has 40% CPU utilization. The average worker metric is (70+40)/2 = 55%. Then, the upper limit is 55% * (1+10/100) = 60.5%, and the lower limit is 55% (1-10/100) = 49.5%. Both worker A and B are outside the upper and lower limit, hence the rebalancing will be triggered.

4. Calculate how much load (CPU utilization) to take from the worker in out-of-range to make them back to the average CPU utilization. The load to take is calculated by subtracting the average load from the current load.
5. Apply `dampeningPercentageValue` (80 by default) to the calculated load to take from the worker in out-of-range. Dampening will prevent the excessive rebalancing that would cause oscillation and help to achieve critical damping.
6. Calculate the throughput to take from the worker with based on the dampened amount of load to take.
7. Select and reassign specific leases matching the dampened throughput target from over-utilized worker to under-utilized workers.

When KCL applications run on platforms that don't support CPU utilization metrics such as Windows, the system automatically falls back to a throughput-based balancing mechanism. In this fallback mode, KCL 3.x distributes leases based on shard throughput, aiming for all workers processing equal throughput of shards.  
  
### Rebalancing related configuration parameters

- `reBalanceThresholdPercentage:` This parameter is a percentage value that determines when the load balancing algorithm should consider reassigning shards among workers. The default value is set to 10. You can set a lower value to make your target resource utilization converge more tightly for more frequent rebalancing. If you want to avoid frequent rebalancing, you can set a higher value to loosen the rebalancing sensitivity.
- `dampeningPercentageValue`: This parameter is used to achieve critical damping during reassignments. It helps prevent overly aggressive rebalancing by limiting the amount of load that can be moved in a single rebalance operation. The default value is set to 80. You can set a lower value for more gradual load balancing that minimize the oscillation. If you want to more aggressive rebalancing, you can set a higher value for quick adjustment of the load.
