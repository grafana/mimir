# Block Builder Architecture

The Block Builder is a distributed system designed to consume Kafka messages and convert them into TSDB blocks for efficient storage and querying. It consists of two main components that work together: the **Block Builder** (worker) and the **Block Builder Scheduler**.

## Architecture Overview

```
┌─────────────────┐     ┌──────────────────────┐     ┌────────────────┐
│     Kafka       │     │   Block Builder      │     │   Object       │
│   (Messages)    │────▶│   Scheduler          │     │   Storage      │
│                 │     │                      │     │   (Blocks)     │
└─────────────────┘     └──────────────────────┘     └────────────────┘
                                    │                          ▲
                                    │ Jobs via gRPC            │
                                    ▼                          │
                        ┌──────────────────────┐               │
                        │   Block Builder      │               │
                        │   Workers            │───────────────┘
                        │                      │
                        └──────────────────────┘
```

## Components

### Block Builder Scheduler

The scheduler is responsible for:
- **Job Discovery**: Monitors Kafka partition offsets and creates consumption jobs
- **Job Assignment**: Distributes jobs to available block builder workers
- **Offset Management**: Tracks committed offsets and ensures data consistency
- **Job Lifecycle**: Manages job states, retries, and cleanup

### Block Builder Workers

The workers are responsible for:
- **Job Execution**: Consumes Kafka messages for assigned offset ranges
- **Block Creation**: Processes messages and creates TSDB blocks
- **Block Upload**: Uploads completed blocks to object storage
- **Status Reporting**: Communicates job progress back to the scheduler

## Communication Protocol

The Block Builder and Scheduler communicate via a gRPC interface defined in `schedulerpb/scheduler.proto`:

### Job Assignment Flow

1. **Job Request**: Workers call `AssignJob()` with their worker ID
2. **Job Response**: Scheduler returns a `JobKey` (unique ID + epoch) and `JobSpec` (Kafka partition + offset range)
3. **Job Processing**: Worker consumes messages from Kafka for the specified offset range
4. **Status Updates**: Worker periodically calls `UpdateJob()` to renew job lease
5. **Job Completion**: Worker calls `UpdateJob()` with `complete=true` when finished

### Key Data Structures

```protobuf
message JobKey {
    string id = 1;      // Unique job identifier
    int64 epoch = 2;    // Version number for job updates
}

message JobSpec {
    string topic = 1;       // Kafka topic
    int32 partition = 2;    // Kafka partition
    int64 start_offset = 3; // Start offset (inclusive)
    int64 end_offset = 4;   // End offset (exclusive)
}
```

## Scheduler Operation Modes

### 1. Observation Mode (Startup)
- Duration: Configurable via `startup_observe_time` (default: 1 minute)
- Purpose: Discovers existing jobs from workers to avoid duplicates
- Behavior: Collects job status from all workers before scheduling new jobs

### 2. Normal Operation Mode
- **Job Creation**: Creates jobs based on time buckets (configurable `job_size`, default: 1 hour)
- **Scheduling**: Assigns jobs to workers via `SchedulingInterval` (default: 1 minute)
- **Offset Tracking**: Maintains committed offsets for each partition
- **Cleanup**: Manages job leases and handles worker failures

## Job Lifecycle

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   Created   │───▶│  Assigned   │───▶│ In Progress │───▶│  Completed  │
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
                          │                   │
                          │                   │
                          ▼                   ▼
                   ┌─────────────┐    ┌─────────────┐
                   │   Expired   │    │   Failed    │
                   └─────────────┘    └─────────────┘
```

### Job States

- **Created**: Job exists in scheduler's pending queue
- **Assigned**: Job assigned to a worker with active lease
- **In Progress**: Worker is actively processing the job
- **Completed**: Job finished successfully, offset committed
- **Expired**: Job lease expired, will be reassigned
- **Failed**: Job failed too many times (configurable via `job_failures_allowed`)

## Configuration

### Scheduler Configuration

```yaml
# How frequently to recompute the schedule
scheduling_interval: 1m

# How long jobs (and therefore blocks) should be
job_size: 1h

# How long to observe worker state before scheduling jobs
startup_observe_time: 1m

# How long a job lease will live before expiring
job_lease_expiry: 2m

# Maximum number of jobs per partition (currently must be 1)
max_jobs_per_partition: 1

# How frequently to enqueue pending jobs
enqueue_interval: 2s
```

### Worker Configuration

```yaml
# Scheduler address for gRPC communication
scheduler_config:
  address: "scheduler:9095"
  
  # Interval between scheduler updates
  update_interval: 20s
  
  # Maximum age of jobs to continue sending to scheduler
  max_update_age: 30m
```

## Error Handling and Resilience

### Worker Failures
- **Lease Expiry**: Jobs automatically reassigned if worker stops updating
- **Graceful Shutdown**: Workers complete in-flight jobs before stopping
- **Restart Recovery**: Scheduler observes existing jobs during startup

### Scheduler Failures
- **Offset Persistence**: Committed offsets stored in Kafka consumer groups
- **State Recovery**: Scheduler rebuilds state from Kafka metadata and worker reports
- **Job Continuity**: Prevents data gaps through careful offset management

### Network Failures
- **Backoff Retry**: Exponential backoff for transient failures
- **Timeout Handling**: Configurable timeouts for gRPC calls
- **Connection Pooling**: Efficient connection management

## Monitoring and Observability

### Key Metrics

**Scheduler Metrics:**
- `mimir_blockbuilder_scheduler_outstanding_jobs`: Number of jobs waiting for assignment
- `mimir_blockbuilder_scheduler_assigned_jobs`: Number of jobs currently assigned
- `mimir_blockbuilder_scheduler_pending_jobs`: Number of jobs waiting to be enqueued
- `mimir_blockbuilder_scheduler_partition_end_offset`: Latest offset for each partition

**Worker Metrics:**
- `mimir_blockbuilder_consume_job_duration`: Time spent processing jobs
- `mimir_blockbuilder_process_partition_duration`: Time spent processing partitions
- `mimir_blockbuilder_fetch_errors`: Number of Kafka fetch errors

### Logging
- Structured logging with job IDs, worker IDs, and partition information
- Debug logs for offset management and job transitions
- Error logs for failures with context

## Performance Considerations

### Throughput
- **Parallel Processing**: Multiple workers can process different partitions
- **Batch Processing**: Jobs process ranges of offsets rather than individual messages
- **Efficient Uploads**: Blocks uploaded directly to object storage

### Scalability
- **Horizontal Scaling**: Add more workers to increase throughput
- **Partition-Based**: Natural partitioning via Kafka partitions
- **Stateless Workers**: Workers can be added/removed dynamically

### Resource Management
- **Memory Usage**: Configurable limits via `apply_max_global_series_per_user_below`
- **Disk Usage**: Temporary storage cleaned up between jobs
- **Network Usage**: Efficient gRPC communication with compression

## Development and Testing

### Local Development
- Configure scheduler address to point to local scheduler instance
- Use file-based object storage for testing
- Adjust timeouts for faster iteration

### Integration Testing
- Test job assignment and completion flows
- Verify offset management and continuity
- Test failure scenarios and recovery

### Production Deployment
- Monitor job completion rates and latency
- Set appropriate timeouts and retry policies
- Configure resource limits based on expected load 