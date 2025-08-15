The Mimir Block Builder.

The Block Builder is a component of Mimir's Ingest Storage architecture. In Ingest Storage, the ingester consumes from the ingest
Kafka (or Kafka-like) in a timely fashion, and serves as a read cache for queries. The block builder is ingester's sibling, responsible for durably consuming ingestion data and uploading it to object storage for long-term querying.

## Architecture Overview

```
         ┌─────────────┐                   
         │  Ingestion  │                   
         │             │                   
         └──────┬──────┘                   
                │                          
          ┌─────▼─────┐       ┌───────────┐
          │   Kafka   ┼───────┼ Block     │
          │           │       │ builder   │
          └┬─────────┬┘       │ scheduler │
           │         │        └─┬─────────┘
           │         │          │          
┌──────────▼──┐   ┌──▼──────────┴┐         
│             │   │              │         
│  Ingester   │   │  Block       │         
│             │   │  builder     │         
│             │   │              │         
└─────┬───────┘   └──────┬───────┘         
      │                  │                 
      │           ┌──────▼───────┐         
┌─────▼───────┐   │              │         
│             │   │  Object      │         
│   Queries   │◄──┤  storage     │         
│             │   │              │         
└─────────────┘   └──────────────┘         
```

The block builder is composed of two services:
1. block-builder-scheduler. The block-builder-scheduler is a singleton
    service that monitors Kafka and periodically creates consumption jobs
    to be carried out by the block-builder workers. It provides an RPC interface that
    the block-builder workers use to obtain and update jobs.
1. block-builder. The block-builder is a pool of stateless workers who receive jobs from the scheduler,
    perform consumption from Kafka and upload the result to object storage.


## Block-builder-scheduler 
