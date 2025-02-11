# Data Engineering Principles
The 3rd project in my exploration of data engineering, it hopes to deepen my data engineering fundementals, expand my toolkit and revise my existing data engineering skillset.

* **5 stages of pipelining**: As it turns out, the pipeline has at most 5 stages. `Collection` -> `Ingestion` -> `Storage` -> `Computation` -> `Consumption`.
    * This project will focus on **Ingestion** (Kafka) and the **storage** (CassandraDB) phase.
    * As an ELT/ETL orchestrator (AWS glue), airflow assists with >1 if not all of these stages.
    * The 5 stages are interchangeable and some of them may be removed.

* ELT vs ETL Pipelines
    * **ETL**: Best used for structured data, slower if dataset is large, best used for structured environment with heavy governance.
    * **ELT**: Best used for large datasets with large volumes, transformation is done within the warehouse itself.

* **[Database vs Data Warehouse vs Data Lake](https://www.youtube.com/watch?v=-bSkREem8dM&t=200s)**
    * **Database**: Closely aligned with OLTP, it houses all incoming, highly detailed data. Likely relational DB
    * **Data Warehouse**: Closely aligned wth OLAP, houses summarized data coming from ETL pipelines. Always has historical data but not current due to dependency on ETL (can be managed by orchestration). Queries are generally faster than databases but schema is more rigid (thus planning is needed when using a warehouse).
    * **Data Lake**: A database designated to house any kind of data into it (Summarized or raw). Generally used for ML stuff.

* **[Data Processing Architectures](https://nexocode.com/blog/posts/lambda-vs-kappa-architecture/)**: In the grand scheme of things, businesses nowadays have 2 main types of infrastructure. Kappa and Lambda.
    * The completed project _**will not**_ fall under these 2 types as I have only covered the `ingestion` and `storage phase.` Leaving only a solid foundation. How the `computation` and `consumption` phase of the pipeline goes will then classify it under the 2 main architectures.
    * **Lambda**: Batch and stream processing, scalable and fault-tolerant but requires x2 processing logic and increased maintenance. Good for continuous data pipelining, IoT systems and real-time data processing.
    * **Kappa**: Stream-processing only, theoretically simpler but requires specialized skillset of distributed systems and stream processing. Good for real-time data processing but the strengths lie in lower maintenance.

![Pipeline Design](https://github.com/user-attachments/assets/3f87e6bc-b7ea-4d15-a276-59af0ad70f1c)
_Image generated with mermaidchart_    

***
# Apache Airflow
This project builds on-top of my airflow basics (Working with Taskflow, familiarisation with airflow UI, playing with some configurations)

* **Task grouping**: An expansion on the `task` decorator, allowing for more organisation of tasks in a dag.
* **Airflow variables**: Analagous to Unity variables even in use-cases, its a key-value store often used for configurations. note to NOT use this for inter-task communication as XCom exists for that.
* **Sensors**: A mechanism allowing Event-driven pipelining. Airflow boasts a wide library of prebuilt ones for common uses with other related softwares but allow easily allows for custom made ones. With taskflow, you can denote a sensor with the decorator`task.sensor`

* **Mixing of pre-built operators and taskflow**: A rather uncommon side of airflow development, but is possible. Just have to be familiar with core concepts from traditional and taskflow API.
* **Image extension**: When adding additional dependencies from pip, you need to get involved with docker. (Refer below)

***
# Apache Kafka
The core of kafka is that it is a fault-tolerant software that distributes incoming data streams into multiple 'topics' (partitions), achiving throughout multiple machines by way of a pub-sub system and kafka 'topics'.

* **Pub-sub**: A core idea that Kafka is built around. The broker can have 0..n producers/consumers which reads from the topics.
* **Topics**: The user-defined partitions to which we produce/consume from. They are best described as distributed commit log files as the messages are sorted, replayable with 1..n consumers, stored on disk, and very scalable.
* **Persistence**: Using a write-ahead log, messages are first stored on disk before system acknowledgement. Combine this with its distributed nature, advancements in disks, configurable retention (expiry dates) and configurable fault tolerance (acks field), Kafka is able to have persistent messages.
* **Offsets**: Analagous to SQL cursors, this feature allows repeatable reads of each topic. This feature is additionally achieved by the structure of the topics
* **Throughput**: Acheived by sequential I/O, zero-copy techniques, storage of messages in binary format and more. Kafka can achieve high volumes of data streams. (The first 2 techniques deserve their own readings).
    * Sequential I/O versus Random I/O: Writing data sequentially to disk is faster than random I/O.
    * Zero-Copy Techniques: Kafka bypasses the application layer, transferring data directly from disk to the network socket, reducing CPU overhead.

***
# CassandraDB
Cassandra is a database with high-write performance, scalability and fault-tolerance due to the absence of traditional locks/transactions, an underlying log-structured merge trees

* **Fault-tolerance**: Cassandra achieves this with a combination of its distributed architecture, tunable consistency, hinted-handoff, anti-entropy repair and replication.
    * **Distributed Architecture**: Picture the database as a non-trivial connected graph as it is a peer-to-peer system. Every cluster node and handle Read/Write requests.
    * **Tunable Consistency**: One can configure the acknowledgements read/write operations require in the operation.
        * `ONE`: Only one replica must acknowledge the operation.
        * `QUORUM`: A majority of replicas must acknowledge the operation.
        * `ALL`: All replicas must acknowledge the operation.
    * **Hinted-handoff**: Should nodes be temporarily down, there will be hints that replay when the node comes back online, ensuring no data is lost.
    * **Anti-entropy repair**: A repair process to ensure no data inconsistencies.
    * **Data replication**: Using a replication factor that can be configured in the code, cassandra ensures >1 replica of data in another cluster node.
* **Snitches**: determine the network topology of the cluster (where the nodes are), helping Cassandra optimize data replication and query routing.
    * **Data Replication**: Ensuring replicas are stored in different physical locations for fault tolerance.
    * **Query Routing**: Optimizing read/write operations by routing requests to the closest node.

![Screenshot 2025-02-10 at 11 55 12 PM](https://github.com/user-attachments/assets/0a314f51-c733-43ea-b81b-f7c08e0ac851)
_Proof of pipeline working_

***
# Docker
The containerisation software in modern day development, it allows for isolated development environments accross different projects on the same machine.

## Volumes
Allowing persistent storage, volumes are essential for databases (e.g., Cassandra) to persist data across container restarts.

## Commands
* `docker compose up -d`: Builds, creates and runs the containers in the background (detached mode) due to the -d flag.
* `docker compose start [Service Name]`: Runs the current container/service
* `docker compose stop [Service Name]`: Stops the current container/service

### Cassandra-Docker commands
* `docker exec -it <cluster_name> cqlsh`: Access the CQL query shell
* `docker exec -it <cluster_name> status`: See the cluster status
* `docker exec -it <cluster_name> status`: See the cluster information (storage details and stuff)

## Extending Images
A common practice in modern SWE, there are a list of reasons why one would do so but in this case, I extended the image as I needed to add package dependencies to the airflow image (`quixstreams`). One could also add custom scripts or configurations, set environment variables, and/or verride entry points or commands.

* **How-to**: 
    1. You need a `dockerfile` that runs the pip installer on the extended image. 
    2. The image in the compose file must also use a different image name so docker knows to run the `dockerfile`. 
    3. If we need >1 requirements, create a `requirements.txt` file and dump the required packages there. 
    4. Afterwards, run `docker build . --tag <custom image name>`
