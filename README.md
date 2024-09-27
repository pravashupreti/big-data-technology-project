# big-data-technology-project

Big data technology project

# Architecture

![Architecture](docs/architecture.jpg)

## Explanation of the Architecture

To provide a more detailed explanation of the architecture:

1. The Python script acts as a data fetcher from Reddit. It utilizes the Reddit API to retrieve the desired data, such as comments, posts, or other relevant information. This script is responsible for fetching the data and publishing it to Kafka.

2. Kafka, a distributed streaming platform, serves as a message broker in this architecture. It receives the data published by the Python script and ensures reliable and scalable data transfer between the producer (Python script) and the consumer (Spark Streaming).

3. Spark Streaming, a real-time processing framework, consumes the data from Kafka in micro-batches. It processes the Reddit comments using various transformations and operations to derive keywords. Spark Streaming provides the ability to handle large volumes of data in real-time and perform complex computations efficiently.

4. The processed data is then stored in an HBase database. HBase is a NoSQL database that offers high scalability and fast read/write operations. It is well-suited for storing large amounts of structured or semi-structured data, making it an ideal choice for this architecture.

5. Another Spark SQL job is responsible for fetching the data from HBase and dumping it into HDFS. HDFS, the Hadoop Distributed File System, is a distributed file system designed to store and process large datasets across multiple machines. It provides fault tolerance and high throughput, making it suitable for big data processing.

6. Sqoop, a data transfer tool, is used to export the data from HDFS to MySQL. Sqoop simplifies the process of transferring data between Hadoop and relational databases. It allows for seamless integration between the Hadoop ecosystem and traditional databases like MySQL.

7. Finally, Grafana is used for data visualization. It connects to the MySQL database and runs SQL queries to fetch the final data. Grafana provides a user-friendly interface to create dashboards and visualizations, enabling users to gain insights from the processed data in a visually appealing manner.

This architecture leverages the power of big data technologies to process and analyze data from Reddit, enabling the extraction of valuable insights and facilitating data-driven decision making.
