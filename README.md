# Kafka Streaming Project

This project demonstrates a Kafka streaming setup with both producer and consumer implementations. The project includes Docker configurations for setting up Kafka and Zookeeper, Python scripts for producing and consuming messages, and Jupyter notebooks for data preparation.

## Project Structure

```
data/                     # Directory containing data files
    fact_table_part_1.json
    ...
    fact_table_part_10.json
deployment/               # Directory containing deployment configurations
    docker-compose.yml    # Docker Compose configuration file
    ...
install_docker.sh         # Script to install Docker and Docker Compose in the VM
kafka-consumer/
    consumer.py
    multithread-consumer.py
kafka-producer/
    kafkaadmin.py
    producer.py
    requirements.txt
notebooks/
    BatchPipeline.ipynb   # Jupyter notebook for batch pipeline
    BatchPipeline.py      # Python script for batch pipeline
    BatchPipelineTwo.ipynb # Jupyter notebook for second batch pipeline
    BatchPipelineTwo.py   # Python script for second batch pipeline
    PrepareStreamingData.ipynb # Jupyter notebook for preparing streaming data
    StreamingPipeline.ipynb # Jupyter notebook for streaming pipeline
```

## Setup

### Prerequisites

- Docker
- Docker Compose
- Python 3.11
- Jupyter Notebook

### Installation

1. **Install Docker and Docker Compose in the VM:**

   Run the `install_docker.sh` script to install Docker and Docker Compose.

   ```sh
   ./install_docker.sh
   ```

2. **Start Spark Driver and Kafka in the VM:**

   Navigate to the `deployment` directory and run Docker Compose.

   ```sh
   cd deployment
   docker-compose up -d
   ```

## Usage

### Producing Messages

The producer script reads data from JSON files and sends messages to the Kafka topic.

1. **Run the producer:**

   ```sh
   python kafka-producer/producer.py
   ```

### Consuming Messages

The consumer scripts read messages from the Kafka topic and print them to the console.

1. **Run the single-threaded consumer:**

   ```sh
   python kafka-consumer/consumer.py
   ```

2. **Run the multi-threaded consumer:**

   ```sh
   python kafka-consumer/multithread-consumer.py
   ```

### Data Preparation

The Jupyter notebook `PrepareStreamingData.ipynb` prepares the data for streaming by splitting it into multiple JSON files.

1. **Run the notebook:**

   Open the notebook in Jupyter and run the cells to prepare the data.

   ```sh
   jupyter notebook notebooks/PrepareStreamingData.ipynb
   ```
