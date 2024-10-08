# kafka-project

## Set up
1. Project was built with Java 11 and maven 3.9.8
2. Clone repository and run maven clean & install
3. Run KafkaRestService.java to start the Rest Server

### Kafka Docker Deployment
1. Follow this guide to install kafka on the OS you are working on: https://learn.conduktor.io/kafka/how-to-install-apache-kafka-on-windows/
2. Open a new terminal in project root folder and run this command to deploy kafka on your local machine: docker-compose up -d

### APIs
1. http://localhost:3003/kafka/publish/allData -> To load publish all the data to Kafka
2. http://localhost:3003/kafka/topic/:topic_name/:offset/:N
   1. This will return the next N records from the kafka topic topic_name, starting at and including the record at offset _offset_ 
   2. Example: http://localhost:3003/kafka/topic/people_data/490/300
