"# producer-consumer"

##Create kafka topic
kafka-topics.bat --create --topic dev --bootstrap-server localhost:9092

# run
java -jar app.jar --server.port=8182 
