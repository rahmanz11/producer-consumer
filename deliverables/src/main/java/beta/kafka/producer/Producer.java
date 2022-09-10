package beta.kafka.producer;

import beta.kafka.payload.*;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

import java.util.Properties;
import java.util.UUID;

@Service
public class Producer {

    private static final Logger logger = LoggerFactory.getLogger(Producer.class);

    @Value("${kafka.topic.dev}")
    private String topicDev;

    Properties properties = new Properties();
    // Create Kafka producer
    KafkaProducer<String, String> producer;
    Gson gson;

    public Producer() {
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("acks"             , "0");
        properties.put("retries"          , "1");
        properties.put("batch.size"       , "20971520");
        properties.put("linger.ms"        , "100");
        properties.put("max.request.size" , "2097152");
        properties.put("compression.type" , "gzip");
        properties.put("key.serializer"   , "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer" , "org.apache.kafka.common.serialization.StringSerializer");

        gson = new GsonBuilder().setPrettyPrinting().create();
    }

    public int publish(Message message) {
        logger.debug("----- incoming message = {}", gson.toJson(message));
        UUID id = UUID.randomUUID();
        try {
            producer = new KafkaProducer<>(properties);
            producer.send(new ProducerRecord<>(topicDev, id.toString(), gson.toJson(message)));
        } finally {
            try {
                producer.close();
            } catch (Exception e) {}
        }

        return HttpStatus.CREATED.value();
    }
}
