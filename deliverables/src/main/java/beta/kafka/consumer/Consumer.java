package beta.kafka.consumer;

import beta.kafka.payload.*;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

@Component
public class Consumer {

    private static final Logger logger = LoggerFactory.getLogger(Consumer.class);

    @Value("${kafka.topic.dev}")
    private String topicDev;
    @Value("${kafka.topic.prod}")
    private String topicProd;
    @Value("${kafka.topic.preprod}")
    private String topicPreprod;

    @Value("${kafka.consumer.group.dev}")
    private String consumerGroupDev;
    @Value("${kafka.consumer.group.prod}")
    private String consumerGroupProd;
    @Value("${kafka.consumer.group.preprod}")
    private String consumerGroupPreprod;

    Properties properties = new Properties();
    KafkaConsumer<String, String> consumer;
    Gson gson;

    public Consumer() {
        properties.put("bootstrap.servers"          , "localhost:9092");
        properties.put("key.deserializer"           , "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer"         , "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("max.partition.fetch.bytes"  , "2097152");
        properties.put("auto.offset.reset"          , "earliest");
        properties.put("enable.auto.commit"         , "false");

        gson = new GsonBuilder().setPrettyPrinting().create();
    }

    @Scheduled(fixedRate = 5, timeUnit = TimeUnit.SECONDS)
    public void readMessageDev() {
        logger.debug("running at: {}", new Date());
        List<Message> messages = new ArrayList<>();

        properties.put("group.id", consumerGroupDev);
        consumer = new KafkaConsumer<>(properties);

        try {

            consumer.subscribe(Arrays.asList(topicDev));
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (TopicPartition tp : records.partitions()) {
                List<ConsumerRecord<String, String>> partRecords = records.records(tp);
                long lastOffset = 0;
                for (ConsumerRecord<String, String> record : partRecords) {
                    lastOffset = record.offset();
                    try {
                        Message message = gson.fromJson(record.value(), Message.class);
                        messages.add(message);
                    } catch (Throwable t) {
                        t.printStackTrace();
                        continue;
                    }
                }

                consumer.commitSync(Collections.singletonMap(tp, new OffsetAndMetadata(lastOffset + 1)));
            }
        } finally {
            consumer.close();
        }

        logger.debug(gson.toJson(messages));
    }
}
