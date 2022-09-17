package beta.kafka;

import beta.kafka.consumer.Consumer;
import beta.kafka.payload.Message;
import beta.kafka.producer.Producer;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;


@RunWith(SpringRunner.class)
@DirtiesContext
@SpringBootTest()
public class ApplicationTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(ApplicationTest.class);

    private static String TOPIC_NAME = "dev_campaign";
    private static String GROUP_ID = "dev_campaign-group_test";

    @Autowired
    private Producer producer;

    @Autowired
    private Consumer consumer;

    private KafkaMessageListenerContainer<String, Message> container;

    private BlockingQueue<ConsumerRecord<String, String>> consumerRecords;

    @ClassRule
    public static EmbeddedKafkaRule embeddedKafka = new EmbeddedKafkaRule(1, true, TOPIC_NAME);

    @Before
    public void setUp() {
        consumerRecords = new LinkedBlockingQueue<>();

        ContainerProperties containerProperties = new ContainerProperties(TOPIC_NAME);

        Map<String, Object> consumerProperties = KafkaTestUtils.consumerProps(
                GROUP_ID, "false", embeddedKafka.getEmbeddedKafka());

        DefaultKafkaConsumerFactory<String, Message> consumer = new DefaultKafkaConsumerFactory<>(consumerProperties);

        container = new KafkaMessageListenerContainer<>(consumer, containerProperties);
        container.setupMessageListener((MessageListener<String, String>) record -> {
            LOGGER.debug("Listened message='{}'", record);
            consumerRecords.add(record);
        });
        container.start();

        ContainerTestUtils.waitForAssignment(container, embeddedKafka.getEmbeddedKafka().getPartitionsPerTopic());
    }

    @After
    public void tearDown() {
        container.stop();
    }

    @Test
    public void send_event() throws InterruptedException, IOException {
        long timestamp = new Date().getTime();

        Message msg = new Message();
        msg.setPath("a/b/c");
        msg.setCampaignType("test");
        msg.setTimestamp(timestamp);

        producer.publish(msg);

        String received = consumer.test(GROUP_ID);

        ObjectMapper mapper = new ObjectMapper();
        String json = mapper.writeValueAsString(msg);
        System.out.println("8888888888888888888888888888888888888" + received);
        System.out.println("999999999999999999999999999999999999" + json);

        assertEquals(received, json);
}

}
