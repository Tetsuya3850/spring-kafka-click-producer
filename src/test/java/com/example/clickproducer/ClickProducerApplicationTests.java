package com.example.clickproducer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.springframework.boot.test.context.SpringBootTest.WebEnvironment.RANDOM_PORT;

@EnableKafka
@EmbeddedKafka(
		partitions = 1,
		topics = { "pageClickTopic" },
		brokerProperties = { "listeners=PLAINTEXT://localhost:9092", "port=9092" }
)
@SpringBootTest(webEnvironment = RANDOM_PORT)
class ClickProducerApplicationTests {

	@Autowired
	EmbeddedKafkaBroker embeddedKafkaBroker;

	@Autowired
	private TestRestTemplate restTemplate;

	private static final String PAGE_CLICK_TOPIC = "pageClickTopic";

	@Test
	void contextLoads() {
		Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(
				"testT", "false", embeddedKafkaBroker
		);
		consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
		consumerProps.put(JsonDeserializer.TRUSTED_PACKAGES, "com.example.clickproducer");
		DefaultKafkaConsumerFactory<String, PageClickEvent> cf = new DefaultKafkaConsumerFactory<>(consumerProps);
		Consumer<String, PageClickEvent> consumer = cf.createConsumer();
		consumer.subscribe(Collections.singleton(PAGE_CLICK_TOPIC));

		String userName = "Tetsuya";
		String page = "HomeScreen";
		PageClickEvent pageClickEvent = new PageClickEvent(
				userName, page
		);
		HttpEntity<Object> pageClickEventHttpEntity = new HttpEntity<>(pageClickEvent);
		restTemplate.exchange("/click", HttpMethod.POST, pageClickEventHttpEntity, Void.class);

		ConsumerRecord<String, PageClickEvent> received = KafkaTestUtils.getSingleRecord(consumer, PAGE_CLICK_TOPIC);
		assertEquals(userName, received.value().getUserName());
		assertEquals(page, received.value().getPage());
	}

}
