package com.iitj.cse.cloud;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class DataConsumer {



	private static Consumer<String, String> consumerLaunch() {
		final Properties consumerProperties = new Properties();
		consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Commons.KAFKA_SERVER);
		consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaConsumerGroup");
		consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

		// Pulling the Properties
		final Consumer<String, String> topicConsumer = new KafkaConsumer(consumerProperties);

		topicConsumer.subscribe(Collections.singletonList(Commons.TOPIC_NAME_KAFKA));
		return topicConsumer;
	}
	
	public static void main(String... args) {
		ConcurrentMap<String, Integer> counterStrucuture = new ConcurrentHashMap<>();
		final Consumer<String, String> topicConsumer = consumerLaunch();
		while (true) {
			final ConsumerRecords<String, String> consumerRecords = topicConsumer.poll(1000);
			consumerRecords.forEach(record -> {
				String word = record.value();

				int count = counterStrucuture.containsKey(word) ? counterStrucuture.get(word) : 0;
				counterStrucuture.put(word, ++count);

			});
			topicConsumer.commitAsync();
		}
	}
}
