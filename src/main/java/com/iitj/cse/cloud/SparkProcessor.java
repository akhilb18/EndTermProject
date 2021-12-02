package com.iitj.cse.cloud;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import scala.Tuple2;

public class SparkProcessor {

	public static void main(final String... args) {
		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Commons.KAFKA_SERVER);
		kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, "SparkConsumerGroup");

		Collection<String> topics = Arrays.asList(Commons.TOPIC_NAME_KAFKA);

		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("SparkConsumerApplication");

		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

		final JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(jssc,
				LocationStrategies.PreferConsistent(), ConsumerStrategies.Subscribe(topics, kafkaParams));

		JavaDStream<String> lines = stream
				.map((Function<ConsumerRecord<String, String>, String>) kafkaRecord -> kafkaRecord.value());

		JavaDStream<String> words = lines
				.flatMap((FlatMapFunction<String, String>) line -> Arrays.asList(line.split(" ")).iterator());

		JavaPairDStream<String, Integer> wordMap = words
				.mapToPair((PairFunction<String, String, Integer>) word -> new Tuple2<>(word, 1));

		JavaPairDStream<String, Integer> wordCount = wordMap
				.reduceByKey((Function2<Integer, Integer, Integer>) (first, second) -> first + second);

		wordCount.print();

		jssc.start();
		try {
			jssc.awaitTermination();
		} catch (InterruptedException e) {
		}

	}

}
