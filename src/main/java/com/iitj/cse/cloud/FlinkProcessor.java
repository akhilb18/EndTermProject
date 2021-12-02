package com.iitj.cse.cloud;

import java.util.Properties;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;

public class FlinkProcessor {

	public static void main(final String... args) {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		final Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Commons.KAFKA_SERVER);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "FlinkConsumerGroup");

		DataStream<String> messageStream = env
				.addSource(new FlinkKafkaConsumer010<>(Commons.TOPIC_NAME_KAFKA, new SimpleStringSchema(), props));

		messageStream.flatMap(new Tokenizer())
				.keyBy(0).sum(1).print();

		try {
			env.execute();
		} catch (Exception e) {
		}
	}

	public static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		@Override
		public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
			String[] tokens = value.toLowerCase().split("\\W+");

			for (String token : tokens) {
				if (token.length() > 0) {
					out.collect(new Tuple2<>(token, 1));
				}
			}
		}
	}
}
