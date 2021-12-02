package com.iitj.cse.cloud;

public class App {

	public static void main(final String... args) {
		String GOAL = System.getenv("GOAL") != null ? System.getenv("GOAL") : "producer";

		if (GOAL.toLowerCase().equals("consumer.kafka")) {
			DataConsumer.main();
		} else if (GOAL.toLowerCase().equals("consumer.spark")) {
			SparkProcessor.main();
		} else if (GOAL.toLowerCase().equals("consumer.flink")) {
			FlinkProcessor.main();
		} else {
			StormProcessor.main();
		}

	}
}