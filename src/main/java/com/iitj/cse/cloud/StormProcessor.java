package com.iitj.cse.cloud;

import java.util.UUID;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;

public class StormProcessor {
	public static void main(final String... args) {
		Config config = new Config();
		config.setDebug(true);
		config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
		String zkConnString = Commons.ZOOKEEPER_SERVER;
		String topic = Commons.TOPIC_NAME_KAFKA;
		BrokerHosts hosts = new ZkHosts(zkConnString);

		SpoutConfig kafkaSpoutConfig = new SpoutConfig(hosts, topic, "/" + topic, UUID.randomUUID().toString());
		kafkaSpoutConfig.bufferSizeBytes = 1024 * 1024 * 4;
		kafkaSpoutConfig.fetchSizeBytes = 1024 * 1024 * 4;
		kafkaSpoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("kafka-spout", new KafkaSpout(kafkaSpoutConfig));
		builder.setBolt("word-splitter", new SplitBolt()).shuffleGrouping("kafka-spout");
		builder.setBolt("word-counter", new CountBolt()).shuffleGrouping("word-splitter");

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("StormConsumerGroup", config, builder.createTopology());

		try {
			Thread.sleep(10000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		cluster.shutdown();
	}
}