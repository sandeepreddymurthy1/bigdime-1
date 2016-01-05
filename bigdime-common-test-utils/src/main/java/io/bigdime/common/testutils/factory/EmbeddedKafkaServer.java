/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.common.testutils.factory;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @author jbrinnand
 * this helper class will create kafka server and provide the management.
 */
public class EmbeddedKafkaServer {
	private static Logger logger = LoggerFactory.getLogger(EmbeddedKafkaServer.class);
	private KafkaServer kafkaServer;
	private Properties properties;
	private final long sleepTime = 3000l;
	protected EmbeddedKafkaServer() { 
		properties = new Properties();
	}
	
	public static EmbeddedKafkaServer newServer() {
		return new EmbeddedKafkaServer();
	}
	public EmbeddedKafkaServer host(String host) {
		properties.setProperty("host.name", host);
		return this;
	}
	public EmbeddedKafkaServer port(String port) {
		properties.setProperty("port", port);
		return this;
	}	
	public EmbeddedKafkaServer brokerId(String brokerId) {
		properties.setProperty("broker.id",  Integer.valueOf(brokerId).toString());  
		return this;
	}	
	public EmbeddedKafkaServer zookeeperConnection(
			String zookeeperHost,
			int zookeeperPort,
			String rootZnode) {
		properties.setProperty("zookeeper.connect", zookeeperHost + ":"
				+ zookeeperPort +"/" +  rootZnode);
		return this;
	}
	public EmbeddedKafkaServer logDirs(String logDirs) {
		properties.setProperty("log.dirs", logDirs);
		return this;
	}
	public EmbeddedKafkaServer logRetention(TimeUnit logTimeUnit, 
			Integer logRetentionDuration) {
		if (logTimeUnit == TimeUnit.MINUTES) {
			properties.setProperty("log.retention.minutes", logRetentionDuration.toString()); 
		}
		else if (logTimeUnit == TimeUnit.HOURS) {
			properties.setProperty("log.retention.hours", logRetentionDuration.toString()); 
		}	
		return this;
	}	
	public EmbeddedKafkaServer numPartitions(String numPartitions) {
		properties.setProperty("num.partitions", numPartitions);
		return this;
	}
	public EmbeddedKafkaServer brokers(String[] brokers) {
		properties.setProperty("brokers", brokers.toString());
		return this;
	}
	public EmbeddedKafkaServer defaultReplicationFactor(String replicationFactor) {
		properties.setProperty("default.replication.factor",  replicationFactor); 
		return this;
	}	
	public EmbeddedKafkaServer messageMaxBytes(String messageMaxBytes) {
		properties.setProperty("message.max.bytes",  messageMaxBytes); 
		return this;
	}	
	public EmbeddedKafkaServer replicaFetchMaxBytes(String replicaFetchMaxBytes) {
		properties.setProperty("replica.fetch.max.bytes",  replicaFetchMaxBytes); 
		return this;
	}	
	public EmbeddedKafkaServer build() throws Exception {
		KafkaConfig kafkaConfig = new KafkaConfig(properties);
		kafkaServer = new KafkaServer(kafkaConfig, kafka.utils.SystemTime$.MODULE$);
		logger.info("Kafka - hostName: " + kafkaServer.config().hostName());
		logger.info("Kafka - port: " + kafkaServer.config().port());			
		return  this;
	}
	public void startup() {
		kafkaServer.startup();
	}
	public void shutdown() throws Exception {
		
		while(kafkaServer.kafkaController().isActive()) {
			logger.info("Waiting on a Kafka server to shutdown - status is: " +  
					kafkaServer.kafkaController().isActive());
			kafkaServer.kafkaController().shutdown();
			Thread.sleep(sleepTime);
		}
		kafkaServer.socketServer().shutdown();
		kafkaServer.shutdown();
		logger.info("KafkaServer {} run state is: {} ",
			kafkaServer.config().hostName(), 
			kafkaServer.kafkaController().isActive());
	}	
	public KafkaServer getServer() {
		return this.kafkaServer;
	}
	public boolean isActive() {
		return kafkaServer.kafkaController().isActive();
	}
}
