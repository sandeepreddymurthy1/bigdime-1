/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.handler.kafka;

import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import io.bigdime.core.InputDescriptor;

/**
 * Encapsulates the fields(topic, partition) that are needed to connect to
 * kafka.
 * 
 * @author Neeraj Jain
 *
 */
@Component
public class KafkaInputDescriptor implements InputDescriptor<String> {

	private String topic;
	private int partition;

	public KafkaInputDescriptor() {

	}

	public KafkaInputDescriptor(String topic, int partition) {
		this.topic = topic;
		this.partition = partition;
	}

	/**
	 * Gets the next topic:partition to process. For Kafka, we just need to
	 * return the same topic and partition.
	 * 
	 * @param availableInputs
	 *            list of available topic:partition, assumed to be in the order
	 *            in which they need to be processed
	 * @param lastInput
	 *            last topic:partition that was last processed, successfully or
	 *            unsuccessfully
	 * @return topic:partition to process next
	 */
	public String getNext(List<String> availableInputs, String lastInput) {
		return lastInput;
	}

	@Override
	public void parseDescriptor(String descriptor) {
		if (StringUtils.isBlank(descriptor))
			throw new IllegalArgumentException("descriptor can't be null or empty");
		String[] stringParts = descriptor.split(":");
		if (stringParts == null || stringParts.length != 2) {
			throw new IllegalArgumentException("descriptor must be in topic:partition format");
		} else {
			topic = stringParts[0].trim();
			partition = Integer.valueOf(stringParts[1]);
		}
	}

	@Override
	public String toString() {
		return topic + ":" + partition;
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public int getPartition() {
		return partition;
	}

	public void setPartition(int partition) {
		this.partition = partition;
	}

}