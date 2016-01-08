/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.handler.constants;

public final class KafkaReaderHandlerConstants {
	private static final KafkaReaderHandlerConstants instance = new KafkaReaderHandlerConstants();

	private KafkaReaderHandlerConstants() {
	}

	public static KafkaReaderHandlerConstants getInstance() {
		return instance;
	}

	public static final String OFFSET_DATA_DIR = "offset-data-dir";
	public static final String INPUT_DESC = "input-desc";
	public static final String MESSAGE_SIZE = "message-size";
	public static final String BROKERS = "brokers";
}