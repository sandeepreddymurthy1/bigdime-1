/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core.handler;

/**
 * @author Neeraj Jain
 */
public final class MemoryChannelInputHandlerConstants {
	private static final MemoryChannelInputHandlerConstants instance = new MemoryChannelInputHandlerConstants();

	private MemoryChannelInputHandlerConstants() {
	}

	public static MemoryChannelInputHandlerConstants getInstance() {
		return instance;
	}

	public static final String BATCH_SIZE = "batchSize";
	public static final int DEFAULT_BATCH_SIZE = 1;
}
