/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core.constants;

public final class AdaptorConstants {
	private static final AdaptorConstants instance = new AdaptorConstants();

	private AdaptorConstants() {
	}

	public static AdaptorConstants getInstance() {
		return instance;
	}

	public static final long SLEEP_WHILE_WAITING_FOR_DATA = 3000;
	public static final int ERROR_THRESHOLD = 3;

}
