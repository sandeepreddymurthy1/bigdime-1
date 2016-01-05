/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.common.testutils;

import org.apache.catalina.Context;

public interface ITomcatRunnable extends Runnable {
	public boolean isRunning();
	public void stop();
	public Context getContext();
}
