/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.common.testutils.factory;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.hive.conf.HiveConf;

import io.bigdime.common.testutils.HiveRunnable;
import io.bigdime.common.testutils.TestUtils;


public class EmbeddedHiveServer {
	private HiveRunnable hiveRunnable;
	private static EmbeddedHiveServer instance;
	private ExecutorService executor = Executors.newFixedThreadPool(1);
	private static final long sleepTime = 3000;
	private int hivePort =  0;
	private boolean serverStarted = false;
	protected EmbeddedHiveServer () { 
	}
	public static synchronized  EmbeddedHiveServer getInstance() {
		if (instance == null) {
		    System.setProperty(HiveConf.ConfVars.PREEXECHOOKS.varname, " ");
		    System.setProperty(HiveConf.ConfVars.POSTEXECHOOKS.varname, " ");
			instance = new EmbeddedHiveServer();
		}
		return instance;
	}
	public void startMetaStore() throws InterruptedException, IOException  {
		// Start Hive.
		// ***********
		hivePort  = TestUtils.findAvailablePort(0);
		hiveRunnable = new HiveRunnable(Integer.toString(hivePort));
		executor.submit(hiveRunnable);
	    Thread.sleep(sleepTime); 
	    serverStarted  = true;
	}

	public int getHivePort(){
		return hivePort;
	}
	public boolean serverStarted(){
		return serverStarted;
	}
}
