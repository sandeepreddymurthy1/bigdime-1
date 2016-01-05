/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.common.testutils.factory;

import io.bigdime.common.testutils.ITomcatRunnable;
import io.bigdime.common.testutils.TomcatRunnable;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.servlet.ServletException;

import org.apache.catalina.Context;
import org.apache.catalina.LifecycleException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;


public class EmbeddedTomcatFactory {
	private ITomcatRunnable tomcat;
	private static final int defaultInstances  = 3;
	private ExecutorService executor = Executors.newFixedThreadPool(defaultInstances);
	private Logger logger = LoggerFactory.getLogger(EmbeddedTomcatFactory.class); 
	private final Integer defaultPort = 8988;
	private String defaultAppName = "embedded-tomcat";
	private static EmbeddedTomcatFactory instance;
	private boolean tomcatStarted = false;
	private final long sleepTime = 1000;
	private final int retriesAttempt = 3;
	protected EmbeddedTomcatFactory () { 
	}
	
	public static synchronized  EmbeddedTomcatFactory getInstance() {
		if (instance == null) {
			instance = new EmbeddedTomcatFactory();
		}
		return instance;
	}
	public void startTomcat() throws ServletException, LifecycleException, InterruptedException {
		startTomcat(defaultPort,defaultAppName);
	}
	public void startTomcat(int port,String appName) throws ServletException, 
		LifecycleException, InterruptedException {
		Preconditions.checkNotNull(port);
		Preconditions.checkNotNull(appName);
		// Start tomcat.
		// ***********
		tomcat = new TomcatRunnable(port,appName);
		executor.submit(tomcat);
		while (tomcat.isRunning() == tomcatStarted) {
			Thread.sleep(sleepTime);
		}
	}

	public boolean isRunning () {
		return tomcat.isRunning();
	}
	
	public Context getContext() {
		return tomcat.getContext();
	}
	public void shutdownTomcat() throws InterruptedException {
		int timeout = 2;
		int retries = 0;
		while (tomcat.isRunning() && retries < retriesAttempt) {
			tomcat.stop();
			logger.info("Waiting for  tomcat to stop. Number of retries is: "
					+ retries);
			Thread.sleep(sleepTime);
			retries++;
		}
		logger.info("Tomcat running status is: " + tomcat.isRunning());
		executor.awaitTermination(timeout, TimeUnit.SECONDS);
		logger.info("Closing down the service in : " + timeout + " "
				+ TimeUnit.SECONDS);
		logger.info("Stopped the Service. Executor terminated status is: "
				+ executor.isTerminated());
		executor.shutdown();
	}
}
