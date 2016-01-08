/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.common.testutils;

import java.io.File;

import javax.servlet.ServletException;

import org.apache.catalina.Context;
import org.apache.catalina.LifecycleException;
import org.apache.catalina.loader.WebappLoader;
import org.apache.catalina.startup.Tomcat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.jersey.spi.spring.container.servlet.SpringServlet;

public class TomcatRunnable implements ITomcatRunnable{
	private Logger logger = LoggerFactory.getLogger(TomcatRunnable.class); 
	
	private final String webAppDir = "src/main/webapp"; 
	private final  String baseDir = "tomcat";
	private Tomcat tomcat = null;
	private boolean isRunning = false;
	private WebappLoader loader;
	private Context context;

	public TomcatRunnable(int port,String applicationName) throws ServletException, LifecycleException {
		tomcat = new Tomcat();
		tomcat.setPort(port);
		tomcat.setBaseDir(baseDir);
		logger.info("Web App Directory is: " + new File(webAppDir).getAbsolutePath());
		tomcat.getHost().setAppBase(new File(webAppDir).getAbsolutePath());

		// Add the app context....
		context = tomcat.addWebapp("/" + 
				applicationName, 
				new File(webAppDir).getAbsolutePath());
		loader = new WebappLoader(SpringServlet.class.getClassLoader());
		context.setLoader(loader);
	}

	
	@Override
	public void run() {
		try {
			tomcat.start();
			isRunning = true;
			tomcat.getServer().await();  	
		} catch (LifecycleException e1) {
			logger.error(e1.getLocalizedMessage());
		}
	}
	@Override
	public boolean isRunning() {
		return isRunning;
	}
	@Override
	public void stop() {
		isRunning = false;
		try {
			tomcat.stop();
		} catch (LifecycleException e) {
			logger.info("Tomcat failed to stop.");
			isRunning = true;
		}
		logger.info("Tomcat stopped.");
	}
	@Override
	public Context getContext() {
		return context;
	}
}

