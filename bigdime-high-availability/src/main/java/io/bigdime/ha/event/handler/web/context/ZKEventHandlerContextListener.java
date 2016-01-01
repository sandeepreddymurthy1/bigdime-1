/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.ha.event.handler.web.context;

import static io.bigdime.ha.event.manager.common.ZKEventManagerConstants.CLASS;
import static io.bigdime.ha.event.manager.common.ZKEventManagerConstants.NAME;
import static io.bigdime.ha.event.manager.common.ZKEventManagerConstants.ZKS_NOTIFICATION_CLIENT_CLASS;
import io.bigdime.ha.event.client.utils.ZKEventManagerUtils;
import io.bigdime.ha.event.handler.INotficationHandler;
import io.bigdime.ha.event.manager.ZKEventManager;
import io.bigdime.ha.event.manager.common.ZKEventManagerConstants;

import java.util.Enumeration;
import java.util.HashMap;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author jbrinnand/mnamburi
 * ZKEventHandlerContextListner is tie with Serlet Context life cycle.
 * this class will be invoked upon adding entry in web.xml;
 */

public class ZKEventHandlerContextListener implements ServletContextListener {
	private static Logger logger = LoggerFactory.getLogger(ZKEventHandlerContextListener.class); 
	private static final String FORWARD_SLASH = "/";
	private ZKEventManager zkEventManager;
	private HashMap<String,Class<INotficationHandler>> notficationHandlers;
	private long defaultSleepTime = 5000l;
	@Override
	public void contextInitialized(ServletContextEvent sce) {
		logger.info("Context Initializing.....");
		ServletContext servletContext = sce.getServletContext();
		String zkEnabled = System.getProperty(ZKEventManagerConstants.ZK_ENABLED);
		logger.info(ZKEventManagerConstants.ZK_ENABLED + " is: " + zkEnabled);

		String sysClientName  = System.getProperty("sys-zks:client-name");
		String clientName = servletContext.getInitParameter("zks:client-name");
		if (sysClientName != null) {
			clientName = sysClientName;
		}
		String clientPath = servletContext.getInitParameter("zks:client-path");
		String clientClass = servletContext.getInitParameter("zks:client-class");
		String listenerPath = servletContext.getInitParameter("zks:client-listener-path");
		String serviceDiscovery = servletContext.getInitParameter("zks:client-service-discovery");
		logger.info("Client class: " + clientClass);

		Class<?> clientClazz = null;
		try {
			clientClazz  = Class.forName(clientClass);
			logger.info("Client class: " + clientClazz.getSimpleName());
		} catch (Exception e) {
			logger.info("Failed to acquire the Client's actionHandler: " + clientClass);
			return;
		}


		if (Boolean.valueOf(zkEnabled).equals(true) ) {
			logger.info("ZookeeperEventManager is enabled. Client path {} listenerPath{} : " + clientPath,listenerPath);
			// Initialize the ZKManager.
			//***********************
			try {
				buildNotificationHandlers(servletContext);
				if(serviceDiscovery != null){
					clientPath = clientPath + FORWARD_SLASH + ZKEventManagerUtils.getProcessID();
				}
				zkEventManager = ZKEventManager.getInstance(
						clientClazz, clientName, clientPath,listenerPath,notficationHandlers);
				zkEventManager.start();
			} catch (Exception e) {
				logger.error("ERROR:{} did not start exception={}", clientName,e);
			}
		}
		logger.info("Storing the zookeeper event manager in the servlet context");
		servletContext.setAttribute(ZKEventManagerConstants.ZKEVENT_MANAGER_KEY, zkEventManager);
	}
	@Override
	public void contextDestroyed(ServletContextEvent sce) {
		logger.info("Context shutting down.");
		String zkEnabled = System
				.getProperty(ZKEventManagerConstants.ZK_ENABLED);
		if (Boolean.valueOf(zkEnabled).equals(true) ) {
			try {
				logger.error("INFO:{}", "Stopping ZKEventManager");
				zkEventManager.stop();
				Thread.sleep(defaultSleepTime);
				zkEventManager.shutdown();
				Thread.sleep(defaultSleepTime);
			} catch (InterruptedException e) {
				logger.error("ERROR:{} did not stop", "ZKEventManager");
			}
		}
	}
	


	@SuppressWarnings("unchecked")
	public void buildNotificationHandlers(ServletContext servletContext) throws ClassNotFoundException
	{
		Enumeration<String> initParameters = servletContext.getInitParameterNames();
		notficationHandlers= new HashMap<String,Class<INotficationHandler>>();
		Class<INotficationHandler> notificationClientClazz = null;
		String notificationClientName;
		String initParameter = null;
		while(initParameters.hasMoreElements()){
			initParameter = initParameters.nextElement();
			if(initParameter.startsWith(ZKS_NOTIFICATION_CLIENT_CLASS)){
				notificationClientClazz = (Class<INotficationHandler>) Class.forName(servletContext.getInitParameter(initParameter));
				notificationClientName = servletContext.getInitParameter(initParameter.replace(CLASS,NAME));
				notficationHandlers.put(notificationClientName, notificationClientClazz);
			}
		}
	}
}
