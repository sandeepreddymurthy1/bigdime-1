/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.ha.zookeeper.client;


import io.bigdime.ha.event.handler.ZKEventHandler;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

import javax.management.modelmbean.InvalidTargetObjectTypeException;

import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;

/**
 * @author jbrinnand
 */
public class ZookeeperClient extends UntypedActor {
	private LoggingAdapter logger = Logging.getLogger(getContext().system(), this);
	private ZookeeperClientActionHandler actionHandler;
	private ZKEventHandler eventHandler;
	
	public ZookeeperClient(String host, int port, String path,
							String clientHost,int clientPort,String listenerPath) throws Exception {
		actionHandler = new ZookeeperClientActionHandler(getSelf(), host, port, path,clientHost,clientPort,listenerPath);
		eventHandler = new ZKEventHandler(actionHandler);	
	}
	@Override
	public void onReceive(Object msg) throws InvocationTargetException,
			InstantiationException, IOException,
			InvalidTargetObjectTypeException, IllegalAccessException {
		logger.debug("Forward the message to eventHandler");
		eventHandler.onReceive(msg, getSelf(), sender());		
	}	
}