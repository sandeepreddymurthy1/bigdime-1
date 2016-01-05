/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.ha.event.listener;

import static io.bigdime.ha.event.manager.common.ZKEventManagerConstants.NOTIFICATION;
import static io.bigdime.ha.event.manager.common.ZKEventManagerConstants.ZK_CLIENT_LISTENER;
import io.bigdime.ha.event.manager.Action;
import io.bigdime.ha.event.manager.ActionReader;

import java.io.IOException;
import java.nio.charset.Charset;

import javax.management.modelmbean.InvalidTargetObjectTypeException;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorEventType;
import org.apache.curator.framework.api.CuratorListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import akka.actor.ActorRef;

/**
 * @author jbrinnand
 * 
 */
public class ZKEventListener implements CuratorListener {
	private static Logger logger = LoggerFactory.getLogger(ZKEventListener.class);
	private ActorRef clientEventHandler;
	private ActorRef parent;
	
	public ZKEventListener(ActorRef clientEventHandler, ActorRef parent) {
		this.clientEventHandler = clientEventHandler;
		this.parent = parent;
	}

	@Override
	public void eventReceived(CuratorFramework framework, CuratorEvent event)
			throws Exception {
		if (event.getPath() == null) {
			logger.warn("Empty path received by the listener. No action is being taken. "
					+ "Event is: {} ",  event.getType());
			return;
		}
		logger.info("Listener {} for {} Received an event  - path {} type {} : ", 
				parent.path().name(), 
				clientEventHandler.path().name(), 
				event.getPath(),
				event.getType());
		byte[] data = null; 
		if (event.getType().equals(CuratorEventType.WATCHED)) {
			 data = framework.getData().forPath(event.getPath());
			 logger.info("Framework data: "
				+ new String(framework.getData().forPath(event.getPath()),Charset.defaultCharset())); 
		}
		else if (event.getType().equals(CuratorEventType.GET_DATA)) {
			 data = event.getData(); 
		}
		if (data != null && data.length > 0) {
			sendMsg(data);
		}
	}
	private void sendMsg (byte[] data) throws IOException, InvalidTargetObjectTypeException {
		ActionReader reader = ActionReader.getInstance(data);
		Action action = reader.toAction() 
				.type(NOTIFICATION)
				.subtype(reader.getType())
				.source(ZK_CLIENT_LISTENER)
				.trail(ZK_CLIENT_LISTENER)
				.build();
		logger.info("Sending client {} a notification {} ", 
				clientEventHandler.path().name(), action.toString());
		clientEventHandler.tell(action.toString().getBytes(Charset.defaultCharset()), ActorRef.noSender());
	}
}
