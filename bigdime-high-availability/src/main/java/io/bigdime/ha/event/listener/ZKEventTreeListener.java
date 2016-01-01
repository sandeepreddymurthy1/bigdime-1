/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.ha.event.listener;


import static io.bigdime.ha.event.manager.common.ZKEventManagerConstants.NOTIFICATION;
import static io.bigdime.ha.event.manager.common.ZKEventManagerConstants.ZK_CLIENT_TREECACHE_LISTENER;
import io.bigdime.ha.event.manager.Action;
import io.bigdime.ha.event.manager.ActionReader;

import java.io.IOException;
import java.nio.charset.Charset;

import javax.management.modelmbean.InvalidTargetObjectTypeException;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import akka.actor.ActorRef;

/**
 * @author mnamburi
 * 
 */
public class ZKEventTreeListener implements TreeCacheListener {
	private static Logger logger = LoggerFactory.getLogger(ZKEventTreeListener.class);
	private ActorRef clientEventHandler;
	private ActorRef parent;

	public ZKEventTreeListener(ActorRef clientEventHandler, ActorRef parent) {
		this.clientEventHandler = clientEventHandler;
		this.parent = parent;
	}

	private void sendMsg (byte[] data) throws IOException, InvalidTargetObjectTypeException {
		ActionReader reader = ActionReader.getInstance(data);
		logger.info("Reader client a notification {} "
				, reader.toString());		
		if(!reader.validateReader()){
			return;
		}

		Action action = reader.toAction() 
				.type(NOTIFICATION)
				.subtype(reader.getType())
				.trail(ZK_CLIENT_TREECACHE_LISTENER)
				.build();
		logger.info("Sending client a notification {} "
				, action.toString());
		clientEventHandler.tell(action.toString().getBytes(Charset.defaultCharset()), ActorRef.noSender());
	}

	@Override
	public void childEvent(CuratorFramework frameWork, TreeCacheEvent event)
			throws Exception {
		ChildData childData  = event.getData();
		if(childData != null){
			byte[] data = childData.getData();
			if (data != null && data.length > 0) {
				sendMsg(data);
			}
		}
	}
}
