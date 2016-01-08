/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.ha.infrastructure.zookeeper.mock;

import io.bigdime.ha.event.manager.MessageHandler;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.Charset;

import javax.management.modelmbean.InvalidTargetObjectTypeException;

import org.codehaus.jackson.JsonProcessingException;

import akka.actor.ActorRef;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;

import com.google.common.cache.Cache;

/**
 * @author jbrinnand
 */
public class MockZookeeperClient extends UntypedActor {
	private LoggingAdapter logger = Logging.getLogger(getContext().system(), this);
	private int messageCount = 0;
	MockZookeeperActionHandler actionHandler;
	MessageHandler msgHandler;
	
	public MockZookeeperClient (Cache<String, ActorRef> cache) {
		actionHandler = new MockZookeeperActionHandler(getSelf());
		msgHandler = MessageHandler.getInstance(actionHandler);
	}
	@Override
	public void onReceive(Object msg) throws JsonProcessingException, 
		IOException, InvalidTargetObjectTypeException,  IllegalAccessException, 
		InvocationTargetException, InstantiationException {
		messageCount++;
		
		String formattedString = String.format(
				"Actor '%s' received the message '%s' from Actor %s "
				+ "using Thread %s message count is: %s ",
	             getSelf().path().name(), new String((byte[])msg,Charset.defaultCharset()), sender().path().name(), 
	             Thread.currentThread().getName(), messageCount);
		
		logger.info (formattedString);
		msgHandler.dispatch(msg, getSelf(), sender());
	}
}
