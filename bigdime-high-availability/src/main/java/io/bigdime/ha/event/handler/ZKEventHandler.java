/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.ha.event.handler;


import io.bigdime.ha.event.manager.IActionHandler;
import io.bigdime.ha.event.manager.MessageHandler;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

import javax.management.modelmbean.InvalidTargetObjectTypeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import akka.actor.ActorRef;

/**
 * @author jbrinnand
 */
public class ZKEventHandler  {
	private static Logger logger = LoggerFactory.getLogger(ZKEventHandler.class);
	@SuppressWarnings("unused")
	private IActionHandler actionHandler;
	private MessageHandler msgHandler;
	
	/**
	 * @param cache
	 * @param actionHandler
	 */
	public ZKEventHandler(IActionHandler actionHandler) {
		this.actionHandler = actionHandler;
		msgHandler = MessageHandler.getInstance(actionHandler);
	}

	public void onReceive(Object msg, ActorRef self, ActorRef sender) throws IOException, InvalidTargetObjectTypeException,  IllegalAccessException, 
		InvocationTargetException, InstantiationException {
		logger.info("ZKEventHandler Message"+msg);
		msgHandler.dispatch(msg, self, sender);
	}
}
