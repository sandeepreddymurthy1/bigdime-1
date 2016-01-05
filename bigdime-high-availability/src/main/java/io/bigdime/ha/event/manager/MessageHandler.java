/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.ha.event.manager;
import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;

import javax.management.modelmbean.InvalidTargetObjectTypeException;

import org.codehaus.jackson.JsonProcessingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import akka.actor.ActorRef;

/**
 * @author jbrinnand
 */
public class MessageHandler {
	private HashMap<String, Method> messageHandler;
	private Logger logger = LoggerFactory.getLogger(MessageHandler.class);
	private Object actionHandler;
	
	protected MessageHandler(Object actionHandler) {
		messageHandler = new HashMap<String, Method>();
		Class<?> clazz = actionHandler.getClass();
		this.actionHandler = actionHandler;
		
		Method[] methods = clazz.getDeclaredMethods();
		for (int i = 0; i < methods.length; i++) {
			logger.info("Inspecting method: " + methods[i].getName());
			Annotation[] an = methods[i].getAnnotations();
			for (int j = 0; j < an.length; j++) {
				logger.info(an[j].annotationType().getSimpleName());
				logger.info("Registering actionName {} actionMethod {} ", 
						an[j].annotationType().getSimpleName( ), 
						methods[i]);
				messageHandler.put(an[j].annotationType().getSimpleName(), methods[i]);
			}
		}
	}
	public static MessageHandler getInstance (Object actor) {
		return new MessageHandler(actor);
	}
	public void dispatch (Object msg, ActorRef sender, ActorRef receiver) 
		throws IOException, InvalidTargetObjectTypeException,  InvocationTargetException, InstantiationException, IllegalAccessException {
		String actionType = "";
		ActionReader reader = null; 
		if (msg instanceof byte[]) {
			reader = ActionReader.getInstance(msg);
			actionType = reader.getType();
			logger.info("Got an action: " + actionType); 
		}
		Method m = messageHandler.get(actionType);
		if (m != null) {
			m.invoke(actionHandler, msg, sender, receiver);
		}
	}	
}
