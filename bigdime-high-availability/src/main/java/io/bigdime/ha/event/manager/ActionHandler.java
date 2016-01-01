/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.ha.event.manager;

import io.bigdime.ha.event.handler.INotficationHandler;

import java.io.IOException;
import java.util.HashMap;

import javax.management.modelmbean.InvalidTargetObjectTypeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import akka.actor.ActorRef;

public abstract class ActionHandler implements IActionHandler {
	private static Logger logger = LoggerFactory.getLogger(ActionHandler.class);
	
	private HashMap<String,INotficationHandler> notficationHandlers = null;
	@Registration
	public void registerClient(byte[] msg, ActorRef sender, ActorRef receiver) 
			throws IOException, InvalidTargetObjectTypeException {
		logger.info ("Received a message {} from {}. ", 
				ActionReader.getInstance(msg).getType(),
				ActionReader.getInstance(msg).getSource());
	}
	@Peer
	public void setPeer(byte[] msg, ActorRef sender, ActorRef receiver) 
			throws IOException, InvalidTargetObjectTypeException {
		ActionReader reader = ActionReader.getInstance(msg);
		logger.info("Message is: " + reader.toString());
	}	
	@Start
	public void start(byte[] msg, ActorRef sender, ActorRef receiver)
			throws IOException,InvalidTargetObjectTypeException  {
		ActionReader reader = ActionReader.getInstance(msg);
		logger.info("Received a {} message: ", reader.getType());	
	}
	@Stop
	public void stop(byte[] msg, ActorRef sender, ActorRef receiver)
			throws IOException,InvalidTargetObjectTypeException {
		ActionReader reader = ActionReader.getInstance(msg);
		logger.info("Received a {} message: ", reader.getType());
	}
	@Write
	public void write(byte[] msg, ActorRef sender, ActorRef receiver)
			throws IOException,InvalidTargetObjectTypeException {
		ActionReader reader = ActionReader.getInstance(msg);
		logger.info("Received a {} message: ", reader.getType());		
	}
	
	@Override
	public void setNotficationHandlers(HashMap<String,INotficationHandler> notficationHandlers){
		this.notficationHandlers = notficationHandlers; 
	}
	
	@Override
	public HashMap<String,INotficationHandler> getNotficationHandlers(){
		return notficationHandlers;
	}
	
	/**
	 * 
	 * @param name
	 * @return
	 */
	public INotficationHandler getNotficationHandler(String handlerName){
		for(String handler : notficationHandlers.keySet()){
			if(handler.contains(handlerName)){
				return notficationHandlers.get(handler);
			}
		}
		return null;
	}
}
