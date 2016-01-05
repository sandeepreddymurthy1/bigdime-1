/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.ha.event.manager;

import io.bigdime.ha.event.handler.INotficationHandler;

import java.io.IOException;
import java.util.HashMap;

import javax.management.modelmbean.InvalidTargetObjectTypeException;


import akka.actor.ActorRef;

/**
 * @author jbrinnand
 */
public interface IActionHandler {
	public void setSelf (ActorRef self) ;

	@Registration
	public void registerClient(byte[] msg, ActorRef sender, ActorRef receiver)
			throws IOException,InvalidTargetObjectTypeException;
 
	@Peer
	public void setPeer(byte[] msg, ActorRef sender, ActorRef receiver)
			throws IOException,InvalidTargetObjectTypeException;

	@Start
	public void start(byte[] msg, ActorRef sender, ActorRef receiver)
			throws IOException,InvalidTargetObjectTypeException;

	@Stop
	public void stop(byte[] msg, ActorRef sender, ActorRef receiver)
			throws IOException,InvalidTargetObjectTypeException;
	
	@Write
	public void write(byte[] msg, ActorRef sender, ActorRef receiver)
			throws  IOException,InvalidTargetObjectTypeException;
	
	public HashMap<String,INotficationHandler> getNotficationHandlers();
	public void setNotficationHandlers(HashMap<String,INotficationHandler> notificationHandlers);

}
