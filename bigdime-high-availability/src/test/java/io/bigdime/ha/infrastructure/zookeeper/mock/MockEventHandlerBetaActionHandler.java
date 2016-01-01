/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.ha.infrastructure.zookeeper.mock;

import io.bigdime.ha.event.manager.Action;
import io.bigdime.ha.event.manager.ActionHandler;
import io.bigdime.ha.event.manager.ActionReader;
import io.bigdime.ha.event.manager.LeaderElection;
import io.bigdime.ha.event.manager.Notification;
import io.bigdime.ha.event.manager.Peer;
import io.bigdime.ha.event.manager.Registration;
import io.bigdime.ha.event.manager.Stop;
import io.bigdime.ha.event.manager.Write;

import java.io.IOException;

import javax.management.modelmbean.InvalidTargetObjectTypeException;

import org.codehaus.jackson.JsonProcessingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import akka.actor.ActorRef;

import com.google.common.cache.Cache;

/**
 * @author jbrinnand
 */
public class MockEventHandlerBetaActionHandler extends ActionHandler {
	private Logger logger = LoggerFactory
			.getLogger(MockEventHandlerBetaActionHandler.class);
	private ActorRef peer;
	private ActorRef self;
	private Cache<String, Object> cache;
	
	public MockEventHandlerBetaActionHandler(Cache<String, Object> cache) { 
		this.cache = cache;
	}	
	
	public MockEventHandlerBetaActionHandler(ActorRef self) {
		this.self = self;
	}
	
	public void setSelf (ActorRef self) {
		this.self = self;
	}
	
	@Override
	@Registration
	public void registerClient(byte[] msg, ActorRef sender, ActorRef receiver)
			throws JsonProcessingException, IOException,
			InvalidTargetObjectTypeException {
		logger.info("Received a message {} from {}. ", ActionReader
				.getInstance(msg).getType(), ActionReader.getInstance(msg)
				.getSource());
		Action action = ActionReader.getInstance(msg).toAction();
		action.trail(self.path().name()).build();
		peer.tell(action.getBytes(), sender);
	}

	@Peer
	@Override
	public void setPeer(byte[] msg, ActorRef sender, ActorRef receiver)
			throws JsonProcessingException, IOException,
			InvalidTargetObjectTypeException {
		ActionReader reader = ActionReader.getInstance(msg);
		logger.info("Message is: " + reader.toString());
		this.peer = receiver;
		logger.info("Set the peer: " + this.peer.path().name());
	}
	@LeaderElection
	public void leaderElection(byte[] msg, ActorRef sender, ActorRef receiver)
			throws JsonProcessingException, IOException,
			InvalidTargetObjectTypeException {
		ActionReader reader = ActionReader.getInstance(msg);
		logger.info("A new leader has been elected: " + reader.toString());
	}
	@Notification
	public void handleNotification(byte[] msg, ActorRef sender, ActorRef receiver)
			throws JsonProcessingException, IOException,
			InvalidTargetObjectTypeException {
		ActionReader reader = ActionReader.getInstance(msg);
		logger.info("Actor {} has received a notification{} : ",  self.path().name(), reader.toString());
	}
	@Override
	@Write
	public void write(byte[] msg, ActorRef sender, ActorRef receiver)
			throws JsonProcessingException, IOException,
			InvalidTargetObjectTypeException {
		super.write(msg, sender, receiver);
		// Tell zookeeper client to write the data to zookeeper.
		//******************************************
		Action action = ActionReader.getInstance(msg).toAction();
		action.trail(self.path().name()).build();
		peer.tell(action.getBytes(), sender);	
	}	
	@Override
	@Stop
	public void stop(byte[] msg, ActorRef sender, ActorRef receiver)
			throws JsonProcessingException, IOException,
			InvalidTargetObjectTypeException {
		super.stop(msg, sender, receiver);
		// Tell zookeeper client to shutdown / close.
		//**********************************
		Action action = ActionReader.getInstance(msg).toAction();
		action.trail(self.path().name()).build();
		peer.tell(action.getBytes(), sender);		
	}


}
