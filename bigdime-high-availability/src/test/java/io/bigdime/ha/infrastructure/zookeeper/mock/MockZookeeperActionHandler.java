/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.ha.infrastructure.zookeeper.mock;

import io.bigdime.ha.event.manager.Action;
import io.bigdime.ha.event.manager.ActionHandler;
import io.bigdime.ha.event.manager.ActionReader;
import io.bigdime.ha.event.manager.Peer;
import io.bigdime.ha.event.manager.Registration;

import java.io.IOException;

import javax.management.modelmbean.InvalidTargetObjectTypeException;

import org.codehaus.jackson.JsonProcessingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import akka.actor.ActorRef;

/**
 * @author jbrinnand
 */
public class MockZookeeperActionHandler  extends ActionHandler {
	private Logger logger = LoggerFactory.getLogger(MockZookeeperActionHandler.class);
	private ActorRef peer;
	private ActorRef self;
	
	public MockZookeeperActionHandler () {}
	
	public MockZookeeperActionHandler (ActorRef self) {
		this.self = self;
	}
	
	public void setSelf (ActorRef self) {
		this.self = self;
	}
	
	@Override
	@Registration
	public void registerClient(byte[] msg, ActorRef sender, ActorRef receiver) 
			throws JsonProcessingException, IOException, InvalidTargetObjectTypeException {
		logger.info ("Received a message {} from {}. ", 
				ActionReader.getInstance(msg).getType(),
				ActionReader.getInstance(msg).getSource());
		try {
			Action action = ActionReader.getInstance(msg).toAction();
			action.trail(self.path().name()).build();
			zkSetData(action.getBytes());
		} catch (Exception e) {
			throw new IOException(e.getMessage());
		}
	}
	@Peer
	@Override
	public void setPeer(byte[] msg, ActorRef sender, ActorRef receiver)
			throws JsonProcessingException, IOException,
			InvalidTargetObjectTypeException {
		ActionReader reader = ActionReader.getInstance(msg);
		logger.info("Message is: " + reader.toString());
		this.peer = receiver;
		logger.info("Set the peer: " + this.peer.path().name()) ; 
	}
	private void zkSetData (byte[] msg) throws Exception {
		logger.info ("Sending a message to zookeeper: " + 
				ActionReader.getInstance(msg).toString());
	}	
}
