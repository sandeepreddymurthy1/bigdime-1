/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.ha.infrastructure.zookeeper.client;

import static io.bigdime.ha.event.manager.common.ZKEventManagerConstants.*;
import io.bigdime.ha.event.manager.Action;
import io.bigdime.ha.infrastructure.zookeeper.mock.MockEventHandlerBeta;
import io.bigdime.ha.infrastructure.zookeeper.mock.MockZookeeperClient;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.Charset;

import javax.management.modelmbean.InvalidTargetObjectTypeException;

import org.codehaus.jackson.JsonProcessingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeTest;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

@ContextConfiguration(locations = {"classpath:applicationContext.xml"})
public class MessageHandlerTest extends AbstractTestNGSpringContextTests {
	private Logger logger = LoggerFactory.getLogger(MessageHandlerTest.class);
	private ActorSystem managerSystem;
    private ActorRef eventHandler; 	
    private ActorRef zookeeperClient;
    private static final String EVENT_HANDLER = "EventHandler";
    
	@BeforeTest
	public void beforeTest() throws IOException, InterruptedException {
		logger.info("Setting the environment");
		System.setProperty("env", "dev");	
	}
	@BeforeClass
	public void beforeClass() throws InterruptedException {
		Cache<String, ActorRef>cache = CacheBuilder.newBuilder()
				.maximumSize(1000)
				.build();	

		managerSystem = ActorSystem.create("MessageHandlerTest");
		eventHandler = managerSystem.actorOf(
				Props.create(MockEventHandlerBeta.class, cache), EVENT_HANDLER);
		zookeeperClient = managerSystem.actorOf(
				Props.create(MockZookeeperClient.class, cache), ZOOKEEPER_CLIENT);	
	}
	@Test(priority=0)
	public void validatePeerInitializationMessage()
			throws JsonProcessingException, IllegalArgumentException,
			IOException, InvalidTargetObjectTypeException,
			IllegalAccessException, InvocationTargetException,
			InstantiationException, InterruptedException {

		Action action = Action.getInstance()
				.source("validateAnnotationDiscovery")
				.type("Peer")
				.message("Testing 123")
				.build();
		
		Assert.assertNotNull(eventHandler);
		Assert.assertNotNull(zookeeperClient);

		eventHandler.tell(action.getBytes(), zookeeperClient);
		Thread.sleep(2000);
		zookeeperClient.tell(action.getBytes(), eventHandler);
		Thread.sleep(2000);
	}
	@Test(priority=1)
	public void validateRegistrationMessage() throws InterruptedException,
			JsonProcessingException, IllegalArgumentException, IOException,
			InvalidTargetObjectTypeException, IllegalAccessException,
			InvocationTargetException, InstantiationException {
		
		validatePeerInitializationMessage();
		
		Action registration = Action.getInstance()
				.type("Registration")
				.source("validateAnnotationDiscovery")
				.message("Testing 123")
				.build();
		
		Assert.assertNotNull(eventHandler);
		Assert.assertNotNull(zookeeperClient);

		// Send a registration message to the event handler; it
		// should forward the mesage to the zookeeper client.
		//******************************************
		eventHandler.tell(registration.getBytes(), ActorRef.noSender());
		Thread.sleep(2000);
	}	
	@Test (priority=2)
	public void validateStartStopMessages() throws JsonProcessingException,
			IllegalArgumentException, IOException,
			InvalidTargetObjectTypeException, IllegalAccessException,
			InvocationTargetException, InstantiationException,
			InterruptedException {
		
		Action action = Action.getInstance()
				.source("validateAnnotationDiscovery")
				.type("Start")
				.message("Start the actor.")
				.build();

		// Send a registration message to the event handler; it
		// should forward the mesage to the zookeeper client.
		//******************************************
		eventHandler.tell(action.getBytes(), ActorRef.noSender());
		Thread.sleep(2000);
		zookeeperClient.tell(action.getBytes(), ActorRef.noSender());
		
		action = Action.getInstance()
				.source("validateAnnotationDiscovery")
				.type("Stop")
				.message("Stop the actor.")
				.build();
		
		eventHandler.tell(action.getBytes(), ActorRef.noSender());
		Thread.sleep(1000);
		zookeeperClient.tell(action.getBytes(), ActorRef.noSender());	
		Thread.sleep(1000);
	}	
	@Test(priority=3)
	public void validateLeaderElectionMessage() throws JsonProcessingException,
			IllegalArgumentException, IOException,
			InvalidTargetObjectTypeException, IllegalAccessException,
			InvocationTargetException, InstantiationException,
			InterruptedException {

		Action action = Action.getInstance()
				.type("LeaderElection")
				.source(ZOOKEEPER_CLIENT)
				.message("Simulating zookeeper client.")
				.build();
		logger.info("Sending a leader election message: " + action.toString());
		eventHandler.tell(action.toString().getBytes(Charset.defaultCharset()), ActorRef.noSender());
	}	
	@Test(priority=4)
	public void validateNotificationMessage() throws JsonProcessingException,
			IllegalArgumentException, IOException,
			InvalidTargetObjectTypeException, IllegalAccessException,
			InvocationTargetException, InstantiationException,
			InterruptedException {

		Action action = Action.getInstance()
				.type("Notification")
				.source(ZK_CLIENT_LISTENER)
				.message("Simulating a notification from the zookeeper event listener.")
				.build();
		logger.info("Sending notification: " + action.toString());
		eventHandler.tell(action.toString().getBytes(Charset.defaultCharset()), ActorRef.noSender());
	}
}