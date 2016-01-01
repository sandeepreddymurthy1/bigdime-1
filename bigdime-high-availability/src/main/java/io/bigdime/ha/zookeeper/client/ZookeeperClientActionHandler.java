/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.ha.zookeeper.client;

import static io.bigdime.ha.event.manager.common.ZKEventManagerConstants.LEADER_ELECTION;
import static io.bigdime.ha.event.manager.common.ZKEventManagerConstants.STOP;
import io.bigdime.ha.event.listener.ZKEventListener;
import io.bigdime.ha.event.listener.ZKEventTreeListener;
import io.bigdime.ha.event.manager.Action;
import io.bigdime.ha.event.manager.ActionHandler;
import io.bigdime.ha.event.manager.ActionReader;
import io.bigdime.ha.event.manager.Peer;
import io.bigdime.ha.event.manager.Registration;
import io.bigdime.ha.event.manager.Start;
import io.bigdime.ha.event.manager.Stop;
import io.bigdime.ha.event.manager.Write;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.concurrent.TimeUnit;

import javax.management.modelmbean.InvalidTargetObjectTypeException;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CuratorListener;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import akka.actor.ActorRef;

/**
 * @author jbrinnand
 */
public class ZookeeperClientActionHandler extends ActionHandler implements LeaderSelectorListener {
	private static Logger logger = LoggerFactory.getLogger(ZookeeperClientActionHandler.class);
	private ActorRef peer;
	private ActorRef self;
	private String path; 
	private String listenerPath;
	private String host;
	private int port; 
	private String clientHost;
	private int clientPort; 	
	private String clientPath; 
	private String clientData; 
	private LeaderSelector leaderSelector;
	private CuratorFramework client;
	private CuratorListener eventListener;
	private TreeCache treeCache;
	private long defaultSleepTime = 3000l;
	private int defaultByteSize = 1024;
	public ZookeeperClientActionHandler (ActorRef self, String zkHost, 
										int zkPort, String path,
										String clientHost,int clientPort,String listenerPath) {
		this.self = self;
		this.host = zkHost;
		this.port = zkPort;
		this.path = path;
		this.clientHost = clientHost;
		this.clientPort = clientPort;		
		this.listenerPath = listenerPath;
	}
	@Override
	public void setSelf(ActorRef self) {
		this.self = self;
	}

	@Override
	@Registration
	public void registerClient(byte[] msg, ActorRef sender, ActorRef receiver) 
			throws IOException, InvalidTargetObjectTypeException {
		super.registerClient(msg, sender, receiver);
		logger.info ("Sending the message to a peer: " + 
				ActionReader.getInstance(msg).toString());		
		try {
			ActionReader reader = ActionReader.getInstance(msg);
			zkSetData(msg,  reader.getType(), self, reader.getMessage());
		} catch (Exception e) {
			throw new IOException(e.getMessage());
		}
	}
	@Peer
	@Override
	public void setPeer(byte[] msg, ActorRef sender, ActorRef receiver)
			throws IOException,InvalidTargetObjectTypeException {
		super.setPeer(msg, sender, receiver);
		this.peer = receiver;
		logger.info("Set the peer: " + this.peer.path().name()) ; 
	}
	@Start
	@Override
	public void start(byte[] msg, ActorRef sender, ActorRef receiver)
			throws IOException,InvalidTargetObjectTypeException  {
		super.start(msg, sender, receiver);
		try {
			logger.info ("Starting up....building the client: " + self.path().name());
			client = this.buildClient(clientPath, clientData);
			client.start();
			logger.info ("Starting up....building the listener: " + self.path().name());
			buildListener();
		} catch (Exception e) {
			logger.info ("Zookeeper client failed to start:" + e.getMessage());
		}
	}
	@Override
	@Stop
	public void stop(byte[] msg, ActorRef sender, ActorRef receiver)
			throws IOException,InvalidTargetObjectTypeException {
		super.stop(msg, sender, receiver);
		logger.info("Telling all clients that {} is stopping.", 
				self.path().name());
		if (leaderSelector != null && leaderSelector.hasLeadership()) {
			logger.info("Closing the selector. Leader status: " + 
					leaderSelector.hasLeadership());
			leaderSelector.close();
		}	
		try {
			String message = peer.path().name() + " " + "has stopped.";
			zkSetData(msg,  STOP, self, message);
			Thread.sleep(defaultSleepTime);
		} catch (Exception e) {
			logger.info("Failed to send the stop message");
		}
		logger.info("Closing the client");
		if(treeCache != null){
			treeCache.close();
		}
		client.close();
		logger.info("Client closed state : " + client.getState().toString());
	}
	@Write
	@Override
	public void write(byte[] msg, ActorRef sender, ActorRef receiver)
			throws IOException,InvalidTargetObjectTypeException {
		super.write(msg, sender, receiver);
		// Tell zookeeper client to write the data to zookeeper.
		//******************************************
		ActionReader reader = ActionReader.getInstance(msg);
		try {
			zkSetData(msg,  reader.getType(), self, reader.getMessage());
		} catch (Exception e) {
			logger.error("Could not write to zookeeper. Data {}: ", reader.toString());
		}
	}	

	private CuratorFramework buildClient (String clientPath, String clientData) throws Exception {
		RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
		logger.info(self.path().name() + " starting the client.\n");
		client = CuratorFrameworkFactory.newClient(host + ":" + port, retryPolicy);
		return client;
	}
	private void buildListener () throws Exception {
		logger.info("path is: " + path);

		InterProcessMutex lock = new InterProcessMutex(client, path);
		try {
			logger.info("Acquiring a lock. ");
			if (lock.acquire(defaultSleepTime, TimeUnit.MILLISECONDS) ) {
				if (client.checkExists().forPath(path) == null ) {
					logger.error("Creating the path : " + path);
					client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
						.forPath(path, "Starting up".getBytes(Charset.defaultCharset()));	
				}
			}
		} catch (Exception e) {
			logger.error("Client did not acquire the lock: " + e.getMessage());
		}
		finally {
			if (lock.isAcquiredInThisProcess()) {
				logger.info("Releasing the lock. " + lock.getParticipantNodes().toString());
				lock.release();
			}
		}	
		logger.info("Setting up the watch path: " + path);
		client.getData().watched().inBackground().forPath(path);
		
		logger.info("Creating the ZKEventListener. ");
		eventListener = new ZKEventListener(peer, self);
		client.getCuratorListenable().addListener(eventListener);	

		logger.info("Setting up Tree Cache Listner : " + listenerPath);
		if(listenerPath != null){
			treeCache = TreeCache.newBuilder(client,listenerPath).build();
			treeCache.start();
			treeCache.getListenable().addListener(new ZKEventTreeListener(peer, self));
		}

		logger.info("Creating the leader selector : " + path);
		leaderSelector = new LeaderSelector(client, path, this);
		leaderSelector.autoRequeue();		
		logger.info("Starting the leader: " + path);
		leaderSelector.start();
	}
	private void zkSetData(byte[] msg, String msgType, ActorRef sender,
			String message) throws Exception {
		Action action = Action.getInstance()
				.type(msgType)
				.source(sender.path().name())
				.trail(self.path().name())
				.message(message,clientHost,clientPort)
				.build();	
		logger.info ("Sending a {} message {} to zookeeper: ", 
				msgType, ActionReader.getInstance(action.getBytes()).toString());		
		
		String clientBasePath = path;
		if (client.checkExists().forPath(clientBasePath) == null) {
			client.create().withMode(CreateMode.EPHEMERAL)
				.forPath(clientBasePath);
		}
		logger.info("Writing data to zookeeper - path {} ",  clientBasePath);
		client.setData().forPath(clientBasePath, action.getBytes());
	}

	@Override
	public void stateChanged(CuratorFramework client, ConnectionState newState) {
		logger.info("State Changed: " + newState + " for client " + self.path().name());
		try {
			byte[] data = client.getData().forPath(path);
			logger.info("***************** Data from path is :{} " , new String (data,Charset.defaultCharset()));
		} catch (Exception e) {
			logger.error("Invalid data or path {}", e.getMessage());
		}
	}

	@Override
	public void takeLeadership(CuratorFramework client) throws Exception {
		logger.info("****** Becoming the leader: " + self.path().name());
		if (peer != null) {
			logger.info("Telling all clients that {} is the Leader.", 
					self.path().name());
			byte[] msg = new byte[defaultByteSize];
			zkSetData(msg,  LEADER_ELECTION, self, 
					self.path().name() + " is the new Leader");
			
			// ***********************
			// sending the leader election message to peer.
			Action action = Action.getInstance()
					.type(LEADER_ELECTION)
					.source(self.path().name())
					.trail(self.path().name())
					.message("Becoming the leader")
					.build();
			peer.tell(action.getBytes(), self);
		}
		try {
			Thread.currentThread().join();
		}
		catch (InterruptedException e) {
			logger.error(self.path().name() + " was interrupted.");
			Thread.currentThread().interrupt();
		}
		finally {
			logger.error(self.path().name() + " relinquishing leadership.\n");
		}
		logger.info("Exiting.... relinquishing leadership.\n");
	}
}
