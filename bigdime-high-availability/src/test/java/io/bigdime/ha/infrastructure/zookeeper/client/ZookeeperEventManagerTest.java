/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.ha.infrastructure.zookeeper.client;

import static io.bigdime.ha.event.manager.common.ZKEventManagerConstants.START;
import static io.bigdime.ha.event.manager.common.ZKEventManagerConstants.STOP;
import static io.bigdime.ha.event.manager.common.ZKEventManagerConstants.WRITE;
import io.bigdime.ha.event.handler.web.context.ZKEventHandlerContextListener;
import io.bigdime.ha.event.manager.Action;
import io.bigdime.ha.event.manager.ActionReader;
import io.bigdime.ha.event.manager.ZKEventManager;
import io.bigdime.ha.event.manager.common.ZKEventManagerConstants;
import io.bigdime.ha.infrastructure.zookeeper.mock.MockEventHandlerBeta;
import io.bigdime.ha.infrastructure.zookeeper.mock.MockEventHandlerBetaActionHandler;
import io.bigdime.ha.zookeeper.client.ZookeeperClient;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import javax.management.modelmbean.InvalidTargetObjectTypeException;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import org.apache.commons.io.FileUtils;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.NettyServerCnxnFactory;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.mock.web.MockServletContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.ITestContext;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

public class ZookeeperEventManagerTest extends ZKClientBaseTest {
	private Logger logger = LoggerFactory.getLogger(ZookeeperEventManagerTest.class);
	private ZooKeeper zk = null;
	private String path = "/bigdime01/services/mars/test-group1";  
	private static final int DEFAULT_TICK_TIME = 50000;
	private ServerCnxnFactory factory; 
	private File dataDir; 
	private File dataLogicDir; 
	private Cache<String, ActorRef> cache;
	private static final String EVENT_HANDLER = "EventHandler";
	private ServletContextListener listener;
	private MockServletContext sc;
	private ServletContextEvent event;
	private String clientName = "MARSZookeeperEventClientAlpha";
	private String clientClazzName = "io.bigdime.ha.infrastructure.zookeeper.mock.MockEventHandlerBetaActionHandler";	
	
	@BeforeTest
	public void beforeTest(final ITestContext testContext) throws IOException, InterruptedException {
		logger.info("RUNNING TEST: " + testContext.getName());
		logger.info("Setting the environment");
		System.setProperty("env", "dev");	
		System.setProperty("zk-host", DEFAULT_HOST);
		System.setProperty("zk-port", String.valueOf(DEFAULT_PORT));
		System.setProperty("sys-zks:client-name", "MARSZookeeperEventClientAlpha");
		System.setProperty(ZKEventManagerConstants.ZK_ENABLED, "true");	
	}
	@BeforeClass 
	public void init() throws IOException, InterruptedException {
		int sessionTimeOut = 1000;
		dataDir = new File (System.getProperty("user.dir") + "/src/test/resources/zkdata");
		dataLogicDir = new File (System.getProperty("user.dir") + "/src/test/resources/zkdata-logic");

		
		// Create the cache.
		cache = CacheBuilder.newBuilder()
				.maximumSize(1000)
				.build();	
	}

	@Test (priority=0)
	public void zooKeeperClient101Connect() throws IOException,
			InterruptedException, KeeperException {
		int sessionTimeOut = 10000;
		final CountDownLatch connectedSignal = new CountDownLatch(1);
		zk = new ZooKeeper(DEFAULT_HOST  + ":" + String.valueOf(DEFAULT_PORT),
				sessionTimeOut, new Watcher() {
			@Override
			public void process(WatchedEvent event) {
				if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {        
					connectedSignal.countDown();      
				}
			}
		}); 
		connectedSignal.await();
		Assert.assertNotNull(zk.getSessionId());
	}
	@Test (priority=1)
	public void zooKeeperClient101CreateGroup() 
			throws IOException, InterruptedException, KeeperException {
		zooKeeperClient101Connect();
		String [] pathElements = path.replaceFirst("^/", "").split("/");
		String zpath =  "";
		String znode = ""; 
		for (int i = 0; i < pathElements.length; i++) {
			znode += "/" + pathElements[i];
			logger.info("Path element is: " + znode);
			Stat stat = zk.exists(znode, true);
			if (stat == null) {
				logger.info("Path does not exist - creating it: " + znode);
				zpath = zk.create(znode,  null,  ZooDefs.Ids.OPEN_ACL_UNSAFE,  
						CreateMode.PERSISTENT);
			}
			else {
				zpath = path;
			}
		}
		logger.info("Constructed path is: " + zpath);
		Assert.assertNotNull(zpath);
		Assert.assertEquals(path, zpath);
	}
	@Test (priority=2)
	public void zooKeeperClient101WriteReadData() 
			throws IOException, InterruptedException, KeeperException {
		zooKeeperClient101CreateGroup();
		ByteBuffer b = ByteBuffer.allocate(1024);
		b.put("testing 123".getBytes(Charset.defaultCharset()));
		byte [] data ;
		data = b.array();

		for (int i = 0; i < 4; i++) {
			String dataPath = path + "/" + "data" +  "-" + i;  
			String createdPath = zk.create(dataPath, data, 
					ZooDefs.Ids.OPEN_ACL_UNSAFE,          
					CreateMode.EPHEMERAL_SEQUENTIAL);
			logger.info("Created path: " + createdPath);
			Assert.assertNotNull(createdPath);

			byte [] zdata = zk.getData(createdPath, null, null);
			Assert.assertNotNull(zdata);
			Assert.assertEquals(new String(data,Charset.defaultCharset()), new String(zdata,Charset.defaultCharset()));
			logger.info("Original data: " + new String(data,Charset.defaultCharset()));
			logger.info("ZClient  data: " + new String(zdata,Charset.defaultCharset()));
		}
	}	
	@Test(priority=3)
	public void curatorFramework101Connect() throws InterruptedException {
		RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
		CuratorFramework client = CuratorFrameworkFactory
				.newClient(DEFAULT_HOST_PORT, retryPolicy);
		client.start();
		// Wait for the  connection to be established.
		Thread.sleep(1000);
		logger.info(client.getState().name());
		Assert.assertNotNull(client);
		Assert.assertTrue(client.getZookeeperClient().isConnected());
		client.close();
	}
	@Test (priority=4)
	public void validateZookeeperClientElectionAsync() throws InterruptedException,
			IOException, KeeperException, InvalidTargetObjectTypeException {
		ActorSystem managerSystem = ActorSystem.create("MessageHandlerTest");
		String sysClientName = System.getProperty("sys-zks:client-name");
		
		// Create the clients.
		ActorRef zookeeperClientAlpha = managerSystem.actorOf(Props.create(
				ZookeeperClient.class, DEFAULT_HOST, 
				DEFAULT_PORT, path,null,0,null), 
				sysClientName);
		ActorRef zookeeperClientBeta = managerSystem.actorOf(Props.create(
				ZookeeperClient.class, DEFAULT_HOST, 
				DEFAULT_PORT, path,null,0,null), 
				"MockEventHandlerClientBeta");	
		ActorRef eventHandlerAlpha = managerSystem.actorOf(Props.create(
				MockEventHandlerBeta.class, cache), EVENT_HANDLER + "ALPHA");
		ActorRef eventHandlerBeta = managerSystem.actorOf(Props.create(
				MockEventHandlerBeta.class, cache), EVENT_HANDLER + "BETA");
			
		// Set up the peer association.
		//**********************
		Action action = Action.getInstance()
				.source("validateAnnotationDiscovery")
				.type("Peer")
				.message("Testing 123")
				.build();
		eventHandlerAlpha.tell(action.getBytes(), zookeeperClientAlpha);
		Thread.sleep(1000);
		zookeeperClientAlpha.tell(action.getBytes(), eventHandlerAlpha);
		Thread.sleep(1000);
		eventHandlerBeta.tell(action.getBytes(), zookeeperClientBeta);
		Thread.sleep(1000);
		zookeeperClientBeta.tell(action.getBytes(), eventHandlerBeta);
		Thread.sleep(1000);
			
		// Start the clients.
		action = Action.getInstance()
				.type(START)
				.source("validateZookeeperClient")
				.message("Simulating a start up scenario.")
				.build();			
		zookeeperClientAlpha.tell(action.getBytes(), ActorRef.noSender());
		zookeeperClientBeta.tell(action.getBytes(), ActorRef.noSender());
		
		action = Action.getInstance()
				.type(STOP)
				.source("validateZookeeperClient")
				.message("Simulating a shutdown scenario.")
				.build();	

		stopCurrentLeader(action, eventHandlerAlpha, eventHandlerBeta,
				EVENT_HANDLER + "ALPHA");
		
		logger.info("*********** Wait starting........");
		Thread.sleep(10000);
		logger.info("*********** Wait stopped........");
		
		stopCurrentLeader(action, eventHandlerAlpha, eventHandlerBeta,
				EVENT_HANDLER + "ALPHA");
		Thread.sleep(2000);
	}
	
	@Test (priority=5)
	public void validateZookeeperClientElectionMessaging()
			throws InterruptedException, IOException, KeeperException,
			InvalidTargetObjectTypeException {
		ActorSystem managerSystem = ActorSystem.create("MessageHandlerTest");

		// Create the clients.
		ActorRef zookeeperClientAlpha = managerSystem.actorOf(Props.create(
				ZookeeperClient.class, DEFAULT_HOST, 
				DEFAULT_PORT, path,null,0,null), 
				"MockEventHandlerClientAlpha");
		ActorRef zookeeperClientBeta = managerSystem.actorOf(Props.create(
				ZookeeperClient.class, DEFAULT_HOST, 
				DEFAULT_PORT, path,null,0,null), 
				"MockEventHandlerClientBeta");	
		ActorRef eventHandlerAlpha = managerSystem.actorOf(Props.create(
				MockEventHandlerBeta.class, cache), EVENT_HANDLER + "ALPHA");
		ActorRef eventHandlerBeta = managerSystem.actorOf(Props.create(
				MockEventHandlerBeta.class, cache), EVENT_HANDLER + "BETA");
			
		// Set up the peer association.
		//**********************
		Action action = Action.getInstance()
				.source("validateAnnotationDiscovery")
				.type("Peer")
				.message("Testing 123")
				.build();
		eventHandlerAlpha.tell(action.getBytes(), zookeeperClientAlpha);
		Thread.sleep(1000);
		zookeeperClientAlpha.tell(action.getBytes(), eventHandlerAlpha);
		Thread.sleep(1000);
		eventHandlerBeta.tell(action.getBytes(), zookeeperClientBeta);
		Thread.sleep(1000);
		zookeeperClientBeta.tell(action.getBytes(), eventHandlerBeta);
		Thread.sleep(1000);
			
		// Start the clients.
		action = Action.getInstance()
				.type(START)
				.source("validateZookeeperClient")
				.message("Simulating a start up scenario.")
				.build();			
		zookeeperClientAlpha.tell(action.getBytes(), ActorRef.noSender());
		zookeeperClientBeta.tell(action.getBytes(), ActorRef.noSender());
		
		action = Action.getInstance()
				.type(STOP)
				.source("validateZookeeperClient")
				.message("Simulating a shutdown scenario.")
				.build();	

		stopCurrentLeader(action, eventHandlerAlpha, eventHandlerBeta,
				EVENT_HANDLER + "ALPHA");
		Thread.sleep(2000);
		
		action = Action.getInstance()
				.type(WRITE)
				.source("validateZookeeperClient")
				.message("Greetings zookeeper.")
				.build();					
		writeCurrentLeader(action, eventHandlerAlpha, eventHandlerBeta,
				EVENT_HANDLER + "ALPHA");
		
		logger.info("*********** Wait starting........");
		Thread.sleep(10000);
		logger.info("*********** Wait stopped........");
		
		action = Action.getInstance()
				.type(STOP)
				.source("validateZookeeperClient")
				.message("Simulating a shutdown scenario.")
				.build();	
		stopCurrentLeader(action, eventHandlerAlpha, eventHandlerBeta,
				EVENT_HANDLER + "ALPHA");
		Thread.sleep(3000);
	}
	@Test (priority=6)
	public void validateZKEventManagerClient() throws Exception {
		ZKEventManager marsEventManagerAlpha = ZKEventManager.getInstance(
				MockEventHandlerBetaActionHandler.class, "MARSEventHandlerAlpha", path,null,null);
		Assert.assertNotNull(marsEventManagerAlpha);
		
		marsEventManagerAlpha.start();
		Thread.sleep(5000);
		// Stop marsEventHandlerAlpha. This should
		// trigger Zookeeper to elect  marsEventManagerBeta 
		// to become the new leader.
		//*******************************************		
		marsEventManagerAlpha.stop();
		Thread.sleep(3000);
		marsEventManagerAlpha.shutdown();
		Thread.sleep(3000);
	}		
	@Test(priority=7)
	public void validateZKEventManagerClientElection() throws Exception {
		ZKEventManager marsEventManagerAlpha = ZKEventManager.getInstance(
				MockEventHandlerBetaActionHandler.class, "MARSEventHandlerAlpha", path,null,null);
		Assert.assertNotNull(marsEventManagerAlpha);
		
		ZKEventManager marsEventManagerBeta = ZKEventManager.getInstance(
				MockEventHandlerBetaActionHandler.class, "MARSEventHandlerBeta", path,null,null);
		Assert.assertNotNull(marsEventManagerBeta);
		
		marsEventManagerAlpha.start();
		marsEventManagerBeta.start();
		String leader = getLeader();
		logger.info("Leader is: " + leader);
		if (leader.equals("MARSEventHandlerAlpha")) {
			marsEventManagerAlpha.stop();
		} 
		else {
			marsEventManagerBeta.stop();
		}
		Thread.sleep(10000);
		if (marsEventManagerAlpha.isActive() == false) {
			marsEventManagerAlpha.shutdown();
		}
		else {
			marsEventManagerBeta.shutdown();
		}
		Thread.sleep(2000);
	}		
	@Test (priority=8)
	public void validateWebContextIntialization() throws InterruptedException,
			IOException, KeeperException, InvalidTargetObjectTypeException {
		
		// Set up the context...
		String sysClientName = System.getProperty("sys-zks:client-name"); 
		if (sysClientName != null) {
			clientName =  sysClientName;
		}
		sc = new MockServletContext("");
		sc.addInitParameter(ZKEventManagerConstants.ZKS_CLIENT_NAME, clientName);
		sc.addInitParameter(ZKEventManagerConstants.ZKS_CLIENT_PATH, path);
		sc.addInitParameter(ZKEventManagerConstants.ZKS_CLIENT_CLASS,
				clientClazzName);
		listener = new ZKEventHandlerContextListener();
		event = new ServletContextEvent(sc);
		listener.contextInitialized(event);	
		ZKEventManager zkEventManager = (ZKEventManager) event
				.getServletContext().getAttribute(
						ZKEventManagerConstants.ZKEVENT_MANAGER_KEY);
		Assert.assertNotNull(zkEventManager);
		Assert.assertTrue(zkEventManager.isActive());
		
		Thread.sleep(3000);
	}
	
	@AfterClass
	public void shutdown() throws InterruptedException, KeeperException, IOException {
		if (listener != null) {
			listener.contextDestroyed(event);
		}
		try {
			List<String> children = zk.getChildren(path, false);    
			if (children != null && children.size() != 0) {
				for (String child : children) {      
					logger.info("Deleting ZK child: " + child);
					zk.delete(path + "/" + child, -1);    
				}
			}
			// Now delete the root / parent of the group.
			zk.delete(path, -1);
		} catch (Exception e) {
			logger.info(e.getMessage());
		}
		finally {
//			while (zks.isRunning()) {
//				zks.shutdown();
//				Thread.sleep(2000);
//			}
			logger.info("Zookeeper has shutdown: ");	
			
			if(factory != null){
				factory.closeAll();
				logger.info("Close Factory");	
				
			}
		}
		logger.info("server is shutdown");

		// Clean up the cache
		cache.invalidateAll();
		cache.cleanUp();	
	}	
	private String stopCurrentLeader(Action action, ActorRef zkClientAlpha,
			ActorRef zkClientBeta, String actorName) throws IOException, InterruptedException,
			KeeperException, InvalidTargetObjectTypeException {
		
		String leader = getLeader();
		logger.info("Leader is: " + leader);
		
		if (leader.equals(actorName)) {
			zkClientAlpha.tell(action.getBytes(), ActorRef.noSender());
		} 
		else {
			zkClientBeta.tell(action.getBytes(), ActorRef.noSender());
		}	
		return leader;
	}
	private void writeCurrentLeader(Action action, ActorRef zkClientAlpha,
			ActorRef zkClientBeta, String actorName) throws IOException, InterruptedException,
			KeeperException, InvalidTargetObjectTypeException {
		
		String leader = getLeader();
		logger.info("Leader is: " + leader);
		
		if (leader.equals(actorName)) {
			zkClientAlpha.tell(action.getBytes(), ActorRef.noSender());
		} 
		else {
			zkClientBeta.tell(action.getBytes(), ActorRef.noSender());
		}	
	}
	private String getLeader() throws IOException, InterruptedException,
			KeeperException, InvalidTargetObjectTypeException {
		Thread.sleep(3000);
		zooKeeperClient101Connect();
		String leader = null;
		byte [] zdata = zk.getData(path, null, null);
		Assert.assertNotNull(zdata);
		
		ActionReader reader = ActionReader.getInstance(zdata);
		String message = reader.getMessage();
		logger.info("Leader is: " + message);
		String [] msg = message.split(" ");
		leader = msg[0];
		
		return leader;
	}
}
