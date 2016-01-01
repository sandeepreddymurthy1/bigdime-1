/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.ha.infrastructure.zookeeper.client;

import io.bigdime.common.testutils.factory.EmbeddedZookeeperServerFactory;
import io.bigdime.ha.event.handler.INotficationHandler;
import io.bigdime.ha.event.handler.web.context.ZKEventHandlerContextListener;
import io.bigdime.ha.event.manager.ZKEventManager;
import io.bigdime.ha.event.manager.common.ZKEventManagerConstants;
import io.bigdime.ha.infrastructure.zookeeper.mock.MockDVSClient;
import io.bigdime.ha.infrastructure.zookeeper.mock.MockEventHandlerBetaActionHandler;
import io.bigdime.ha.infrastructure.zookeeper.mock.MockEventNotficationHandlerActionHandler;
import io.bigdime.ha.infrastructure.zookeeper.mock.MockServiceNode;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;

import javax.management.modelmbean.InvalidTargetObjectTypeException;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import org.apache.commons.io.FileUtils;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.mock.web.MockServletContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.ITestContext;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import static io.bigdime.ha.event.manager.common.ZKEventManagerConstants.*;

/**
 * 
 * @author mnamburi
 *
 */
public class ZookeeperServiceNotificationsTests extends ZKClientBaseTest {
	private Logger logger = LoggerFactory.getLogger(ZookeeperServiceNotificationsTests.class);
	private TestingServer _zkServer;
//	@Value("${test.zk.dataDirectory}") private String zkDataDirectory = null;
//	@Value("${test.zk.dataLogDirectory}") private String zkDataLogDirectory = null;

	private String zkDataDirectory = null;
	private String zkDataLogDirectory = null;

	private EmbeddedZookeeperServerFactory zksUtil = null;
	private static final String USER_DIR = "user.dir";

	private ServletContextListener listener;
	private MockServletContext sc;
	private ServletContextEvent event;

	private static final String listnerPath = "/bigdime02";
	private static final String dvsPath = "/bigdime02/services/data-validation-service";
	private static final String dpsPath = "/bigdime02/services/data-publish-service";

	private static final String rdbmsPath = "/bigdime02/data-adapters/rdbms-data-adapter";
	private static final String DVS_NAME_ONE = "data-validation-service-one";
	private static final String DVS_NAME_TWO = "data-validation-service-two";

	private static final String DPS_NAME_ONE = "data-publish-service-one";
	private static final String DPS_NAME_TWO = "data-publish-service-two";

	private static final String RDBMS_NAME = "rdbms-data-adapter";
	private static final String DVS_NAME = "data-validation-service";
	private static final String DPS_NAME = "data-publish-service";

	private String sysClientName = "io.bigdime.ha.infrastructure.zookeeper.mock.MockEventNotficationHandlerActionHandler";
	private String mockDVSClientName = "io.bigdime.ha.infrastructure.zookeeper.mock.MockDVSClient";	
	private String mockDPSClientName = "io.bigdime.ha.infrastructure.zookeeper.mock.MockDPSClient";	

	private String dataDir = null;
	private String dataLogDir = null;


	@BeforeTest
	public void beforeTest(final ITestContext testContext) throws Exception {
		logger.info("Setting the environment");
		System.setProperty("env", "dev");	
		System.setProperty(ZKEventManagerConstants.ZK_ENABLED,"true");
		System.setProperty("zk-port",String.valueOf(DEFAULT_PORT));
	}

	@Test (priority=0)
	public void testTreeCacheListner() throws InterruptedException, ClassNotFoundException{
		logger.info("testTreeCacheListner the environment");

		HashMap<String,Class<INotficationHandler>>  handlers =	buildListners();

		ZKEventManager dvsEventManagerAlpha = ZKEventManager.getInstance(
				MockEventHandlerBetaActionHandler.class,DVS_NAME_ONE, dvsPath+"/dvsInstanceOne",null,null);
		Assert.assertNotNull(dvsEventManagerAlpha);

		System.setProperty(CLIENT_PORT, "8090");
		ZKEventManager dpsEventManagerAlpha = ZKEventManager.getInstance(
				MockEventHandlerBetaActionHandler.class,DPS_NAME_ONE, dpsPath+"/dpsInstanceOne",null,null);
		Assert.assertNotNull(dpsEventManagerAlpha);

		ZKEventManager rdbmsEventManagerAlpha = ZKEventManager.getInstance(
				MockEventNotficationHandlerActionHandler.class, RDBMS_NAME, rdbmsPath,listnerPath,handlers);
		Assert.assertNotNull(rdbmsEventManagerAlpha);

		rdbmsEventManagerAlpha.start();
		Thread.sleep(3000);
		dvsEventManagerAlpha.start();
		Thread.sleep(10000);
		dpsEventManagerAlpha.start();
		Thread.sleep(10000);

		INotficationHandler iDVSNotficationHandler = rdbmsEventManagerAlpha.getNotificationHandlers().get(DVS_NAME);
		INotficationHandler iDPSNotficationHandler = rdbmsEventManagerAlpha.getNotificationHandlers().get(DPS_NAME);

		Assert.assertEquals(iDVSNotficationHandler.totalActiveNodes(),1);
		Assert.assertEquals(iDPSNotficationHandler.totalActiveNodes(),1);
		
		Assert.assertNotNull(((MockServiceNode) (iDVSNotficationHandler.getList().get(0))).getHost());
		Assert.assertEquals(8080,((MockServiceNode) (iDVSNotficationHandler.getList().get(0))).getPort());
		Assert.assertEquals(8090,((MockServiceNode) (iDPSNotficationHandler.getList().get(0))).getPort());
		
//		Assert.assertEquals(8080,iDVSNotficationHandler.getList().get(0).getPort());
//		Assert.assertNotNull(iDPSNotficationHandler.getList().get(0).getHost());
//		Assert.assertEquals(8090,iDPSNotficationHandler.getList().get(0).getPort());

		System.setProperty(CLIENT_PORT, "8020");
		
		ZKEventManager dvsEventManagerBeta = ZKEventManager.getInstance(
				MockEventHandlerBetaActionHandler.class,DVS_NAME_TWO, dvsPath+"/dvsInstanceTwo",null,null);
		Assert.assertNotNull(dvsEventManagerBeta);

		dvsEventManagerBeta.start();
		Thread.sleep(10000);
		Assert.assertEquals(iDVSNotficationHandler.totalActiveNodes(),2);

		Assert.assertEquals(8020,((MockServiceNode) (iDVSNotficationHandler.getList().get(1))).getPort());

//		Assert.assertNotNull(iDVSNotficationHandler.getList().get(1).getHost());
//		Assert.assertNotNull(iDPSNotficationHandler.getList().get(0).getHost());
		
		Assert.assertNotNull(((MockServiceNode) (iDVSNotficationHandler.getList().get(1))).getHost());
		Assert.assertNotNull(((MockServiceNode) (iDPSNotficationHandler.getList().get(0))).getHost());


		System.setProperty(CLIENT_PORT, "8030");

		ZKEventManager dpsEventManagerBeta = ZKEventManager.getInstance(
				MockEventHandlerBetaActionHandler.class,DPS_NAME_TWO, dpsPath+"/dpsInstanceTwo",null,null);
		Assert.assertNotNull(dpsEventManagerBeta);
		dpsEventManagerBeta.start();
		Thread.sleep(10000);
		Assert.assertEquals(iDPSNotficationHandler.totalActiveNodes(),2);
		Assert.assertEquals(8030,((MockServiceNode) (iDPSNotficationHandler.getList().get(1))).getPort());

		dvsEventManagerAlpha.stop();
		Thread.sleep(10000);
		//
		Assert.assertEquals(iDVSNotficationHandler.totalActiveNodes(),1);
		Assert.assertEquals(iDPSNotficationHandler.totalActiveNodes(),2);

		dvsEventManagerAlpha.start();
		Thread.sleep(10000);

		Assert.assertEquals(iDVSNotficationHandler.totalActiveNodes(),2);
		Assert.assertEquals(iDPSNotficationHandler.totalActiveNodes(),2);

		dpsEventManagerAlpha.stop();
		Thread.sleep(10000);

		Assert.assertEquals(iDVSNotficationHandler.totalActiveNodes(),2);
		Assert.assertEquals(iDPSNotficationHandler.totalActiveNodes(),1);
		
		dvsEventManagerAlpha.stop();
		dvsEventManagerBeta.stop();
		dpsEventManagerBeta.stop();
		Thread.sleep(10000);

		Assert.assertEquals(iDVSNotficationHandler.totalActiveNodes(),0);
		Assert.assertEquals(iDPSNotficationHandler.totalActiveNodes(),0);
		
		logger.info("testTreeCacheListner stop the environment");
	}

	@Test (priority=1)
	public void testTreeCacheListnerUsingServletContext() throws InterruptedException,
	IOException, KeeperException, InvalidTargetObjectTypeException {
		// Set up the context...

		// ******************* RDBMS Adapter Start ********************* // 
		MockServletContext rdbmsServletContext = new MockServletContext("");
		ServletContextEvent rdbmsServletEvent = null;
		ZKEventHandlerContextListener rdbmsContextListener = null;
		rdbmsServletContext.addInitParameter(ZKEventManagerConstants.ZKS_CLIENT_NAME, RDBMS_NAME);
		rdbmsServletContext.addInitParameter(ZKEventManagerConstants.ZKS_CLIENT_PATH, rdbmsPath);
		rdbmsServletContext.addInitParameter(ZKEventManagerConstants.ZKS_CLIENT_LISTENER_PATH, listnerPath);

		rdbmsServletContext.addInitParameter(ZKEventManagerConstants.ZKS_CLIENT_CLASS,
				sysClientName);
		rdbmsServletContext.addInitParameter(ZKEventManagerConstants.ZKS_NOTIFICATION_CLIENT_CLASS+DASH+DVS_NAME,
				mockDVSClientName);
		rdbmsServletContext.addInitParameter(ZKEventManagerConstants.ZKS_NOTIFICATION_CLIENT_NAME+DASH+DVS_NAME,
				DVS_NAME);
		rdbmsServletContext.addInitParameter(ZKEventManagerConstants.ZKS_NOTIFICATION_CLIENT_CLASS+DASH+DPS_NAME,
				mockDPSClientName);
		rdbmsServletContext.addInitParameter(ZKEventManagerConstants.ZKS_NOTIFICATION_CLIENT_NAME+DASH+DPS_NAME,
				DPS_NAME);

		rdbmsContextListener = new ZKEventHandlerContextListener();
		rdbmsServletEvent = new ServletContextEvent(rdbmsServletContext);
		rdbmsContextListener.contextInitialized(rdbmsServletEvent);	
		ZKEventManager rdbmszkEventManager = (ZKEventManager) rdbmsServletEvent
				.getServletContext().getAttribute(
						ZKEventManagerConstants.ZKEVENT_MANAGER_KEY);
		Assert.assertNotNull(rdbmszkEventManager);
		Assert.assertTrue(rdbmszkEventManager.isActive());
		Assert.assertNotNull(rdbmszkEventManager.getNotificationHandlers().get(DVS_NAME));
		Assert.assertNotNull(rdbmszkEventManager.getNotificationHandlers().get(DPS_NAME));

		INotficationHandler iDVSNotficationHandler = rdbmszkEventManager.getNotificationHandlers().get(DVS_NAME);
		INotficationHandler iDPSNotficationHandler = rdbmszkEventManager.getNotificationHandlers().get(DPS_NAME);

		Thread.sleep(5000);

		// ******************* DVS Service Start ********************* // 
		MockServletContext dvsServletContext = new MockServletContext("");
		ServletContextEvent dvsServletEvent = null;
		ZKEventHandlerContextListener dvsContextListener = null;
		dvsServletContext.addInitParameter(ZKEventManagerConstants.ZKS_CLIENT_NAME, DVS_NAME);
		dvsServletContext.addInitParameter(ZKEventManagerConstants.ZKS_CLIENT_PATH, dvsPath);
		dvsServletContext.addInitParameter(ZKEventManagerConstants.ZKS_CLIENT_SERVICE_DISCOVERY, Boolean.TRUE.toString());

		dvsServletContext.addInitParameter(ZKEventManagerConstants.ZKS_CLIENT_CLASS,
				sysClientName);
		dvsContextListener = new ZKEventHandlerContextListener();
		dvsServletEvent = new ServletContextEvent(dvsServletContext);
		dvsContextListener.contextInitialized(dvsServletEvent);	
		ZKEventManager dvszkEventManager = (ZKEventManager) dvsServletEvent
				.getServletContext().getAttribute(
						ZKEventManagerConstants.ZKEVENT_MANAGER_KEY);
		Assert.assertNotNull(dvszkEventManager);
		Assert.assertTrue(dvszkEventManager.isActive());

		Thread.sleep(5000);	
		// ******************* DVS Service End ********************* // 

		// ******************* DPS Service Start ********************* // 
		MockServletContext dpsServletContext = new MockServletContext("");
		ServletContextEvent dpsServletEvent = null;
		ZKEventHandlerContextListener dpsContextListener = null;
		dpsServletContext.addInitParameter(ZKEventManagerConstants.ZKS_CLIENT_NAME, DPS_NAME);
		dpsServletContext.addInitParameter(ZKEventManagerConstants.ZKS_CLIENT_PATH, dpsPath);
		dpsServletContext.addInitParameter(ZKEventManagerConstants.ZKS_CLIENT_CLASS,
				sysClientName);
		dvsServletContext.addInitParameter(ZKEventManagerConstants.ZKS_CLIENT_SERVICE_DISCOVERY, Boolean.TRUE.toString());
		
		dpsContextListener = new ZKEventHandlerContextListener();
		dpsServletEvent = new ServletContextEvent(dpsServletContext);
		dpsContextListener.contextInitialized(dpsServletEvent);	
		ZKEventManager dpszkEventManager = (ZKEventManager) dpsServletEvent
				.getServletContext().getAttribute(
						ZKEventManagerConstants.ZKEVENT_MANAGER_KEY);
		Assert.assertNotNull(dpszkEventManager);
		Assert.assertTrue(dpszkEventManager.isActive());
		Thread.sleep(5000);			
		// ******************* DPS Service Stop ********************* // 

		Assert.assertEquals(iDPSNotficationHandler.totalActiveNodes(),1);
		Assert.assertEquals(iDVSNotficationHandler.totalActiveNodes(),1);

		dpszkEventManager.stop();
		dvszkEventManager.stop();
		Thread.sleep(10000);
		Assert.assertEquals(iDPSNotficationHandler.totalActiveNodes(),0);
		Assert.assertEquals(iDVSNotficationHandler.totalActiveNodes(),0);
		rdbmszkEventManager.stop();
		Thread.sleep(10000);
	}

	@Test (priority=2)
	public void testTreeCacheListnerHandlerTestUsingServletContext() throws InterruptedException,
	IOException, KeeperException, InvalidTargetObjectTypeException {
		logger.info("testTreeCacheListnerHandlerTestUsingServletContext the environment");

		// Set up the context...
		sc = new MockServletContext("");
		sc.addInitParameter(ZKEventManagerConstants.ZKS_CLIENT_NAME, RDBMS_NAME);
		sc.addInitParameter(ZKEventManagerConstants.ZKS_CLIENT_PATH, rdbmsPath);
		sc.addInitParameter(ZKEventManagerConstants.ZKS_CLIENT_CLASS,
				sysClientName);
		sc.addInitParameter(ZKEventManagerConstants.ZKS_NOTIFICATION_CLIENT_CLASS+DASH+DVS_NAME,
				mockDVSClientName);
		sc.addInitParameter(ZKEventManagerConstants.ZKS_NOTIFICATION_CLIENT_NAME+DASH+DVS_NAME,
				DVS_NAME);
		sc.addInitParameter(ZKEventManagerConstants.ZKS_NOTIFICATION_CLIENT_CLASS+DASH+DPS_NAME,
				mockDPSClientName);
		sc.addInitParameter(ZKEventManagerConstants.ZKS_NOTIFICATION_CLIENT_NAME+DASH+DPS_NAME,
				DPS_NAME);		
		listener = new ZKEventHandlerContextListener();
		event = new ServletContextEvent(sc);
		listener.contextInitialized(event);	
		ZKEventManager zkEventManager = (ZKEventManager) event
				.getServletContext().getAttribute(
						ZKEventManagerConstants.ZKEVENT_MANAGER_KEY);
		Assert.assertNotNull(zkEventManager);
		Assert.assertTrue(zkEventManager.isActive());
		Assert.assertNotNull(zkEventManager.getNotificationHandlers().get(DVS_NAME));
		Assert.assertNotNull(zkEventManager.getNotificationHandlers().get(DPS_NAME));
		//zkEventManager.stop();
		Thread.sleep(10000);
		logger.info("testTreeCacheListnerHandlerTestUsingServletContext the environment");

	}
	@SuppressWarnings("unchecked")
	public HashMap<String,Class<INotficationHandler>>   buildListners() throws ClassNotFoundException{
		HashMap<String,Class<INotficationHandler>> handlers = new HashMap<String,Class<INotficationHandler>> ();
		Class<INotficationHandler> notificationClientClazz = (Class<INotficationHandler>) Class.forName(mockDVSClientName);
		handlers.put(DVS_NAME,notificationClientClazz);

		notificationClientClazz = (Class<INotficationHandler>) Class.forName(mockDPSClientName);
		handlers.put(DPS_NAME,notificationClientClazz);

		return handlers;

	}
}
