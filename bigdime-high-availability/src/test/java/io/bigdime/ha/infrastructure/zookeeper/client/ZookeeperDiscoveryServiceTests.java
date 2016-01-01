/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.ha.infrastructure.zookeeper.client;

import io.bigdime.ha.event.manager.Action;
import io.bigdime.ha.infrastructure.zookeeper.server.ZKTestServer;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.x.discovery.ServiceCache;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.ServiceProvider;
import org.apache.curator.x.discovery.details.ServiceCacheListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.ITestContext;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;
/**
 * 
 * @author mnamburi
 *
 */
public class ZookeeperDiscoveryServiceTests extends ZKClientBaseTest {
	private Logger logger = LoggerFactory.getLogger(ZookeeperDiscoveryServiceTests.class);

	private static final String        ADAPTER_INSTANCE_NAME_01 = "adapter-instance-01";
	private static final String        ADAPTER_INSTANCE_NAME_02 = "adapter-instance-02";
	private static final String        ADAPTER_INSTANCE_NAME_03 = "adapter-instance-03";
	private static final String dataPath = "/bigdime03/test/discovery-service";

	private ServiceInstance<String> serviceInstance;



	@Test (priority=0)
	public void testDiscoveryService101() throws Exception{
		List<Closeable>     closeables = Lists.newArrayList();
		CuratorFramework client = CuratorFrameworkFactory.newClient(DEFAULT_HOST, new RetryOneTime(1));
		final CountDownLatch latch =  new CountDownLatch(2);
		ServiceDiscovery<String>      discoveryService101  = null;
		ServiceDiscovery<String>      discoveryService102 = null;
		try {
			client.start();
			closeables.add(client);

			// ******************* Register Services to ZK Discovery Service
			discoveryService101 = ServiceDiscoveryBuilder.builder(String.class).client(client).basePath(dataPath).build();	
			discoveryService101.start();
			closeables.add(discoveryService101);
			registerServices(client,discoveryService101);

			Thread.sleep(1000);

			// ************** Validate Service Availability Changes.
			discoveryService102 = ServiceDiscoveryBuilder.builder(String.class).client(client).basePath(dataPath).build();	
			discoveryService102.start();
			closeables.add(discoveryService102);

			ServiceCacheListener        listener = new ServiceCacheListener()
			{
				@Override
				public void cacheChanged()
				{
					logger.info("Cache has changed !");
					latch.countDown();
				}

				@Override
				public void stateChanged(CuratorFramework client, ConnectionState newState)
				{
				}
			};

			Collection<ServiceInstance<String>> dvsInstancesList = discoveryService102.queryForInstances(ADAPTER_INSTANCE_NAME_01);
			for (Iterator<ServiceInstance<String>> iterator = dvsInstancesList.iterator(); iterator.hasNext();) {
				ServiceInstance<String> serviceInstance =  iterator.next();
				Assert.assertEquals(serviceInstance.getName(), ADAPTER_INSTANCE_NAME_01);				
			}

			ServiceCache<String> serviceCache = discoveryService102.serviceCacheBuilder().name(ADAPTER_INSTANCE_NAME_02).build();
			serviceCache.start();
			serviceCache.addListener(listener);

			//******** Update the host details and verify the Discovery Service.
			Collection<ServiceInstance<String>> rdbmsInstancesList = discoveryService101.queryForInstances(ADAPTER_INSTANCE_NAME_02);
			ServiceInstance<String> serviceInstance = null;
			ServiceInstance<String> serviceInstancev1 = null;
			Assert.assertEquals(serviceCache.getInstances().get(0).getPort().intValue(), 8080);
			for (Iterator<ServiceInstance<String>> iterator = rdbmsInstancesList.iterator(); iterator.hasNext();) {
				serviceInstance =  iterator.next();
				serviceInstancev1 = ServiceInstance.<String>builder().payload("Changed").name(ADAPTER_INSTANCE_NAME_02).port(8090).id(serviceInstance.getId()).build();
				discoveryService101.updateService(serviceInstancev1);
			}	
			Thread.sleep(1000);
			Assert.assertEquals(serviceCache.getInstances().get(0).getPort().intValue(), 8090);


			//******** Unregister Host from Discovery Service
			Assert.assertEquals(serviceCache.getInstances().size(), 1);
			rdbmsInstancesList = discoveryService101.queryForInstances(ADAPTER_INSTANCE_NAME_02);
			for (Iterator<ServiceInstance<String>> iterator = rdbmsInstancesList.iterator(); iterator.hasNext();) {
				serviceInstance =  iterator.next();
				Assert.assertEquals(serviceInstance.getName(), ADAPTER_INSTANCE_NAME_02);	
				discoveryService101.unregisterService(serviceInstance);
			}
			Thread.sleep(1000);
			Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));
			Assert.assertEquals(serviceCache.getInstances().size(), 0);


		} catch (Exception e) {
			logger.error("Error :\" e={}",e.getLocalizedMessage());
		}
		finally
		{
			if(discoveryService101 != null){
				discoveryService101.close();
			}
			if(discoveryService102 != null){
				discoveryService102.close();
			}			
		}
	}

	@Test (priority=1)
	public void discoveryDVSServiceTests() throws IOException{
		List<Closeable>     closeables = Lists.newArrayList();
		CuratorFramework client = CuratorFrameworkFactory.newClient(DEFAULT_HOST, new RetryOneTime(1));
		ServiceDiscovery<String>      discoveryService101  = null;
		ServiceDiscovery<String>      discoveryService102 = null;
		try {
			client.start();
			closeables.add(client);

			// ******************* Register Services to ZK Discovery Service
			discoveryService101 = ServiceDiscoveryBuilder.builder(String.class).client(client).basePath(dataPath).build();	
			discoveryService101.start();
			closeables.add(discoveryService101);

			Action action = Action.getInstance()
					.type("REGISTRATION")
					.message("DVS Host Registration")
					.source(ADAPTER_INSTANCE_NAME_01)
					.build();

			ServiceInstance<String> dvsInstanceOne = ServiceInstance.<String>builder()
					.name(ADAPTER_INSTANCE_NAME_01)
					.port(8070)
					.payload(action.toString())
					.build();
			ServiceInstance<String> dvsInstanceTwo = ServiceInstance.<String>builder()
					.name(ADAPTER_INSTANCE_NAME_01)
					.port(8080)
					.payload(action.toString())
					.build();
			ServiceInstance<String> dvsInstanceThree = ServiceInstance.<String>builder()
					.name(ADAPTER_INSTANCE_NAME_01)
					.port(8090)
					.payload(action.toString())
					.build();			
			discoveryService101.registerService(dvsInstanceOne);
			discoveryService101.registerService(dvsInstanceTwo);
			discoveryService101.registerService(dvsInstanceThree);
			Thread.sleep(1000);

			// ************** Validate Service Availability Changes.
			discoveryService102 = ServiceDiscoveryBuilder.builder(String.class).client(client).basePath(dataPath).build();	
			discoveryService102.start();
			closeables.add(discoveryService102);
			//******** The default is {@link RoundRobinStrategy}
			ServiceProvider<String> serviceProvider = discoveryService102.serviceProviderBuilder().serviceName(ADAPTER_INSTANCE_NAME_01).build();
			serviceProvider.start();

			logger.info("Port 1 "+serviceProvider.getInstance().getPort().intValue());
			logger.info("Port 2 "+serviceProvider.getInstance().getPort().intValue());
			logger.info("Port 3 "+serviceProvider.getInstance().getPort().intValue());

			logger.info("Port 4"+serviceProvider.getInstance().getPort().intValue());
			logger.info("Port 5"+serviceProvider.getInstance().getPort().intValue());
			logger.info("Port 6"+serviceProvider.getInstance().getPort().intValue());

			//			Assert.assertEquals(serviceProvider.getInstance().getPort().intValue(), 8070);
			//			Assert.assertEquals(serviceProvider.getInstance().getPort().intValue(), 8080);
			//			Assert.assertEquals(serviceProvider.getInstance().getPort().intValue(), 8090);
			//
			//			Assert.assertEquals(serviceProvider.getInstance().getPort().intValue(), 8070);
			//			Assert.assertEquals(serviceProvider.getInstance().getPort().intValue(), 8080);
			//			Assert.assertEquals(serviceProvider.getInstance().getPort().intValue(), 8090);
		} catch (Exception e) {
			logger.error("Error :\" e={}",e.getLocalizedMessage());
		}
		finally
		{
			//            Collections.reverse(closeables);
			//            for ( Closeable c : closeables )
			//            {
			//                CloseableUtils.closeQuietly(c);
			//            }
		}		
	}

	private final AtomicInteger   index = new AtomicInteger(0);

	public void printInstancesList(Collection<ServiceInstance<String>> instances){
		if(instances == null || instances.isEmpty()){
			logger.info("*******Services Instances not Available*********");
			return;
		}
		for (Iterator<ServiceInstance<String>> iterator = instances.iterator(); iterator
				.hasNext();) {
			ServiceInstance<String> serviceInstance = (ServiceInstance<String>) iterator.next();
			logger.info(
					"Service Instance Details :\" address={} id={} name={} port={}",
					serviceInstance.getAddress(), serviceInstance.getId() ,serviceInstance.getName(),serviceInstance.getPort());
		}
	}

	public void registerServices(CuratorFramework client,ServiceDiscovery<String> discovery) throws Exception{

		Action action = Action.getInstance()
				.type("REGISTRATION")
				.message("Host Registration")
				.source(ADAPTER_INSTANCE_NAME_01)
				.build();

		ServiceInstance<String> dvsInstanceOne = ServiceInstance.<String>builder()
				.name(ADAPTER_INSTANCE_NAME_01)
				.port(8080)
				.payload(action.toString())
				.build();
		ServiceInstance<String> dvsInstanceTwo = ServiceInstance.<String>builder()
				.name(ADAPTER_INSTANCE_NAME_01)
				.port(8090)
				.payload(action.toString())
				.build();

		action = Action.getInstance()
				.type("REGISTRATION")
				.message("Host Registration")
				.source(ADAPTER_INSTANCE_NAME_02)
				.build();

		ServiceInstance<String> rdbmsAdapterInstance = ServiceInstance.<String>builder()
				.name(ADAPTER_INSTANCE_NAME_02)
				.port(8080)
				.payload(action.toString())
				.build();
		action = Action.getInstance()
				.type("REGISTRATION")
				.message("Host Registration")
				.source(ADAPTER_INSTANCE_NAME_03)
				.build();

		ServiceInstance<String> fileAdapterInstance = ServiceInstance.<String>builder()
				.name(ADAPTER_INSTANCE_NAME_03)
				.port(8080)
				.payload(action.toString())
				.build();

		discovery.registerService(dvsInstanceOne);
		discovery.registerService(dvsInstanceTwo);
		discovery.registerService(rdbmsAdapterInstance);
		discovery.registerService(fileAdapterInstance);
	}
}
