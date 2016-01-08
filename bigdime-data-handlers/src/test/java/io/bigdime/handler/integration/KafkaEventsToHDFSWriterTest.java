/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.handler.integration;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import io.bigdime.core.DataAdaptorException;
import io.bigdime.core.adaptor.DataAdaptor;
import io.bigdime.core.config.AdaptorConfigReader;
import io.bigdime.testng.BasicTest;

@Test(singleThreaded = true)
public class KafkaEventsToHDFSWriterTest extends BasicTest {

	int zookeeperAvailbePort = 0;
	int kafkaPort = 0;

	@Autowired
	AdaptorConfigReader adaptorConfigReader;

	@Autowired
	DataAdaptor dataAdaptor;

	@BeforeMethod
	public void setup() throws Exception {
		ReflectionTestUtils.setField(adaptorConfigReader, "CONFIG_FILE_LOCATION", "META-INF/adaptor-kafka-hdfs.json");
	}

	/**
	 * TODO remove dependency on KafkaReaderHandlerIntegrationTest-1
	 * @throws DataAdaptorException
	 * @throws InterruptedException
	 */
	@Test(groups = "KafkaEventsToHDFSWriterTest-1", singleThreaded = true, dependsOnGroups = "KafkaReaderHandlerIntegrationTest-1")
	public void testHandlerChain() throws DataAdaptorException, InterruptedException {
		dataAdaptor.start();
		// dataAdaptor.stop();
	}

	@AfterTest
	public void shutDown() throws DataAdaptorException {
		dataAdaptor.getAdaptorContext();
		dataAdaptor.stop();
	}

}
