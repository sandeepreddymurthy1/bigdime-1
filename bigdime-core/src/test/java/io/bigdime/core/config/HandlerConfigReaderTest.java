/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core.config;

import java.io.IOException;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.Test;

import io.bigdime.core.AdaptorConfigurationException;
import io.bigdime.core.commons.JsonHelper;

@ContextConfiguration(classes = { JsonHelper.class, HandlerConfigReader.class })
public class HandlerConfigReaderTest extends AbstractTestNGSpringContextTests {
	@Autowired
	HandlerConfigReader handlerConfigReader;

	@Test
	public void testInstance() {
		Assert.assertNotNull(handlerConfigReader);
	}

	@Test
	public void testReadHandler() throws AdaptorConfigurationException, JsonProcessingException, IOException {
		String jsonString = "{\"name\": \"memory-channel-reader\", \"description\": \"read data from channels\", \"handler-class\": \"io.bigdime.core.handler.MemoryChannelInputHandler\", \"properties\" : {\"name\" : \"value\"}}";
		ObjectMapper mapper = new ObjectMapper();
		JsonNode actualObj = mapper.readTree(jsonString);
		HandlerConfig handlerConfig = handlerConfigReader.readHandlerConfig(actualObj);
		Assert.assertNotNull(handlerConfig.getHandlerProperties());
		Assert.assertEquals(handlerConfig.getHandlerProperties().size(), 1, "size of the properties map should be one");
	}

	/**
	 * Assert that if the adaptor configuration does not properties node,
	 * handler will be configured with an empty map.
	 * 
	 * @throws AdaptorConfigurationException
	 * @throws JsonProcessingException
	 * @throws IOException
	 */
	@Test
	public void testReadHandlerWithNoProperties()
			throws AdaptorConfigurationException, JsonProcessingException, IOException {
		String jsonString = "{\"name\": \"memory-channel-reader\", \"description\": \"read data from channels\", \"handler-class\": \"io.bigdime.core.handler.MemoryChannelInputHandler\"}";
		ObjectMapper mapper = new ObjectMapper();
		JsonNode actualObj = mapper.readTree(jsonString);
		HandlerConfig handlerConfig = handlerConfigReader.readHandlerConfig(actualObj);
		Assert.assertNotNull(handlerConfig.getHandlerProperties());
		Assert.assertTrue(handlerConfig.getHandlerProperties().isEmpty(),
				"properties should be empty since the properties node does not exist in the configuration");
	}
}
