/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.handler.json;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import io.bigdime.core.ActionEvent;
import io.bigdime.core.ActionEvent.Status;
import io.bigdime.core.AdaptorConfigurationException;
import io.bigdime.core.DataChannel;
import io.bigdime.core.HandlerException;
import io.bigdime.core.commons.JsonHelper;
import io.bigdime.core.commons.MapDescriptorParser;
import io.bigdime.core.config.AdaptorConfigConstants;
import io.bigdime.core.handler.HandlerContext;
import io.bigdime.core.handler.HandlerJournal;
import io.bigdime.core.handler.SimpleJournal;
import io.bigdime.core.runtimeinfo.RuntimeInfo;
import io.bigdime.core.runtimeinfo.RuntimeInfoStore;
import io.bigdime.handler.constants.KafkaReaderHandlerConstants;


public class JsonMapperHandlerTest {


	public Map<String, Object> mockProperties() throws JsonProcessingException, IOException{
		Map<String, Object> propertyMap = new HashMap<>();
		MapDescriptorParser descriptorParser = new MapDescriptorParser();
		String jsonString = "{\"unit-input\" : {\"entity-name\" : \"unit-entity-name-value\",\"topic\" : \"topic1\",\"partition\" : \"1\", \"unit-separator\" : \"unit-separator-value\"}}";

		ObjectMapper mapper = new ObjectMapper();
		JsonNode actualObj = mapper.readTree(jsonString);
		Map<String, Object> properties = new JsonHelper().getNodeTree(actualObj);
		Map<Object, String> srcDEscEntryMap = descriptorParser.parseDescriptor("unit-input", properties.get("unit-input"));


		propertyMap.put(AdaptorConfigConstants.SourceConfigConstants.SRC_DESC,
				srcDEscEntryMap.entrySet().iterator().next());
		propertyMap.put(KafkaReaderHandlerConstants.BROKERS, "unit-test-ip-1:9999,unit-test-ip-2:8888");
		propertyMap.put(KafkaReaderHandlerConstants.OFFSET_DATA_DIR, "/tmp");
		return propertyMap;
	}
	@Test
	public void testBuild() throws HandlerException, AdaptorConfigurationException, JsonProcessingException, IOException {
		
		JsonMapperHandler handler = new JsonMapperHandler();
		RuntimeInfoStore<RuntimeInfo> runtimeInfoStore = Mockito.mock(RuntimeInfoStore.class);
		ReflectionTestUtils.setField(handler, "runtimeInfoStore", runtimeInfoStore);
		handler.setPropertyMap(mockProperties());
		handler.build();
		Assert.assertEquals(handler.getInputDescriptor().getTopic(), "topic1");
		Assert.assertEquals(handler.getInputDescriptor().getPartition(), 1);
		Assert.assertEquals(handler.getInputDescriptor().getEntityName(), "unit-entity-name-value");

	}

	private JsonMapperHandler buildjsonMapperHandler() throws AdaptorConfigurationException, JsonProcessingException, IOException {
		JsonMapperHandler jsonMapperHandler = new JsonMapperHandler();
		jsonMapperHandler.setPropertyMap(mockProperties());
		RuntimeInfoStore<RuntimeInfo> runtimeInfoStore = Mockito.mock(RuntimeInfoStore.class);
		ReflectionTestUtils.setField(jsonMapperHandler, "runtimeInfoStore", runtimeInfoStore);
		
		jsonMapperHandler.build();
		return jsonMapperHandler;
	}

	@Test
	public void testProcess() throws AdaptorConfigurationException, IOException, HandlerException {
		JsonMapperHandler jsonMapperHandler = buildjsonMapperHandler();
		byte[] jsonMessage = buildSampleJsonMessage();
		ActionEvent actionEvent = new ActionEvent();
		actionEvent.setBody(jsonMessage);
		HandlerContext handlerContext = HandlerContext.get();
		handlerContext.createSingleItemEventList(actionEvent);
		jsonMapperHandler.process();
		HandlerJournal journal = (HandlerJournal) handlerContext.getJournal(jsonMapperHandler.getId());
		Assert.assertEquals(journal.getTotalRead(), 0);
		Assert.assertEquals(journal.getTotalSize(), 0);
	}

	@Test
	public void testProcessWithChannel() throws AdaptorConfigurationException, IOException, HandlerException {
		JsonMapperHandler jsonMapperHandler = buildjsonMapperHandler();
		byte[] jsonMessage = buildSampleJsonMessage();
		DataChannel dataChannel = Mockito.mock(DataChannel.class);
		Mockito.doNothing().when(dataChannel).put(Mockito.any(ActionEvent.class));
		ReflectionTestUtils.setField(jsonMapperHandler, "outputChannel", dataChannel);
		ActionEvent actionEvent = new ActionEvent();
		actionEvent.setBody(jsonMessage);
		HandlerContext handlerContext = HandlerContext.get();
		handlerContext.createSingleItemEventList(actionEvent);
		jsonMapperHandler.process();
		HandlerJournal journal = (HandlerJournal) handlerContext.getJournal(jsonMapperHandler.getId());
		Assert.assertEquals(journal.getTotalRead(), 0);
		Assert.assertEquals(journal.getTotalSize(), 0);
		Mockito.verify(dataChannel, Mockito.times(1)).put(actionEvent);
	}

	private byte[] buildSampleJsonMessage() throws IOException {
		String msg = "{\"name\": \"John Doe1\", \"favorite_number\": {\"int\": 421}, \"favorite_color\":{\"string\": \"Blue\"}}";
		JsonNode m = buildJsonMessage(msg);
		byte[] jsonMessage = m.toString().getBytes();
		return jsonMessage;
	}


	public static JsonNode buildJsonMessage(String str) {
		ObjectMapper mapper = new ObjectMapper();
		JsonNode actualObj = null;
		try {
			actualObj = mapper.readTree(str);
		} catch (IOException e) {
			Assert.fail(e.getMessage());
		}
		return actualObj;
	}

	/**
	 * Assert that id the data is there in journal, it can be picked up and if
	 * more than one records are there, CALLBACK is returned.
	 * 
	 * @throws HandlerException
	 * @throws IOException
	 * @throws AdaptorConfigurationException 
	 */
	@Test
	public void testWithDataInJournal() throws HandlerException, IOException, AdaptorConfigurationException {
		JsonMapperHandler jsonMapperHandler = buildjsonMapperHandler();
		HandlerJournal journal = new SimpleJournal();
		journal.setTotalRead(1);
		journal.setTotalSize(10);
		HandlerContext.get().setJournal(jsonMapperHandler.getId(), journal);

		List<ActionEvent> actionEvents = new ArrayList<>();
		ActionEvent actionEvent = Mockito.mock(ActionEvent.class);
		Mockito.when(actionEvent.getBody()).thenReturn(buildSampleJsonMessage());
		actionEvents.add(actionEvent);
		actionEvent = Mockito.mock(ActionEvent.class);
		Mockito.when(actionEvent.getBody()).thenReturn(buildSampleJsonMessage());
		actionEvents.add(Mockito.mock(ActionEvent.class));
		journal.setEventList(actionEvents);
		Status status = jsonMapperHandler.process();
		Assert.assertSame(status, Status.CALLBACK);
	}

}
