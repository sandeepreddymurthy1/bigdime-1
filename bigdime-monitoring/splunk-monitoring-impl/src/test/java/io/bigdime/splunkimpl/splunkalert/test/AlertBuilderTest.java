/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.splunkimpl.splunkalert.test;

import static io.bigdime.splunkimpl.splunkalert.constants.TestResourceConstants.ENVIORNMENT;
import static io.bigdime.splunkimpl.splunkalert.constants.TestResourceConstants.ENVIRONMENT_VALUE;
import static io.bigdime.splunkimpl.splunkalert.constants.TestResourceConstants.TEST_STRING;
import static io.bigdime.splunkimpl.splunkalert.constants.TestResourceConstants.ERROR;
import static io.bigdime.splunkimpl.splunkalert.constants.TestResourceConstants.FATAL;
import static io.bigdime.splunkimpl.splunkalert.constants.TestResourceConstants.DATEASSTRING;
import static io.bigdime.splunkimpl.splunkalert.constants.TestResourceConstants.EVENT_TYPE;
import static io.bigdime.splunkimpl.splunkalert.constants.TestResourceConstants.SOURCE_TYPE;

import java.io.IOException;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import io.bigdime.adaptor.metadata.MetadataAccessException;
import io.bigdime.adaptor.metadata.MetadataStore;
import io.bigdime.alert.AlertException;
import io.bigdime.alert.Logger;
import io.bigdime.alert.Logger.ALERT_SEVERITY;
import io.bigdime.alert.Logger.ALERT_TYPE;
import io.bigdime.alert.LoggerFactory;
import io.bigdime.alert.ManagedAlert;
import io.bigdime.alert.ManagedAlertService;
import io.bigdime.alert.Logger.ALERT_CAUSE;
import io.bigdime.alert.ManagedAlertService.ALERT_STATUS;
import io.bigdime.splunkalert.SplunkAlert;
import io.bigdime.splunkalert.common.exception.AuthorizationException;
import io.bigdime.splunkalert.handler.SplunkAuthTokenProvider;
import io.bigdime.splunkalert.response.AlertBuilder;
import io.bigdime.splunkalert.retriever.SplunkSourceMetadataRetriever;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.ObjectMapper;

import static org.mockito.Matchers.*;

import org.mockito.Mockito;
import org.powermock.modules.testng.PowerMockTestCase;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.ExpectedExceptions;
import org.testng.annotations.Test;

/**
 * 
 * @author Sandeep Reddy,Murthy
 * 
 */
public class AlertBuilderTest extends PowerMockTestCase {
	private static final Logger logger = LoggerFactory
			.getLogger(AlertBuilderTest.class);
	AlertBuilder alertBuilder;

	@BeforeTest
	public void setup() {
		logger.info(SOURCE_TYPE, "Test Phase", "Setting the environment");
		System.setProperty(ENVIORNMENT, ENVIRONMENT_VALUE);
		alertBuilder = new AlertBuilder();
	}

	@Test
	public void getAlertsFromSplunkTest() throws Exception {
		AlertBuilder alertBuilder = new AlertBuilder();
		SplunkSourceMetadataRetriever mockSplunkSourceMetadataRetriever = Mockito
				.mock(SplunkSourceMetadataRetriever.class);
		SplunkAuthTokenProvider splunkAuthTokenProvider = Mockito
				.mock(SplunkAuthTokenProvider.class);
		ReflectionTestUtils.setField(alertBuilder,
				"splunkSourceMetadataRetriever",
				mockSplunkSourceMetadataRetriever);
		ReflectionTestUtils.setField(alertBuilder, "splunkAuthTokenProvider",
				splunkAuthTokenProvider);
		Mockito.when(splunkAuthTokenProvider.getNewAuthToken()).thenReturn(
				TEST_STRING);

		Properties mockProperties = Mockito.mock(Properties.class);
		SplunkSourceMetadataRetriever mockSplunkSourceMetadataRetriever2 = Mockito
				.mock(SplunkSourceMetadataRetriever.class);
		JsonNode mockJsonNode = Mockito.mock(JsonNode.class);
		Mockito.when(
				mockSplunkSourceMetadataRetriever.getSourceMetadata(
						anyString(), (Properties) any())).thenReturn(
				mockJsonNode);

		JsonNode mockJsonNode1 = Mockito.mock(JsonNode.class);
		JsonNode mockJsonNode2 = Mockito.mock(JsonNode.class);
		Iterator<JsonNode> json1Iterator = Mockito.mock(Iterator.class);
		Mockito.when(mockJsonNode.get(anyString())).thenReturn(mockJsonNode1);
		Mockito.when(mockJsonNode1.iterator()).thenReturn(json1Iterator);
		Mockito.when(json1Iterator.hasNext()).thenReturn(true, false);
		Mockito.when(json1Iterator.next()).thenReturn(mockJsonNode2);

		JsonNode mockJsonNode3 = Mockito.mock(JsonNode.class);
		JsonNode mockJsonNode4 = Mockito.mock(JsonNode.class);
		Mockito.when(mockJsonNode2.get(anyString())).thenReturn(mockJsonNode3);
		Mockito.when(mockJsonNode3.get(anyString())).thenReturn(mockJsonNode4);
		Mockito.when(mockJsonNode4.asText()).thenReturn(TEST_STRING);

		List<String> jobPathsStrings = Mockito.mock(List.class);
		Iterator<String> jobPathsStringsIterator = Mockito.mock(Iterator.class);
		Mockito.when(jobPathsStrings.iterator()).thenReturn(
				jobPathsStringsIterator);
		Mockito.when(jobPathsStringsIterator.hasNext()).thenReturn(true, false);
		Mockito.when(jobPathsStringsIterator.next()).thenReturn(TEST_STRING);

		SplunkSourceMetadataRetriever mockSplunkSourceMetadataRetriever3 = Mockito
				.mock(SplunkSourceMetadataRetriever.class);
		JsonNode mockJsonNode5 = Mockito.mock(JsonNode.class);
		Mockito.when(
				mockSplunkSourceMetadataRetriever2.getSourceMetadata(
						anyString(), (Properties) any())).thenReturn(
				mockJsonNode5);

		JsonNode mockJsonNode7 = Mockito.mock(JsonNode.class);
		Iterator<JsonNode> mockJsonNode6Iterator = Mockito.mock(Iterator.class);
		JsonNode mockJsonNode6 = Mockito.mock(JsonNode.class);
		Mockito.when(mockJsonNode5.get(anyString())).thenReturn(mockJsonNode6);
		Mockito.when(mockJsonNode6.iterator())
				.thenReturn(mockJsonNode6Iterator);
		Mockito.when(mockJsonNode6Iterator.hasNext()).thenReturn(true, false);
		Mockito.when(mockJsonNode6Iterator.next()).thenReturn(mockJsonNode7);
		Mockito.when(mockJsonNode1.size()).thenReturn(1);

		JsonNode mockJsonNode8 = Mockito.mock(JsonNode.class);
		Mockito.when(mockJsonNode1.get(0)).thenReturn(mockJsonNode1);
		JsonNode mockJsonNode9 = Mockito.mock(JsonNode.class);
		Mockito.when(mockJsonNode1.get(anyString())).thenReturn(mockJsonNode6);

		Mockito.when(mockJsonNode6.getTextValue()).thenReturn(TEST_STRING);

		Mockito.when(mockJsonNode1.getTextValue()).thenReturn(ERROR);
		Assert.assertNotNull(alertBuilder.getAlertsFromSplunk(TEST_STRING));

	}

	@Test(expectedExceptions = { Exception.class })
	public void getAlertsFromSplunkMetadataIOExceptionTest() throws Exception {
		AlertBuilder alertBuilder = new AlertBuilder();
		SplunkSourceMetadataRetriever mockSplunkSourceMetadataRetriever = Mockito
				.mock(SplunkSourceMetadataRetriever.class);
		SplunkAuthTokenProvider splunkAuthTokenProvider = Mockito
				.mock(SplunkAuthTokenProvider.class);
		ReflectionTestUtils.setField(alertBuilder,
				"splunkSourceMetadataRetriever",
				mockSplunkSourceMetadataRetriever);
		ReflectionTestUtils.setField(alertBuilder, "splunkAuthTokenProvider",
				splunkAuthTokenProvider);
		Mockito.when(splunkAuthTokenProvider.getNewAuthToken()).thenThrow(
				new IOException());
		Properties mockProperties = Mockito.mock(Properties.class);
		SplunkSourceMetadataRetriever mockSplunkSourceMetadataRetriever2 = Mockito
				.mock(SplunkSourceMetadataRetriever.class);
		JsonNode mockJsonNode = Mockito.mock(JsonNode.class);
		Mockito.when(
				mockSplunkSourceMetadataRetriever.getSourceMetadata(
						anyString(), (Properties) any())).thenReturn(
				mockJsonNode);

		JsonNode mockJsonNode1 = Mockito.mock(JsonNode.class);
		JsonNode mockJsonNode2 = Mockito.mock(JsonNode.class);
		Iterator<JsonNode> json1Iterator = Mockito.mock(Iterator.class);
		Mockito.when(mockJsonNode.get(anyString())).thenReturn(mockJsonNode1);
		Mockito.when(mockJsonNode1.iterator()).thenReturn(json1Iterator);
		Mockito.when(json1Iterator.hasNext()).thenReturn(true, false);
		Mockito.when(json1Iterator.next()).thenReturn(mockJsonNode2);

		JsonNode mockJsonNode3 = Mockito.mock(JsonNode.class);
		JsonNode mockJsonNode4 = Mockito.mock(JsonNode.class);
		Mockito.when(mockJsonNode2.get(anyString())).thenReturn(mockJsonNode3);
		Mockito.when(mockJsonNode3.get(anyString())).thenReturn(mockJsonNode4);
		Mockito.when(mockJsonNode4.asText()).thenReturn(TEST_STRING);

		List<String> jobPathsStrings = Mockito.mock(List.class);
		Iterator<String> jobPathsStringsIterator = Mockito.mock(Iterator.class);
		Mockito.when(jobPathsStrings.iterator()).thenReturn(
				jobPathsStringsIterator);
		Mockito.when(jobPathsStringsIterator.hasNext()).thenReturn(true, false);
		Mockito.when(jobPathsStringsIterator.next()).thenReturn(TEST_STRING);

		SplunkSourceMetadataRetriever mockSplunkSourceMetadataRetriever3 = Mockito
				.mock(SplunkSourceMetadataRetriever.class);
		JsonNode mockJsonNode5 = Mockito.mock(JsonNode.class);
		Mockito.when(
				mockSplunkSourceMetadataRetriever2.getSourceMetadata(
						anyString(), (Properties) any())).thenReturn(
				mockJsonNode5);

		JsonNode mockJsonNode7 = Mockito.mock(JsonNode.class);
		Iterator<JsonNode> mockJsonNode6Iterator = Mockito.mock(Iterator.class);
		JsonNode mockJsonNode6 = Mockito.mock(JsonNode.class);
		Mockito.when(mockJsonNode5.get(anyString())).thenReturn(mockJsonNode6);
		Mockito.when(mockJsonNode6.iterator())
				.thenReturn(mockJsonNode6Iterator);
		Mockito.when(mockJsonNode6Iterator.hasNext()).thenReturn(true, false);
		Mockito.when(mockJsonNode6Iterator.next()).thenReturn(mockJsonNode7);
		Mockito.when(mockJsonNode1.size()).thenReturn(1);

		JsonNode mockJsonNode8 = Mockito.mock(JsonNode.class);
		Mockito.when(mockJsonNode1.get(0)).thenReturn(mockJsonNode1);
		JsonNode mockJsonNode9 = Mockito.mock(JsonNode.class);
		Mockito.when(mockJsonNode1.get(anyString())).thenReturn(mockJsonNode6);

		Mockito.when(mockJsonNode6.getTextValue()).thenReturn(TEST_STRING);

		Mockito.when(mockJsonNode1.getTextValue()).thenReturn(ERROR);
		Assert.assertNotNull(alertBuilder.getAlertsFromSplunk(TEST_STRING));

	}

	@Test
	public void splunkAlertObjectBuilderERRORTest() {
		JsonNode jsonNode1 = Mockito.mock(JsonNode.class);
		Mockito.when(jsonNode1.get(anyInt())).thenReturn(jsonNode1);
		Mockito.when(jsonNode1.get(anyString())).thenReturn(jsonNode1);
		Mockito.when(jsonNode1.getTextValue()).thenReturn(TEST_STRING);
		Mockito.when(jsonNode1.get(anyInt()).get(EVENT_TYPE).getTextValue())
				.thenReturn(ERROR);
		Assert.assertNotNull(alertBuilder.splunkAlertObjectBuilder(jsonNode1));
	}

	@Test
	public void splunkAlertObjectBuilderNullTest() {
		JsonNode jsonNode1 = Mockito.mock(JsonNode.class);
		Mockito.when(jsonNode1.get(anyInt())).thenReturn(jsonNode1);
		Mockito.when(jsonNode1.get(anyString())).thenReturn(jsonNode1);
		Mockito.when(jsonNode1.getTextValue()).thenReturn(TEST_STRING);
		Mockito.when(jsonNode1.get(anyInt()).get(EVENT_TYPE)).thenReturn(null);
		Assert.assertNotNull(alertBuilder.splunkAlertObjectBuilder(jsonNode1));
	}

	@Test
	public void splunkAlertObjectBuilderFATALTest() {
		JsonNode jsonNode1 = Mockito.mock(JsonNode.class);
		Mockito.when(jsonNode1.get(anyInt())).thenReturn(jsonNode1);
		Mockito.when(jsonNode1.get(anyString())).thenReturn(jsonNode1);
		Mockito.when(jsonNode1.getTextValue()).thenReturn(TEST_STRING);
		Mockito.when(
				jsonNode1.get(anyInt()).get("tag::eventtype").getTextValue())
				.thenReturn(FATAL);
		Assert.assertNotNull(alertBuilder.splunkAlertObjectBuilder(jsonNode1));
	}

	@Test
	public void splunkAlertObjectBuilderTimeTest() throws ParseException {
		JsonNode jsonNode1 = Mockito.mock(JsonNode.class);
		Mockito.when(jsonNode1.get(anyInt())).thenReturn(jsonNode1);
		Mockito.when(jsonNode1.get(anyString())).thenReturn(jsonNode1);
		Mockito.when(jsonNode1.getTextValue()).thenReturn(DATEASSTRING);
		alertBuilder.splunkAlertObjectBuilder(jsonNode1);
		Assert.assertNotNull(alertBuilder.splunkAlertObjectBuilder(jsonNode1));
	}

	@Test
	public void insertAlertsIntoPersistantStoreTest() throws IOException,
			URISyntaxException, AlertException, MetadataAccessException {
		AlertBuilder alertBuilder = new AlertBuilder();
		MetadataStore metadataStore = Mockito.mock(MetadataStore.class);
		Set<String> alertSet = Mockito.mock(Set.class);
		Mockito.when(metadataStore.getDataSources()).thenReturn(alertSet);
		Iterator<String> setIterator = Mockito.mock(Iterator.class);
		Mockito.when(alertSet.iterator()).thenReturn(setIterator);
		Mockito.when(setIterator.hasNext()).thenReturn(true).thenReturn(false);
		Mockito.when(setIterator.next()).thenReturn("test");
		SplunkAuthTokenProvider splunkAuthTokenProvider = Mockito
				.mock(SplunkAuthTokenProvider.class);
		Mockito.when(splunkAuthTokenProvider.getNewAuthToken()).thenReturn(
				"test");
		JsonNode jsonNode = Mockito.mock(JsonNode.class);
		SplunkSourceMetadataRetriever splunkSourceMetadataRetriever = Mockito
				.mock(SplunkSourceMetadataRetriever.class);
		Mockito.when(
				splunkSourceMetadataRetriever.getSourceMetadata(
						Mockito.anyString(), (Properties) Mockito.any()))
				.thenReturn(jsonNode);
		Iterator<JsonNode> jsonNodeIterator = Mockito.mock(Iterator.class);
		Mockito.when(jsonNode.get(Mockito.anyString())).thenReturn(jsonNode);
		Mockito.when(jsonNode.iterator()).thenReturn(jsonNodeIterator);
		Mockito.when(jsonNodeIterator.hasNext()).thenReturn(true)
				.thenReturn(false);
		Mockito.when(jsonNodeIterator.next()).thenReturn(jsonNode);
		Mockito.when(jsonNode.get(Mockito.anyString())).thenReturn(jsonNode);
		List<String> jobPathsStrings = Mockito.mock(List.class);
		Iterator<String> listIterator = Mockito.mock(Iterator.class);
		Mockito.when(jobPathsStrings.iterator()).thenReturn(listIterator);
		Mockito.when(listIterator.hasNext()).thenReturn(true).thenReturn(false);
		Mockito.when(listIterator.next()).thenReturn("test");
		Mockito.when(jsonNode.size()).thenReturn(1).thenReturn(1);
		ManagedAlert managedAlert = Mockito.mock(ManagedAlert.class);
		Mockito.when(jsonNode.get(Mockito.anyInt())).thenReturn(jsonNode);
		AlertBuilder alertBuilderMock = Mockito.mock(AlertBuilder.class);
		Mockito.doNothing().when(alertBuilderMock)
				.updateAlert((List<ManagedAlert>) Mockito.any());
		// Mockito.when(jsonNode.get(Mockito.anyString())).thenReturn(jsonNode);
		// Mockito.when(alertBuilderMock.managedAlertObjectBuilder((JsonNode)
		// Mockito.any())).thenReturn(managedAlert);
		String jsonString = "{\"adaptor_name\":\"test\",\"message_context\":\"test\",\"alert_code\":\"BIG-0002\",\"alert_cause\":\"internal error\",\"alert_severity\":\"BLOCKER\",\"detail_message\":\"test\",\"_time\":\"1449205506541\",\"comment\":\"test\",\"alertStatus\":\"test\"}";
		ObjectMapper objectMapper = new ObjectMapper();
		JsonNode jobResult = objectMapper.readTree(jsonString);
		Mockito.when(jsonNode.get(Mockito.anyInt())).thenReturn(jobResult);

		ReflectionTestUtils.setField(alertBuilder, "metadataStore",
				metadataStore);
		ReflectionTestUtils.setField(alertBuilder,
				"splunkSourceMetadataRetriever", splunkSourceMetadataRetriever);
		ReflectionTestUtils.setField(alertBuilder, "splunkAuthTokenProvider",
				splunkAuthTokenProvider);
		ManagedAlertService managedAlertService = Mockito
				.mock(ManagedAlertService.class);
		ReflectionTestUtils.setField(alertBuilder, "managedAlertService",
				managedAlertService);
		Mockito.when(
				managedAlertService.updateAlert((ManagedAlert) Mockito.any(),
						(ALERT_STATUS) Mockito.any(), Mockito.anyString()))
				.thenReturn(true);
		alertBuilder.insertAlertsIntoPersistantStore();
	}

	@Test
	public void managedAlertObjectBuilderTest() throws JsonProcessingException,
			IOException {
		AlertBuilder alertBuilder = new AlertBuilder();
		ObjectMapper objectMapper = new ObjectMapper();
		String jsonString = "{\"adaptor_name\":\"test\",\"message_context\":\"test\",\"alert_code\":\"BIG-0002\",\"alert_cause\":\"internal error\",\"alert_severity\":\"BLOCKER\",\"detail_message\":\"test\",\"_time\":\"1449205506541\",\"comment\":\"test\",\"alertStatus\":\"test\"}";
		JsonNode jobResult = objectMapper.readTree(jsonString);
		ManagedAlert managedAlert = alertBuilder
				.managedAlertObjectBuilder(jobResult);
		Assert.assertTrue(managedAlert.getApplicationName().equals("test"));
		Assert.assertTrue(managedAlert.getCause().toString()
				.equals("APPLICATION_INTERNAL_ERROR"));
		Assert.assertTrue(managedAlert.getMessage().equals("test"));
		Assert.assertTrue(managedAlert.getMessageContext().equals("test"));
		Assert.assertTrue(managedAlert.getType().toString()
				.equals("INGESTION_FAILED"));
		Assert.assertTrue(managedAlert.getSeverity().toString()
				.equals("BLOCKER"));
	}

	@Test
	public void managedAlertObjectBuilderParseExceptionTest()
			throws JsonProcessingException, IOException {
		AlertBuilder alertBuilder = new AlertBuilder();
		ObjectMapper objectMapper = new ObjectMapper();
		String jsonString = "{\"adaptor_name\":\"test\",\"message_context\":\"test\",\"alert_code\":\"BIG-0002\",\"alert_cause\":\"internal error\",\"alert_severity\":\"BLOCKER\",\"detail_message\":\"test\",\"_time\":\"WRONG_DATE_FORMAT\",\"comment\":\"test\",\"alertStatus\":\"test\"}";
		JsonNode jobResult = objectMapper.readTree(jsonString);
		ManagedAlert managedAlert = alertBuilder
				.managedAlertObjectBuilder(jobResult);
		Assert.assertNotNull(managedAlert.getDateTime());
	}

	@Test 
	public void managedAlertObjectBuilderAlertCauseTest(){
		AlertBuilder alertBuilder = new AlertBuilder();
		JsonNode jobResult = Mockito.mock(JsonNode.class);
		Mockito.when(jobResult.get(Mockito.anyString())).thenReturn(jobResult);
		Mockito.when(jobResult.getTextValue()).thenReturn("test");
		List<String> listOfCauses=new ArrayList<String>();
		listOfCauses.add("adaptor configuration is invalid");
		listOfCauses.add("data validation error");
		listOfCauses.add("unsupported data or character type");
		listOfCauses.add("data could not be read from source");
		listOfCauses.add("internal error");
		listOfCauses.add("shutdown command received");
		listOfCauses.add("input message too big");
		listOfCauses.add("input data schema changed");
		for(String alertCauses:listOfCauses){
		Mockito.when(jobResult.get("alert_cause").getTextValue()).thenReturn(alertCauses);
		ManagedAlert managedAlert=alertBuilder.managedAlertObjectBuilder(jobResult);
		boolean alertCauseValidator=false;
		for(ALERT_CAUSE alertCause:ALERT_CAUSE.values()){
		if(alertCause.equals(managedAlert.getCause())){
			alertCauseValidator=true;
		     }
		  }
		Assert.assertTrue(alertCauseValidator);
	    }
	}
	
	@Test 
	public void managedAlertObjectBuilderAlertCodeTest(){
		AlertBuilder alertBuilder = new AlertBuilder();
		JsonNode jobResult = Mockito.mock(JsonNode.class);
		Mockito.when(jobResult.get(Mockito.anyString())).thenReturn(jobResult);
		Mockito.when(jobResult.getTextValue()).thenReturn("test");
		List<String> listOfalertCodes=new ArrayList<String>();
		listOfalertCodes.add("BIG-0001");
		listOfalertCodes.add("BIG-0002");
		listOfalertCodes.add("BIG-0003");
		listOfalertCodes.add("BIG-0004");
		listOfalertCodes.add("BIG-0005");
		listOfalertCodes.add("BIG-9999");		
		for(String alertCodes:listOfalertCodes){
		Mockito.when(jobResult.get("alert_code").getTextValue()).thenReturn(alertCodes);
		ManagedAlert managedAlert=alertBuilder.managedAlertObjectBuilder(jobResult);
		boolean alertCodeValidator=false;
		for(ALERT_TYPE alertCode:ALERT_TYPE.values()){
		if(alertCode.equals(managedAlert.getType())){
			alertCodeValidator=true;
		     }
		  }
		Assert.assertTrue(alertCodeValidator);
	    }
	}
	@Test 
	public void managedAlertObjectBuilderAlertSeverityTest(){
		AlertBuilder alertBuilder = new AlertBuilder();
		JsonNode jobResult = Mockito.mock(JsonNode.class);
		Mockito.when(jobResult.get(Mockito.anyString())).thenReturn(jobResult);
		Mockito.when(jobResult.getTextValue()).thenReturn("test");
		List<String> listOfalertSeverity=new ArrayList<String>();
		listOfalertSeverity.add("BLOCKER");
		listOfalertSeverity.add("MAJOR");
		listOfalertSeverity.add("NORMAL");		
		for(String alertsSeverity:listOfalertSeverity){
		Mockito.when(jobResult.get("alert_severity").getTextValue()).thenReturn(alertsSeverity);
		ManagedAlert managedAlert=alertBuilder.managedAlertObjectBuilder(jobResult);
		boolean alertSeverityValidator=false;
		for(ALERT_SEVERITY alertSeverity:ALERT_SEVERITY.values()){
		if(alertSeverity.equals(managedAlert.getSeverity())){
			alertSeverityValidator=true;
		     }
		  }
		Assert.assertTrue(alertSeverityValidator);
	    }
	}
	
	@Test
	public void managedAlertObjectBuilderDateTimeTest() throws ParseException{
		AlertBuilder alertBuilder = new AlertBuilder();
		JsonNode jobResult = Mockito.mock(JsonNode.class);
		Mockito.when(jobResult.get(Mockito.anyString())).thenReturn(jobResult);
		Mockito.when(jobResult.getTextValue()).thenReturn("test");
		Mockito.when(jobResult.get("_time").getTextValue()).thenReturn("2012-10-01T09:45:00.0000");
		ManagedAlert managedAlert=alertBuilder.managedAlertObjectBuilder(jobResult);
        Assert.assertEquals(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSS").parse("2012-10-01T09:45:00.0000").getTime(),managedAlert.getDateTime().getTime());
	}
	
	@Test
	public void splunkAlertObjectBuilderDateTimeTest() throws ParseException{
		AlertBuilder alertBuilder = new AlertBuilder();
		JsonNode jobResult = Mockito.mock(JsonNode.class);
		Mockito.when(jobResult.get(Mockito.anyInt())).thenReturn(jobResult);
		Mockito.when(jobResult.get(Mockito.anyString())).thenReturn(jobResult);
		Mockito.when(jobResult.getTextValue()).thenReturn("test");
		Mockito.when(jobResult.get("_time").getTextValue()).thenReturn("2012-10-01T09:45:00.0000");
		SplunkAlert splunkAlert=alertBuilder.splunkAlertObjectBuilder(jobResult);
        Assert.assertEquals(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSS").parse("2012-10-01T09:45:00.0000").getTime(),splunkAlert.getDateTime().getTime());
	}

	@Test(expectedExceptions = AlertException.class)
	public void insertAlertsIntoPersistantStoreAlertExceptionForEmptySetTest() throws AlertException, MetadataAccessException, IOException, URISyntaxException{
		AlertBuilder alertBuilder = new AlertBuilder();
		MetadataStore metadataStore=Mockito.mock(MetadataStore.class);
		Set<String> datasourceSet=new HashSet<String>();
		Mockito.when(metadataStore.getDataSources()).thenReturn(datasourceSet);
		ReflectionTestUtils.setField(alertBuilder, "metadataStore", metadataStore);
		alertBuilder.insertAlertsIntoPersistantStore();
	}
	
	@Test
	public void insertAlertsIntoPersistantStoreAlertExceptionTest() throws AlertException, MetadataAccessException, IOException, URISyntaxException{
		AlertBuilder alertBuilder = new AlertBuilder();
		MetadataStore metadataStore=Mockito.mock(MetadataStore.class);
		Set<String> datasourceSet=new HashSet<String>();
		datasourceSet.add("test");
		Mockito.when(metadataStore.getDataSources()).thenReturn(datasourceSet);
		SplunkAuthTokenProvider splunkAuthTokenProvider=Mockito.mock(SplunkAuthTokenProvider.class);
		ReflectionTestUtils.setField(alertBuilder, "metadataStore", metadataStore);
		ReflectionTestUtils.setField(alertBuilder, "splunkAuthTokenProvider", splunkAuthTokenProvider);
		Mockito.when(splunkAuthTokenProvider.getNewAuthToken()).thenThrow(AlertException.class);
		alertBuilder.insertAlertsIntoPersistantStore();
	}
}
