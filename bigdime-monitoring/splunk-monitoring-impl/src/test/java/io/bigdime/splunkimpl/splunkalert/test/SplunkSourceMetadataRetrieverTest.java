/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.splunkimpl.splunkalert.test;


import static io.bigdime.splunkimpl.splunkalert.constants.TestResourceConstants.ENVIORNMENT;
import static io.bigdime.splunkimpl.splunkalert.constants.TestResourceConstants.SOURCE_TYPE;
import io.bigdime.alert.Alert;
import io.bigdime.alert.AlertException;
import io.bigdime.alert.AlertServiceRequest;
import io.bigdime.alert.AlertServiceResponse;
import io.bigdime.alert.Logger;
import io.bigdime.alert.LoggerFactory;
import io.bigdime.alert.ManagedAlertService;
import io.bigdime.alert.ManagedAlertService.ALERT_STATUS;
import io.bigdime.splunkalert.common.exception.AuthorizationException;
import io.bigdime.splunkalert.handler.SplunkAuthTokenProvider;
import io.bigdime.splunkalert.response.AlertBuilder;
import io.bigdime.splunkalert.retriever.SplunkSourceMetadataRetriever;
import io.bigdime.splunkalert.util.HttpClientProvider;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Properties;

import org.apache.commons.lang.NotImplementedException;
import org.apache.http.HttpEntity;
import org.apache.http.StatusLine;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.modules.testng.PowerMockTestCase;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
/**
 * 
 * @author Sandeep Reddy,Murthy
 *
 */

public class SplunkSourceMetadataRetrieverTest extends PowerMockTestCase {
	
	private static final Logger logger = LoggerFactory.getLogger(SplunkSourceMetadataRetrieverTest.class);

	HttpClientProvider httpClientProvider;
	@Value("${splunk.username}")
	private String USERNAME_VALUE;
	
	AlertBuilder alertBuilder;

	SplunkSourceMetadataRetriever splunkSourceMetadataRetriever;
	
	@BeforeTest
	public void setup() {
		logger.info(SOURCE_TYPE, "Test Phase", "Setting the environment");
		System.setProperty(ENVIORNMENT, "test");
		 httpClientProvider=new HttpClientProvider();
		 alertBuilder=new AlertBuilder();
		 splunkSourceMetadataRetriever=new SplunkSourceMetadataRetriever();
		
	}
	

	 @Test
	   public void getSourceMetadata200Test() throws ClientProtocolException,AuthorizationException,Exception {
		   httpClientProvider = Mockito.mock(HttpClientProvider.class);
		   CloseableHttpClient closeableHttpClient  = Mockito.mock(CloseableHttpClient.class);
		   
		   CloseableHttpResponse closeableHttpResponse = Mockito.mock(CloseableHttpResponse.class);
		   Mockito.when(httpClientProvider.getHttpClient()).thenReturn(closeableHttpClient);
		   Mockito.when(closeableHttpClient.execute(Mockito.any(HttpUriRequest.class))).thenReturn(closeableHttpResponse);
		   StatusLine statusLine = Mockito.mock(StatusLine.class); 
		   HttpEntity httpEntity = Mockito.mock(HttpEntity.class);
		   Mockito.when(closeableHttpResponse.getEntity()).thenReturn(httpEntity);
		   String json = "[{ \"_raw\": \"Raw Test Data\", \"host\": \"host name\",\"sourcetype\": \"source type\",\"tag::eventtype\": \"normal\",\"_time\": \"2015-10-08T19:59:59.045+00:00\"}]";
		   Mockito.when(closeableHttpResponse.getStatusLine()).thenReturn(statusLine);
		   Mockito.when(statusLine.getStatusCode()).thenReturn(200).thenReturn(200);
		   Mockito.when(closeableHttpResponse.getEntity().getContent()).thenReturn(new ByteArrayInputStream(json.getBytes(Charset.defaultCharset()))).thenReturn(new ByteArrayInputStream(json.getBytes(Charset.defaultCharset())));
           SplunkAuthTokenProvider splunkAuthTokenProvider = Mockito.mock(SplunkAuthTokenProvider.class);
		   Properties properties =new Properties();
		   Mockito.when(splunkAuthTokenProvider.getNewAuthToken()).thenReturn("authtoken");
		   ReflectionTestUtils.setField(splunkSourceMetadataRetriever, "httpClientProvider", httpClientProvider);
		   ReflectionTestUtils.setField(splunkSourceMetadataRetriever, "splunkAuthTokenProvider", splunkAuthTokenProvider);
		   Assert.assertNotNull(splunkSourceMetadataRetriever.getSourceMetadata("unit",properties));
		   	
	   }
	 
   
   @Test
   public void getSourceMetadata401Test() throws ClientProtocolException,AuthorizationException,Exception{
	   httpClientProvider = Mockito.mock(HttpClientProvider.class);
	   CloseableHttpClient closeableHttpClient  = Mockito.mock(CloseableHttpClient.class);	   
	   CloseableHttpResponse closeableHttpResponse = Mockito.mock(CloseableHttpResponse.class);
	   Mockito.when(httpClientProvider.getHttpClient()).thenReturn(closeableHttpClient);
	   Mockito.when(closeableHttpClient.execute(Mockito.any(HttpUriRequest.class))).thenReturn(closeableHttpResponse);
	   StatusLine statusLine = Mockito.mock(StatusLine.class); 
	   HttpEntity httpEntity = Mockito.mock(HttpEntity.class);
	   Mockito.when(closeableHttpResponse.getEntity()).thenReturn(httpEntity);
	   String json = "[{ \"_raw\": \"Raw Test Data\", \"host\": \"host name\",\"sourcetype\": \"source type\",\"tag::eventtype\": \"normal\",\"_time\": \"2015-10-08T19:59:59.045+00:00\"}]";
	   Mockito.when(closeableHttpResponse.getStatusLine()).thenReturn(statusLine);
	   Mockito.when(statusLine.getStatusCode()).thenReturn(401).thenReturn(200);
	   Mockito.when(closeableHttpResponse.getEntity().getContent()).thenReturn(new ByteArrayInputStream(json.getBytes(Charset.defaultCharset()))).thenReturn(new ByteArrayInputStream(json.getBytes(Charset.defaultCharset())));
       SplunkAuthTokenProvider splunkAuthTokenProvider = Mockito.mock(SplunkAuthTokenProvider.class);
	   Mockito.when(splunkAuthTokenProvider.getNewAuthToken()).thenReturn("authtoken");
	   Properties properties=new Properties();
	   ReflectionTestUtils.setField(splunkSourceMetadataRetriever, "httpClientProvider", httpClientProvider);
	   ReflectionTestUtils.setField(splunkSourceMetadataRetriever, "splunkAuthTokenProvider", splunkAuthTokenProvider);
	   Assert.assertNotNull(splunkSourceMetadataRetriever.getSourceMetadata("unit",properties));
	   	
   }
   
   
   @Test
   public void getSourceMetadataNot200Test() throws ClientProtocolException, IOException, AuthorizationException {
	   httpClientProvider = Mockito.mock(HttpClientProvider.class);
	   CloseableHttpClient closeableHttpClient  = Mockito.mock(CloseableHttpClient.class);
	   try{
	   CloseableHttpResponse closeableHttpResponse = Mockito.mock(CloseableHttpResponse.class);
	   Mockito.when(httpClientProvider.getHttpClient()).thenReturn(closeableHttpClient);
	   Mockito.when(closeableHttpClient.execute(Mockito.any(HttpUriRequest.class))).thenReturn(closeableHttpResponse);
	   StatusLine statusLine = Mockito.mock(StatusLine.class); 
	   HttpEntity httpEntity = Mockito.mock(HttpEntity.class);
	   Mockito.when(closeableHttpResponse.getEntity()).thenReturn(httpEntity);
	   String json = "[{ \"_raw\": \"Raw Test Data\", \"host\": \"host name\",\"sourcetype\": \"source type\",\"tag::eventtype\": \"normal\",\"_time\": \"2015-10-08T19:59:59.045+00:00\"}]";
	   Mockito.when(closeableHttpResponse.getStatusLine()).thenReturn(statusLine);
	   Mockito.when(statusLine.getStatusCode()).thenReturn(401).thenReturn(201).thenReturn(201);
	   Mockito.when(closeableHttpResponse.getEntity().getContent()).thenReturn(new ByteArrayInputStream(json.getBytes(Charset.defaultCharset()))).thenReturn(new ByteArrayInputStream(json.getBytes(Charset.defaultCharset())));
       SplunkAuthTokenProvider splunkAuthTokenProvider = Mockito.mock(SplunkAuthTokenProvider.class);
       Properties properties=new Properties();
	   Mockito.when(splunkAuthTokenProvider.getNewAuthToken()).thenReturn("authtoken");
	   ReflectionTestUtils.setField(splunkSourceMetadataRetriever, "httpClientProvider", httpClientProvider);
	   ReflectionTestUtils.setField(splunkSourceMetadataRetriever, "splunkAuthTokenProvider", splunkAuthTokenProvider);
	   splunkSourceMetadataRetriever.getSourceMetadata("unit",properties);
	   }
	   catch(AuthorizationException e){
		Assert.assertEquals(e.getClass(),AuthorizationException.class);
	   }
	   catch(Exception e){
		   Assert.assertEquals(e.getClass(), Exception.class);
		   
	   }
	    	
   }
   
   @Test
   public void getAlertsTest() throws AlertException{
	   AlertServiceRequest alertServiceRequest=Mockito.mock(AlertServiceRequest.class);
	   ManagedAlertService managedAlertService=Mockito.mock(ManagedAlertService.class);
	   AlertServiceResponse alertServiceResponse=Mockito.mock(AlertServiceResponse.class);
	   Mockito.when(managedAlertService.getAlerts(alertServiceRequest)).thenReturn(alertServiceResponse);
	   ReflectionTestUtils.setField(splunkSourceMetadataRetriever, "managedAlertService", managedAlertService);   
	   splunkSourceMetadataRetriever.getAlerts(alertServiceRequest);
   }
   
   @Test
   public void getAlertsExceptionTest() throws AlertException{
	   AlertServiceRequest alertServiceRequest=Mockito.mock(AlertServiceRequest.class);
	   ManagedAlertService managedAlertService=Mockito.mock(ManagedAlertService.class);
	   AlertServiceResponse alertServiceResponse=Mockito.mock(AlertServiceResponse.class);
	   Mockito.when(managedAlertService.getAlerts(alertServiceRequest)).thenThrow(AlertException.class);
	   ReflectionTestUtils.setField(splunkSourceMetadataRetriever, "managedAlertService", managedAlertService);
	  try{
		  splunkSourceMetadataRetriever.getAlerts(alertServiceRequest);
	  }catch(Exception e){
		Assert.assertEquals(e.getClass().getSimpleName(),"AlertException");
	  }
	  
   }
   
   
   @Test
   public void updateAlertTest() throws JsonGenerationException, JsonMappingException, NotImplementedException, AlertException, IOException{
	   Alert alertMessage=Mockito.mock(Alert.class);
	   ManagedAlertService managedAlertService =Mockito.mock(ManagedAlertService.class); 
	   Mockito.when(managedAlertService.updateAlert((Alert) Mockito.any(), (ALERT_STATUS) Mockito.any(),Mockito.any(String.class))).thenReturn(true);
	   ReflectionTestUtils.setField(splunkSourceMetadataRetriever, "managedAlertService", managedAlertService);
	   splunkSourceMetadataRetriever.updateAlert(alertMessage, ALERT_STATUS.ACKNOWLEDGED, "test");
       
   }
   @Test
   public void updateAlertExceptionTest() throws JsonGenerationException, JsonMappingException, NotImplementedException, AlertException, IOException{
	   Alert alertMessage=Mockito.mock(Alert.class);
	   ManagedAlertService managedAlertService =Mockito.mock(ManagedAlertService.class); 
	   Mockito.when(managedAlertService.updateAlert((Alert) Mockito.any(), (ALERT_STATUS) Mockito.any(),Mockito.any(String.class))).thenThrow(AlertException.class);
	   ReflectionTestUtils.setField(splunkSourceMetadataRetriever, "managedAlertService", managedAlertService);
	   splunkSourceMetadataRetriever.updateAlert(alertMessage, ALERT_STATUS.ACKNOWLEDGED, "test");
   }
}
