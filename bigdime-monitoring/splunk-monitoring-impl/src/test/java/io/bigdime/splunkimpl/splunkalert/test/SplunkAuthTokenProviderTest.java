/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.splunkimpl.splunkalert.test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.Charset;

import io.bigdime.alert.Logger;
import io.bigdime.alert.LoggerFactory;
import io.bigdime.splunkalert.common.exception.AuthorizationException;
import io.bigdime.splunkalert.handler.SplunkAuthTokenProvider;
import io.bigdime.splunkalert.response.AlertBuilder;
import io.bigdime.splunkalert.util.HttpClientProvider;

import org.apache.commons.httpclient.HttpStatus;
import org.apache.http.HttpEntity;
import org.apache.http.StatusLine;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.CloseableHttpClient;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.modules.testng.PowerMockTestCase;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import static io.bigdime.splunkimpl.splunkalert.constants.TestResourceConstants.ENVIORNMENT;
import static io.bigdime.splunkimpl.splunkalert.constants.TestResourceConstants.ENVIRONMENT_VALUE;
import static io.bigdime.splunkimpl.splunkalert.constants.TestResourceConstants.SOURCE_TYPE;
/**
 * 
 * @author Sandeep Reddy,Murthy
 *
 */

public class SplunkAuthTokenProviderTest extends PowerMockTestCase{
	@Mock
	HttpClientProvider httpClientProvider;

//	@Autowired
	SplunkAuthTokenProvider splunkAuthTokenProvider;
//	@Autowired
	AlertBuilder alertBuilder;

	private static final Logger logger = LoggerFactory
			.getLogger(SplunkAuthTokenProviderTest.class);

	@BeforeTest
	public void setup() {
		logger.info(SOURCE_TYPE,"Test Phase","Setting the environment");
		System.setProperty(ENVIORNMENT, ENVIRONMENT_VALUE);
		splunkAuthTokenProvider=new SplunkAuthTokenProvider();
		alertBuilder=new AlertBuilder();
		

	}

	@Test
	public void getAuthTokenTest() throws ClientProtocolException, IOException,
			AuthorizationException, URISyntaxException {
		SplunkAuthTokenProvider splunkAuthTokenProvider = new SplunkAuthTokenProvider();
		HttpClientProvider httpClientProvider = Mockito
				.mock(HttpClientProvider.class);
		String authToken = null;
		CloseableHttpClient closeableHttpClient = Mockito
				.mock(CloseableHttpClient.class);
		Mockito.when(httpClientProvider.getHttpClient()).thenReturn(
				closeableHttpClient);
		CloseableHttpResponse closeableHttpResponse = Mockito
				.mock(CloseableHttpResponse.class);
		Mockito.when(closeableHttpClient.execute(Mockito.any(HttpPost.class)))
				.thenReturn(closeableHttpResponse);
		StatusLine statusLine = Mockito.mock(StatusLine.class);
		Mockito.when(closeableHttpResponse.getStatusLine()).thenReturn(
				statusLine);
		Mockito.when(statusLine.getStatusCode()).thenReturn(200);
		JsonNode jsonNode2 = Mockito.mock(JsonNode.class);
		ObjectMapper objectMapper = Mockito.mock(ObjectMapper.class);
		HttpEntity httpEntity = Mockito.mock(HttpEntity.class);
		Mockito.when(closeableHttpResponse.getEntity()).thenReturn(httpEntity);
		String json = "{\"sessionKey\":\"value\"}";
		ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(
				json.getBytes(Charset.defaultCharset()));
		Mockito.when(httpEntity.getContent()).thenReturn(byteArrayInputStream);
		Mockito.when(objectMapper.readTree(byteArrayInputStream)).thenReturn(
				jsonNode2);
		JsonNode jsonNode3 = Mockito.mock(JsonNode.class);
		Mockito.when(jsonNode2.get(Mockito.any(String.class))).thenReturn(
				jsonNode3);
		Mockito.when(jsonNode3.asText()).thenReturn("testString");

		ReflectionTestUtils.setField(splunkAuthTokenProvider,
				"httpClientProvider", httpClientProvider);
		ReflectionTestUtils.setField(splunkAuthTokenProvider, "authToken",
				authToken);
		Assert.assertNotNull(splunkAuthTokenProvider.getNewAuthToken());

	}

	@Test(expectedExceptions = { AuthorizationException.class })
	public void getAuthTokenNot200Test() throws ClientProtocolException,
			IOException, AuthorizationException, URISyntaxException {
		SplunkAuthTokenProvider splunkAuthTokenProvider = new SplunkAuthTokenProvider();
		HttpClientProvider httpClientProvider = Mockito
				.mock(HttpClientProvider.class);
		String authToken = null;
		CloseableHttpClient closeableHttpClient = Mockito
				.mock(CloseableHttpClient.class);
		Mockito.when(httpClientProvider.getHttpClient()).thenReturn(
				closeableHttpClient);
		CloseableHttpResponse closeableHttpResponse = Mockito
				.mock(CloseableHttpResponse.class);
		Mockito.when(closeableHttpClient.execute(Mockito.any(HttpPost.class)))
				.thenReturn(closeableHttpResponse);
		StatusLine statusLine = Mockito.mock(StatusLine.class);
		Mockito.when(closeableHttpResponse.getStatusLine()).thenReturn(
				statusLine);
		Mockito.when(statusLine.getStatusCode()).thenReturn(201);
		ReflectionTestUtils.setField(splunkAuthTokenProvider,
				"httpClientProvider", httpClientProvider);
		ReflectionTestUtils.setField(splunkAuthTokenProvider, "authToken",
				authToken);
		splunkAuthTokenProvider.getAuthToken();

	}
	
	@Test(expectedExceptions = { AuthorizationException.class })
	public void getNewAuthTokenAuthorizationExceptionforServiceUnavailabilityTest() throws AuthorizationException, IOException, URISyntaxException{
		SplunkAuthTokenProvider splunkAuthTokenProvider = new SplunkAuthTokenProvider();
		HttpClientProvider httpClientProvider = Mockito
				.mock(HttpClientProvider.class);
		ReflectionTestUtils.setField(splunkAuthTokenProvider,
				"httpClientProvider", httpClientProvider);
		ReflectionTestUtils.setField(splunkAuthTokenProvider,
				"SPLUNK_HOST", "test");
		ReflectionTestUtils.setField(splunkAuthTokenProvider,
				"SPLUNK_PORT", 8080);
		ReflectionTestUtils.setField(splunkAuthTokenProvider,
				"USERNAME_VALUE", "test");
		ReflectionTestUtils.setField(splunkAuthTokenProvider,
				"PASSWORD_VALUE", "test");
		ReflectionTestUtils.setField(splunkAuthTokenProvider, "authToken",
				"test");
		CloseableHttpClient closeableHttpClient=Mockito.mock(CloseableHttpClient.class);
		CloseableHttpResponse closeableHttpResponse=Mockito.mock(CloseableHttpResponse.class);
		Mockito.when(httpClientProvider.getHttpClient()).thenReturn(closeableHttpClient);
		Mockito.when(closeableHttpClient.execute((HttpUriRequest) Mockito.any())).thenReturn(closeableHttpResponse);
		StatusLine statusLine=Mockito.mock(StatusLine.class);
		Mockito.when(closeableHttpResponse.getStatusLine()).thenReturn(statusLine);
		Mockito.when(statusLine.getStatusCode()).thenReturn(HttpStatus.SC_SERVICE_UNAVAILABLE);		
		splunkAuthTokenProvider.getNewAuthToken();
	}
	
	@Test(expectedExceptions = { AuthorizationException.class })
	public void getNewAuthTokenAuthorizationExceptionforUnauthorizedTest() throws AuthorizationException, IOException, URISyntaxException{
		SplunkAuthTokenProvider splunkAuthTokenProvider = new SplunkAuthTokenProvider();
		HttpClientProvider httpClientProvider = Mockito
				.mock(HttpClientProvider.class);
		ReflectionTestUtils.setField(splunkAuthTokenProvider,
				"httpClientProvider", httpClientProvider);
		ReflectionTestUtils.setField(splunkAuthTokenProvider,
				"SPLUNK_HOST", "test");
		ReflectionTestUtils.setField(splunkAuthTokenProvider,
				"SPLUNK_PORT", 8080);
		ReflectionTestUtils.setField(splunkAuthTokenProvider,
				"USERNAME_VALUE", "test");
		ReflectionTestUtils.setField(splunkAuthTokenProvider,
				"PASSWORD_VALUE", "test");
		ReflectionTestUtils.setField(splunkAuthTokenProvider, "authToken",
				"test");
		CloseableHttpClient closeableHttpClient=Mockito.mock(CloseableHttpClient.class);
		CloseableHttpResponse closeableHttpResponse=Mockito.mock(CloseableHttpResponse.class);
		Mockito.when(httpClientProvider.getHttpClient()).thenReturn(closeableHttpClient);
		Mockito.when(closeableHttpClient.execute((HttpUriRequest) Mockito.any())).thenReturn(closeableHttpResponse);
		StatusLine statusLine=Mockito.mock(StatusLine.class);
		Mockito.when(closeableHttpResponse.getStatusLine()).thenReturn(statusLine);
		Mockito.when(statusLine.getStatusCode()).thenReturn(HttpStatus.SC_UNAUTHORIZED);		
		splunkAuthTokenProvider.getNewAuthToken();
	}
}
