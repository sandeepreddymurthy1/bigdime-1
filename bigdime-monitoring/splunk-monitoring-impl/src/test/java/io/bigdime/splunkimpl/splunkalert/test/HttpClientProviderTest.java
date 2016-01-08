/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.splunkimpl.splunkalert.test;

import static io.bigdime.splunkimpl.splunkalert.constants.TestResourceConstants.ENVIORNMENT;
import static io.bigdime.splunkimpl.splunkalert.constants.TestResourceConstants.ENVIRONMENT_VALUE;
import static io.bigdime.splunkimpl.splunkalert.constants.TestResourceConstants.SOURCE_TYPE;
import io.bigdime.hbase.client.DataInsertionSpecification;
import io.bigdime.splunkalert.handler.SplunkAuthTokenProvider;
import io.bigdime.splunkalert.response.AlertBuilder;
import io.bigdime.splunkalert.util.HttpClientProvider;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;

import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;

import org.apache.http.conn.socket.LayeredConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.testng.PowerMockTestCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
/**
 * 
 * @author samurthy
 *
 */
//@ContextConfiguration (locations = { "classpath:application-context.xml" })
//public class HttpClientProviderTest extends AbstractTestNGSpringContextTests{
@PrepareForTest({SSLContext.class,HttpClients.class,SSLConnectionSocketFactory.class})
public class HttpClientProviderTest extends PowerMockTestCase{
	
//	@Autowired
	HttpClientProvider httpClientProvider;
	
//	@Autowired
	SplunkAuthTokenProvider splunkAuthTokenProvider;
//	@Autowired
	AlertBuilder alertBuilder;
	
	private static final Logger logger = LoggerFactory.getLogger(HttpClientProviderTest.class);
	
//	@BeforeTest
	public void setup() {
		logger.info(SOURCE_TYPE,"Test Phase","Setting the environment");
		System.setProperty(ENVIORNMENT, ENVIRONMENT_VALUE);
		httpClientProvider=new HttpClientProvider();
		splunkAuthTokenProvider=new SplunkAuthTokenProvider();
		alertBuilder=new AlertBuilder();
		
	}
//	
//	@Test
//	public void initTest() throws Exception{
////		HttpClientProvider httpClientProvider=new HttpClientProvider();
////		SSLContext sslContext=SSLContext.getInstance("SSL");
//		PowerMockito.mockStatic(SSLContext.class);
//		SSLContext sslContext=PowerMockito.mock(SSLContext.class);
//		PowerMockito.when(SSLContext.getInstance(Mockito.anyString())).thenReturn(sslContext);
//		PowerMockito.doNothing().when(sslContext).init((KeyManager[])Mockito.any(), (TrustManager[])Mockito.any(), (SecureRandom)Mockito.any());
//		PowerMockito.mockStatic(HttpClients.class);
//		HttpClientBuilder  httpClientBuilder=Mockito.mock(HttpClientBuilder.class);
//		PowerMockito.when(HttpClients.custom()).thenReturn(httpClientBuilder);
//		PowerMockito.mockStatic(SSLConnectionSocketFactory.class);
//		SSLSocketFactory sSLSocketFactory=Mockito.mock(SSLSocketFactory.class);
////		PowerMockito.whenNew(SSLSocketFactory.class).withArguments(sslContext,sSLSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER).thenReturn(sSLSocketFactory);
//		Mockito.when(httpClientBuilder.setSSLSocketFactory(sSLSocketFactory)).thenReturn(httpClientBuilder);
//		CloseableHttpClient httpClient=Mockito.mock(CloseableHttpClient.class);
//		Mockito.when(httpClientBuilder.build()).thenReturn(httpClient);
//		httpClientProvider.init();
//		
//	}
//	
//	@Test
//	public void getHttpClientTest(){
////		CloseableHttpClient closeableHttpClient=Mockito.mock(CloseableHttpClient.class);
//		Assert.assertNotNull(httpClientProvider.getHttpClient());		
//	}
//	
//	@Test
//	public void closeTest() throws IOException{
//		httpClientProvider.close();
//	}
}
