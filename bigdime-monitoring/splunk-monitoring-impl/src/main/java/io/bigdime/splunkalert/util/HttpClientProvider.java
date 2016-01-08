/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.splunkalert.util;



import io.bigdime.alert.Logger;
import io.bigdime.alert.LoggerFactory;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

import javax.annotation.PostConstruct;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
/**
 * HttpClientProvider class is used to initialize CloseableHttpClient.
 * @author Sandeep Reddy,Murthy
 *
 */
@Component
//@Scope("prototype")
public class HttpClientProvider {
	
	private static final Logger logger = LoggerFactory.getLogger(HttpClientProvider.class);

	private CloseableHttpClient httpClient;
	private final String PROTOCOL = "SSL";
	/**
	 * init method run after the object is constructed an is used to instantiate a copy of CloseableHttpClient class
	 * @throws KeyManagementException
	 * @throws NoSuchAlgorithmException
	 */
	@PostConstruct
	public void init() throws KeyManagementException, NoSuchAlgorithmException {
		SSLContext sslContext = SSLContext.getInstance(PROTOCOL);

		sslContext.init(null, new TrustManager[] { new X509TrustManager() {

			@Override
			public void checkClientTrusted(X509Certificate[] chain,String authType) throws CertificateException {	
			}

			@Override
			public void checkServerTrusted(X509Certificate[] chain,String authType) throws CertificateException {						
			}

			@Override
			public java.security.cert.X509Certificate[] getAcceptedIssuers() {
				return null;
			}
		} }, new SecureRandom());
		httpClient = HttpClients.custom().setSSLSocketFactory(new SSLSocketFactory(sslContext, SSLConnectionSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER)).build();
	}
	/**
	 * 
	 * @return Returns a CloseableHttpClient object to caller.
	 */
	
	public  CloseableHttpClient getHttpClient() {
		return httpClient;
	}
	/**
	 * Close method is used to simply closes the stream .Any system resources tied up would also be released. 
	 * @throws IOException
	 */
	public void close() throws IOException {
		httpClient.close();
	}
	
}
