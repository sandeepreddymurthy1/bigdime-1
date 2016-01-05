/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.splunkalert.handler;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.httpclient.HttpStatus;

import io.bigdime.alert.Logger;
import io.bigdime.alert.LoggerFactory;
import io.bigdime.splunkalert.common.exception.AuthorizationException;
import io.bigdime.splunkalert.util.HttpClientProvider;

import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import static io.bigdime.splunkalert.constants.SplunkMetadataConstants.SCHEME;
import static io.bigdime.splunkalert.constants.SplunkMetadataConstants.SPLUNK_TOKEN_PATH;
import static io.bigdime.splunkalert.constants.SplunkMetadataConstants.JSON_OUTPUT_MODE;
import static io.bigdime.splunkalert.constants.SplunkMetadataConstants.OUTPUT_MODE_KEY;
import static io.bigdime.splunkalert.constants.SplunkMetadataConstants.USERNAME;
import static io.bigdime.splunkalert.constants.SplunkMetadataConstants.PASSWORD;
import static io.bigdime.splunkalert.constants.SplunkMetadataConstants.SOURCE_NAME;

/**
 * SplunkAuthTokenProvider provides methods to get authorization key from splunk
 * 
 * @author Sandeep Reddy,Murthy
 * 
 */

@Component
public class SplunkAuthTokenProvider {
	private static final Logger logger = LoggerFactory
			.getLogger(SplunkAuthTokenProvider.class);

	@Autowired
	private HttpClientProvider httpClientProvider;
	@Value("${splunk.host}")
	private String SPLUNK_HOST;
	@Value("${splunk.port}")
	private int SPLUNK_PORT;
	@Value("${splunk.username}")
	private String USERNAME_VALUE;
	@Value("${splunk.password}")
	private String PASSWORD_VALUE;

	private String authToken;

	/**
	 * Gets an authorization token from splunk.If the authorization token is
	 * null, calls the new method, gets a new token and returns it to the
	 * caller.
	 * 
	 * @return An authentication token that can be used by the caller to connect
	 *         to splunk
	 * @throws AuthorizationException
	 * @throws IOException
	 * @throws URISyntaxException
	 * @deprecated this method would be deprecated from the final cut
	 */

	public String getAuthToken() throws AuthorizationException, IOException,
			URISyntaxException {
		if (authToken == null) {
			// synchronized (SplunkAuthTokenProvider.class) {
			// if (authToken == null) {
			getNewAuthToken();
			// }
			// }
		}
		return authToken;
	}

	/**
	 * Gets a new authorization token returns it to the caller.Saves it in the
	 * authToken field so that the next requester can use it
	 * 
	 * @return A brand new authentication token that can be used by the caller
	 *         to connect to splunk
	 * @throws AuthorizationException
	 * @throws IOException
	 * @throws URISyntaxException
	 */
	public String getNewAuthToken() throws AuthorizationException, IOException,
			URISyntaxException {
		logger.info(SOURCE_NAME, "Authentication Phase",
				"Getting a new authorization token from splunk");
		CloseableHttpClient httpclient = httpClientProvider.getHttpClient();
		URIBuilder uriBuilder = new URIBuilder();
		uriBuilder.setScheme(SCHEME).setHost(SPLUNK_HOST).setPort(SPLUNK_PORT)
				.setPath(SPLUNK_TOKEN_PATH)
				.addParameter(OUTPUT_MODE_KEY, JSON_OUTPUT_MODE);
		HttpPost request = new HttpPost(uriBuilder.build());
		List<BasicNameValuePair> nvps = new ArrayList<BasicNameValuePair>();
		nvps.add(new BasicNameValuePair(USERNAME, USERNAME_VALUE));
		nvps.add(new BasicNameValuePair(PASSWORD, PASSWORD_VALUE));
		request.setEntity(new UrlEncodedFormEntity(nvps));
		try (CloseableHttpResponse response = httpclient.execute(request)) {
			if (response.getStatusLine().getStatusCode() == HttpStatus.SC_SERVICE_UNAVAILABLE) {
				throw new AuthorizationException(
						"Unable to get Authorization to splunk as the service is currently unavailable");
			} else if (response.getStatusLine().getStatusCode() == HttpStatus.SC_UNAUTHORIZED) {
				throw new AuthorizationException(
						"Unable to get Authorization to splunk as the user credentials are incorrect");
			} else if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
				throw new AuthorizationException(
						"Unable to get Authorization to splunk with exception code "+response.getStatusLine().getStatusCode());
			}
			ObjectMapper om = new ObjectMapper();
			JsonNode authroot = om.readTree(response.getEntity().getContent());
			authToken = authroot.get("sessionKey").asText();
			return authToken;
		}

	}

}
