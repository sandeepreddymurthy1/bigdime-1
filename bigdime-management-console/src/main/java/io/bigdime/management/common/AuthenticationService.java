/**
 * Copyright (C) 2015 Stubhub.
 */
/**
 * Provides user authentication services
 * @author Sandeep Reddy,Murthy
 *
 */
package io.bigdime.management.common;

import io.bigdime.alert.Logger;
import io.bigdime.alert.LoggerFactory;

import java.io.IOException;
import java.net.URLEncoder;
import java.util.Hashtable;
import java.text.MessageFormat;

import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.apache.http.Header;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicHeader;
import org.apache.http.params.HttpConnectionParams;

import static io.bigdime.management.common.ApplicationConstants.UTF_FORMAT;

@Component
public class AuthenticationService {
	static final long serialVersionUID = 1L;
	
	private static final Logger logger = LoggerFactory.getLogger(AuthenticationService.class);	
	private static final Integer TIMEOUT = new Integer(30000);
	
//	private static final String aquirePattern = "grant_type=password&username={0}&password={1}&scope=PRODUCTION";

	@Value("${ldap.platform.accountName}")
	private String serviceAccount;

	@Value("${ldap.platform.account.pw}")
	private String serviceAccountPW;

	@Value("${ldap.url}")
	private String ldapURL;
	
	@Value("${ldap.BASE}")
	private String base;
	
	@Value("${app.key}")
	private String base64ConsumerKeySecret;
	
	@Value("${login.endpoint}")
	private String endPoint;
	
	@Value("${login.aquirePattern}")
	private String aquirePattern;
	
	
	
	/**
	 * 
	 * @param authenticateUser
	 * @param password
	 * @return
	 */

	public AuthenticateUser authenticate(AuthenticateUser authenticateUser) {
		DirContext ctx;
		try {
			ctx = getContext(serviceAccount, serviceAccountPW, true);
			SearchControls sc = new SearchControls();
			sc.setSearchScope(SearchControls.SUBTREE_SCOPE);
			NamingEnumeration<SearchResult> results = ctx.search(
					base,
					"(&(objectClass=*)(sAMAccountName="
							+ authenticateUser.getUserId() + "))", sc);
			while (results.hasMore()) {
				SearchResult result = results.next();
				String dn = result.getName() + ",";
				try {
					DirContext context = getContext(dn,
							authenticateUser.getPassword(), false);
					NamingEnumeration<SearchResult> results1 = context.search(
							base,
							"(&(objectClass=*)(sAMAccountName="
									+ authenticateUser.getUserId() + "))", sc);
					if (results1.hasMore()) {
						SearchResult searchResults = results1.next();

						Attributes attributes = searchResults.getAttributes();
						Attribute attr = attributes.get("cn");
						String dispName = (String) attr.get();
						authenticateUser.setDisplayName(dispName);
					}

					context.close();
					authenticateUser.setLoginStatus(true);

				} catch (javax.naming.NamingException e) {
					logger.warn("BIGDIME-MANAGEMENT-CONSOLE",
							"Error occured calling LDAP", e.getMessage());
					ctx.close();
				}
			}
			ctx.close();
		} catch (NamingException e) {
			logger.warn("BIGDIME-MANAGEMENT-CONSOLE",
					"Error occured calling LDAP", e.getMessage());
		}
		return authenticateUser;
	}

	/**
	 * 
	 * @param dn
	 * @param password
	 * @param serviceAcc
	 * @return
	 * @throws NamingException
	 */
	public DirContext getContext(String dn, String password, boolean serviceAcc)
			throws NamingException {
		Hashtable<String, String> env = new Hashtable<String, String>();
		env.put(Context.INITIAL_CONTEXT_FACTORY,
				"com.sun.jndi.ldap.LdapCtxFactory");
		env.put(Context.PROVIDER_URL, ldapURL);
		if (serviceAcc) {
			env.put(Context.SECURITY_PRINCIPAL, "CN=" + serviceAccount
					+ ",OU=ServiceAccts,"+base);
		} else {
			env.put(Context.SECURITY_PRINCIPAL, dn + base);
		}
		env.put(Context.REFERRAL, "follow");
		env.put(Context.SECURITY_CREDENTIALS, password);
		env.put(Context.SECURITY_AUTHENTICATION, "simple");
		DirContext initial = new InitialDirContext(env);
		return initial;
	}
	
//	 private Header[] buildHeaders() {
//		String auth = base64ConsumerKeySecret;
//		Header headers[] = new Header[2];
//		headers[0] = new BasicHeader(HttpHeaders.AUTHORIZATION, "Bearer " + auth);
//		headers[1] = new BasicHeader(HttpHeaders.CONTENT_TYPE, "application/x-www-form-urlencoded; charset=UTF-8");
//		return headers;
//		}
//	 
//	 
//	
//	public String getUserAccessToken(String userName, String password) {
//	Header headers[] = buildHeaders();
//	String url = endPoint;
//	String accessToken = null;
//	 try {
//	if(userName.toLowerCase().indexOf("stubcorp/") < 0 ) {
//	   userName = "stubcorp/" + userName;
//	 			}
//	 			String payload = MessageFormat.format(aquirePattern, URLEncoder.encode(userName, UTF_FORMAT), URLEncoder.encode(password, UTF_FORMAT));
//	 			SimpleHttpResponse httpResponse = httpClient.postToUrlWithHeadersCorrect(url, payload, headers, TIMEOUT, false/* only200*/);
//	 			int status = httpResponse.getStatusCode();
//	 			String responseContent = httpResponse.getContent();
//	 			 if(status == HttpStatus.SC_OK) {
//                      //Authorized User
//	 			}
//	 		} catch (Exception e) {
//	 			e.printStackTrace();
//	 		}
//	 		return accessToken;
//	 
//	 	}
//	
//	public final static SimpleHttpResponse postToUrlWithHeadersNew(final String fullUrl, final String content, Header[] headers, final Integer timeout,
//			                                                    final boolean only200, final boolean newCookieStore) throws IOException {
//			       final HttpPost method = new HttpPost(fullUrl);
//			        final StringEntity entity = new StringEntity(content, "UTF-8");
//			        method.setEntity(entity);
//			        // use headers
//			        for (int i = 0; i < headers.length; i++)
//			        	method.addHeader(headers[i]);
//			        if (timeout != null) {
//			            // set timeout for receiving data (connection established)
//			            HttpConnectionParams.setSoTimeout(method.getParams(), timeout);
//			        }// else use default settings HttpClient4UtilInitializer
//			        return executeMethod(method, only200, newCookieStore);
//			    }
}
