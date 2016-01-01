/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.splunkalert.constants;
/**
 * SplunkMetadataConstants encapuslates all the static constants required by the application
 * @author Sandeep Reddy,Murthy
 *
 */
public class SplunkMetadataConstants {
//	schema keys
	public static final String ALERT_LIST = "alert list";
	public static final String ALERT_NAME = "alertName";
	public static final String PATH = "path";
	
//	combined sources keys
	public static final String SCHEMA = "schema";
	public static final String ALERT_CONFIG = "alert-config";
	public static final String SPECIFIC_ALERTS = "specific-alerts";
	
//	uri constants
	public static final String SCHEME = "https";		
	public static final String OUTPUT_MODE_KEY = "output_mode";
	public static final String JSON_OUTPUT_MODE = "json";
	public static final String COUNT_KEY = "count";
	public static final String COUNT_VALUE = "0";
	
//Additional Params from Authorization	
	public static final String AUTHORIZATION_HEADER_KEY="Authorization";
	public static final String AUTHORIZATION_HEADER_VALUE_PREFIX = "Splunk ";
	
	public static final String SPLUNK_TOKEN_PATH="/servicesNS/admin/search/auth/login/";
	public static final String SPLUNK_FIRED_ALERTS_PATH="BigDataPlatform/alerts/fired_alerts";
	public static final String BIGDATAPLATFORM="BigDataPlatform";
	public static final String ALERTS="alerts";
	public static final String FIRED_ALERTS="fired_alerts";
	public static final String SERVICENS="servicesNS";
	public static final String ENTRY="entry";
	public static final String LINKS="links";
	public static final String JOB="job";
	public static final String RESULTS="results";

	public static final String USERNAME="username";
	public static final String PASSWORD="password";
	
	private static final String LOGIN_AUTH_PATH = "/servicesNS/admin/search/auth/login/";
	
	public static final String SOURCE_NAME="SPLUNK-MONITORING-IMPL";
	//HTTP codes
	public static final int FOUR_ZERO_ONE=401;
	public static final int TWO_ZERO_ZERO=200;
	public static final int TWO_ZERO_ONE=201;
	



}
