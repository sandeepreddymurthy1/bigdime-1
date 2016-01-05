/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.splunkalert.response;

import io.bigdime.adaptor.metadata.MetadataAccessException;
import io.bigdime.adaptor.metadata.MetadataStore;
import io.bigdime.alert.Alert;
import io.bigdime.alert.AlertException;
import io.bigdime.alert.Logger;
import io.bigdime.alert.ManagedAlertService;
import io.bigdime.alert.Logger.ALERT_CAUSE;
import io.bigdime.alert.Logger.ALERT_SEVERITY;
import io.bigdime.alert.AlertServiceRequest;
import io.bigdime.alert.Logger.ALERT_TYPE;
import io.bigdime.alert.ManagedAlertService.ALERT_STATUS;
import io.bigdime.alert.LoggerFactory;
import io.bigdime.alert.ManagedAlert;
import io.bigdime.splunkalert.SplunkAlert;
import io.bigdime.splunkalert.common.exception.AuthorizationException;
import io.bigdime.splunkalert.handler.SplunkAuthTokenProvider;
import io.bigdime.splunkalert.retriever.SplunkSourceMetadataRetriever;

import java.io.IOException;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.codehaus.jackson.JsonNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import static io.bigdime.splunkalert.constants.SplunkMetadataConstants.PATH;
import static io.bigdime.splunkalert.constants.SplunkMetadataConstants.SERVICENS;
import static io.bigdime.splunkalert.constants.SplunkMetadataConstants.BIGDATAPLATFORM;
import static io.bigdime.splunkalert.constants.SplunkMetadataConstants.ALERTS;
import static io.bigdime.splunkalert.constants.SplunkMetadataConstants.FIRED_ALERTS;
import static io.bigdime.splunkalert.constants.SplunkMetadataConstants.LINKS;
import static io.bigdime.splunkalert.constants.SplunkMetadataConstants.JOB;
import static io.bigdime.splunkalert.constants.SplunkMetadataConstants.RESULTS;
import static io.bigdime.splunkalert.constants.SplunkMetadataConstants.ENTRY;
import static io.bigdime.splunkalert.constants.SplunkMetadataConstants.SOURCE_NAME;

/**
 * Provides methods to return List of alert objects to the caller.
 * 
 * @author Sandeep Reddy,Murthy
 * 
 */
@Component
public class AlertBuilder {

	private static final Logger logger = LoggerFactory
			.getLogger(AlertBuilder.class);

	@Autowired
	private SplunkSourceMetadataRetriever splunkSourceMetadataRetriever;
	@Autowired
	private SplunkAuthTokenProvider splunkAuthTokenProvider;
	@Value("${splunk.username}")
	private String USERNAME_VALUE;
	@Autowired
	private ManagedAlertService managedAlertService;
	@Autowired
	private MetadataStore metadataStore;

	/**
	 * dateFormatHolder.get() used to get an instance of SimpleDateFormat object
	 */
	private static final ThreadLocal<SimpleDateFormat> dateFormatHolder = new ThreadLocal<SimpleDateFormat>() {
		@Override
		protected SimpleDateFormat initialValue() {
			return new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSS");
		}
	};

	/**
	 * Makes calls 1)To get authorization to splunk and the then 2)Utilizes the
	 * authorization key to get data from splunk 3)The data is converted into
	 * list of alert objects that would be returned to the caller.
	 * 
	 * @param alertName
	 * @return
	 * @throws IOException
	 * @throws URISyntaxException
	 * @throws AuthorizationException
	 * @deprecated this method would be deprecated from the final cut
	 */
	public List<SplunkAlert> getAlertsFromSplunk(String alertName)
			throws AuthorizationException {

		List<SplunkAlert> splunkAlertList = new ArrayList<SplunkAlert>();
		SplunkAlert splunkAlert = null;
		Properties properties = new Properties();
		List<String> jobPaths = new ArrayList<String>(2);
		String authToken = null;
		try {
			authToken = splunkAuthTokenProvider.getNewAuthToken();
			properties.put(PATH, "/" + SERVICENS + "/" + USERNAME_VALUE + "/"
					+ BIGDATAPLATFORM + "/" + ALERTS + "/" + FIRED_ALERTS + "/"
					+ alertName);
			JsonNode firedAlerts = splunkSourceMetadataRetriever
					.getSourceMetadata(authToken, properties);
			for (JsonNode alertEntry : firedAlerts.get(ENTRY)) {
				if (alertEntry.get(LINKS) != null
						&& alertEntry.get(LINKS).get(JOB) != null) {
					jobPaths.add(alertEntry.get(LINKS).get(JOB).asText() + "/"
							+ RESULTS);
				}
			}
			for (String jobPath : jobPaths) {
				properties.put(PATH, jobPath);
				JsonNode jobResult = splunkSourceMetadataRetriever
						.getSourceMetadata(authToken, properties);
				if (jobResult.get(RESULTS).size() != 0) {
					splunkAlert = splunkAlertObjectBuilder(jobResult
							.get(RESULTS));
					splunkAlertList.add(splunkAlert);
				}

			}
			return splunkAlertList;
		} catch (IOException | AuthorizationException | URISyntaxException e) {
			logger.info(SOURCE_NAME,
					"Exception while fetching alerts from splunk",
					e.getMessage());
			throw new AuthorizationException(
					"Exception while fetching alerts from splunk"
							+ e.getMessage());
		}
	}

	/**
	 * This method would be called by handlers in a regular interval.The method
	 * would get the alerts from splunk for the past x days and adds them to the
	 * persistent store.
	 * 
	 * @throws AlertException
	 * @throws MetadataAccessException
	 * @throws URISyntaxException
	 * @throws IOException
	 */

	public void insertAlertsIntoPersistantStore() throws AlertException,
			MetadataAccessException, IOException, URISyntaxException {
		List<ManagedAlert> managedAlertList = new ArrayList<ManagedAlert>();
		Set<String> datasourceSet = metadataStore.getDataSources();
		if (!datasourceSet.isEmpty()) {
			for (String alert : datasourceSet) {
				ManagedAlert managedAlert = null;
				Properties properties = new Properties();
				List<String> jobPaths = new ArrayList<String>();
				String authToken = null;
				try {
					authToken = splunkAuthTokenProvider.getNewAuthToken();
					properties.put(PATH, "/" + SERVICENS + "/" + USERNAME_VALUE
							+ "/" + BIGDATAPLATFORM + "/" + ALERTS + "/"
							+ FIRED_ALERTS + "/" + alert);
					JsonNode firedAlerts = splunkSourceMetadataRetriever
							.getSourceMetadata(authToken, properties);
					if (!firedAlerts.isNull()) {
						for (JsonNode alertEntry : firedAlerts.get(ENTRY)) {
							if (alertEntry.get(LINKS) != null
									&& alertEntry.get(LINKS).get(JOB) != null) {
								jobPaths.add(alertEntry.get(LINKS).get(JOB)
										.asText()
										+ "/" + RESULTS);
							}
						}
					}
					if (!jobPaths.isEmpty()) {
						for (String jobPath : jobPaths) {
							properties.put(PATH, jobPath);
							JsonNode jobResult = splunkSourceMetadataRetriever
									.getSourceMetadata(authToken, properties);
							if (!jobResult.isNull() && jobResult.get(RESULTS).size() != 0) {
								for (int i = 0; i < jobResult.get(RESULTS)
										.size(); i++) {
									managedAlert = managedAlertObjectBuilder(jobResult
											.get(RESULTS).get(i));
									managedAlertList.add(managedAlert);
								}
							}
						}
					}
				} catch (AlertException e) {
					logger.info(SOURCE_NAME,
							"Exception while fetching alerts from splunk",
							e.getMessage());
				}
				updateAlert(managedAlertList);
			}
		} else {
			throw new AlertException(
					"No datasources available in metadata for monitoring");
		}

	}

	/**
	 * Inserts the list of alerts fetched from end point into persistent store.
	 * @param alertMessage
	 * @param alertStatus
	 * @param comment
	 * @throws AlertException
	 */
	public void updateAlert(List<ManagedAlert> alertList) {
		for (ManagedAlert alertMessage : alertList) {
			try {
				managedAlertService.updateAlert(alertMessage,
						alertMessage.getAlertStatus(),
						alertMessage.getComment());
			} catch (AlertException e) {
				logger.info(
						"HBASE",
						"Inserting alert in HBASE",
						"Failed to insert/update into HBASE : "
								+ e.getMessage());
			}
		}
	}

	/**
	 * Takes the raw json data and returns an object of the alert
	 * 
	 * @param jobResult
	 * @return ManagedAlert object
	 */

	public ManagedAlert managedAlertObjectBuilder(JsonNode jobResult) {
		ManagedAlert managedAlert = new ManagedAlert();
		if (jobResult != null) {
			if (jobResult.get("adaptor_name") != null) {
				managedAlert.setApplicationName(jobResult.get("adaptor_name")
						.getTextValue());
			}
			if (jobResult.get("alert_cause") != null) {
				if (jobResult.get("alert_cause").getTextValue()
						.equalsIgnoreCase("adaptor configuration is invalid")) {
					managedAlert
							.setCause(ALERT_CAUSE.INVALID_ADAPTOR_CONFIGURATION);
				} else if (jobResult.get("alert_cause").getTextValue()
						.equalsIgnoreCase("data validation error")) {
					managedAlert.setCause(ALERT_CAUSE.VALIDATION_ERROR);
				} else if (jobResult.get("alert_cause").getTextValue()
						.equalsIgnoreCase("input message too big")) {
					managedAlert.setCause(ALERT_CAUSE.MESSAGE_TOO_BIG);
				} else if (jobResult.get("alert_cause").getTextValue()
						.equalsIgnoreCase("unsupported data or character type")) {
					managedAlert.setCause(ALERT_CAUSE.UNSUPPORTED_DATA_TYPE);
				} else if (jobResult.get("alert_cause").getTextValue()
						.equalsIgnoreCase("input data schema changed")) {
					managedAlert
							.setCause(ALERT_CAUSE.INPUT_DATA_SCHEMA_CHANGED);
				} else if (jobResult.get("alert_cause").getTextValue()
						.equalsIgnoreCase("data could not be read from source")) {
					managedAlert.setCause(ALERT_CAUSE.INPUT_ERROR);
				} else if (jobResult.get("alert_cause").getTextValue()
						.equalsIgnoreCase("internal error")) {
					managedAlert
							.setCause(ALERT_CAUSE.APPLICATION_INTERNAL_ERROR);
				} else if (jobResult.get("alert_cause").getTextValue()
						.equalsIgnoreCase("shutdown command received")) {
					managedAlert.setCause(ALERT_CAUSE.SHUTDOWN_COMMAND);
				}
			}
			if (jobResult.get("detail_message") != null) {
				managedAlert.setMessage(jobResult.get("detail_message")
						.getTextValue());
			}
			if (jobResult.get("message_context") != null) {
				managedAlert.setMessageContext(jobResult.get("message_context")
						.getTextValue());
			}
			if (jobResult.get("alert_code") != null) {
				if (jobResult.get("alert_code").getTextValue()
						.equalsIgnoreCase("BIG-0001")) {
					managedAlert.setType(ALERT_TYPE.ADAPTOR_FAILED_TO_START);
				} else if (jobResult.get("alert_code").getTextValue()
						.equalsIgnoreCase("BIG-0002")) {
					managedAlert.setType(ALERT_TYPE.INGESTION_FAILED);
				} else if (jobResult.get("alert_code").getTextValue()
						.equalsIgnoreCase("BIG-0003")) {
					managedAlert.setType(ALERT_TYPE.INGESTION_DID_NOT_RUN);
				} else if (jobResult.get("alert_code").getTextValue()
						.equalsIgnoreCase("BIG-0004")) {
					managedAlert.setType(ALERT_TYPE.INGESTION_STOPPED);
				} else if (jobResult.get("alert_code").getTextValue()
						.equalsIgnoreCase("BIG-0005")) {
					managedAlert.setType(ALERT_TYPE.DATA_FORMAT_CHANGED);
				} else if (jobResult.get("alert_code").getTextValue()
						.equalsIgnoreCase("BIG-9999")) {
					managedAlert.setType(ALERT_TYPE.OTHER_ERROR);
				}
			}
			if (jobResult.get("alert_severity") != null) {
				if (jobResult.get("alert_severity").getTextValue()
						.equalsIgnoreCase("BLOCKER")) {
					managedAlert.setSeverity(ALERT_SEVERITY.BLOCKER);
				} else if (jobResult.get("alert_severity").getTextValue()
						.equalsIgnoreCase("MAJOR")) {
					managedAlert.setSeverity(ALERT_SEVERITY.MAJOR);
				} else if (jobResult.get("alert_severity").getTextValue()
						.equalsIgnoreCase("NORMAL")) {
					managedAlert.setSeverity(ALERT_SEVERITY.NORMAL);
				}
			}
			try {
				Date date = dateFormatHolder.get().parse(
						jobResult.get("_time").getTextValue());
				managedAlert.setDateTime(date);
			} catch (ParseException e) {
				logger.info(SOURCE_NAME, "Parsing Datetime fields",
						"Exception while parsing the Datatime value");
			}
		}
		return managedAlert;
	}

	/**
	 * Takes the raw json data and returns an object of the SplunkAlert
	 * 
	 * @param jobResult
	 * @return SplunkAlert object
	 * @deprecated this method would be deprecated from the final cut
	 */

	public SplunkAlert splunkAlertObjectBuilder(JsonNode jobResult) {
		SplunkAlert splunkAlert = new SplunkAlert();
		if (jobResult.get(0) != null) {
			if (jobResult.get(0).get("_raw") != null
					&& jobResult.get(0).get("_raw").getTextValue() != null) {
				splunkAlert
						.set_raw(jobResult.get(0).get("_raw").getTextValue());
			}
			if (jobResult.get(0).get("host") != null
					&& jobResult.get(0).get("host").getTextValue() != null) {
				splunkAlert
						.setHost(jobResult.get(0).get("host").getTextValue());
			}
			if (jobResult.get(0).get("sourcetype") != null
					&& jobResult.get(0).get("sourcetype").getTextValue() != null) {
				splunkAlert.setSourceType(jobResult.get(0).get("sourcetype")
						.getTextValue());
			}
			if (jobResult.get(0).get("tag::eventtype") == null) {
				splunkAlert.setSeverity(ALERT_SEVERITY.NORMAL);
			} else if (jobResult.get(0).get("tag::eventtype") != null
					&& jobResult.get(0).get("tag::eventtype").getTextValue() != null
					&& jobResult.get(0).get("tag::eventtype").getTextValue()
							.equalsIgnoreCase("ERROR")) {
				splunkAlert.setSeverity(ALERT_SEVERITY.MAJOR);
			} else if (jobResult.get(0).get("tag::eventtype") != null
					&& jobResult.get(0).get("tag::eventtype").getTextValue() != null
					&& jobResult.get(0).get("tag::eventtype").getTextValue()
							.equalsIgnoreCase("FATAL")) {
				splunkAlert.setSeverity(ALERT_SEVERITY.BLOCKER);
			} else {
				splunkAlert.setSeverity(ALERT_SEVERITY.NORMAL);
			}
		}

		try {
			Date date = dateFormatHolder.get().parse(
					jobResult.get(0).get("_time").getTextValue());
			splunkAlert.setDateTime(date);
		} catch (ParseException e) {
			logger.info(SOURCE_NAME, "Parsing Datetime fields",
					"Exception while parsing the Datatime value");
		}
		return splunkAlert;
	}
}
