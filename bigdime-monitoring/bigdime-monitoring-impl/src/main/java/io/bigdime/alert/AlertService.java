/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.alert;

/**
 * Provides methods to fetch the alert messages from alert store(be it a file
 * storage, splunk etc).
 * @formatter:off
 * Usage:
 *
 * AlertService service = AlertFetchServiceFactory.get();
   AlertServiceRequest alertServiceRequest = new AlertServiceRequest();
   alertServiceRequest.setAlertName("file-load-alert");
   alertServiceRequest.setFromDate(null);//null defaults to 24 hours
   alertServiceRequest.setToDate(null);//null defaults to now
   alertServiceRequest.setLimit(0);//0 defaults to defaults to 50. Max value of limit is 50.
   alertServiceRequest.setOffset(0);//0 defaluts to most recent message for the given condition.
   alertServiceRequest.setFromDate(null);//null defaults to 24 hours
   AlertServiceResponse response = service.getAlerts(alertServiceRequest);
 *
 * @author Neeraj Jain
 *
 */
public interface AlertService<T extends Alert> {
	/**
	 * Get the list of alerts raised based on the alert name. An alert name
	 * encapsulates the alert condition. If the number of alert messages found
	 * are more than the default limit that one call can support, default limit
	 * is applied.
	 *
	 * @param alertServiceRequest request encapsulating query parameters
	 * @return instance of {@link AlertServiceResponse}
	 */
	public AlertServiceResponse<T> getAlerts(AlertServiceRequest alertServiceRequest) throws AlertException;
}
