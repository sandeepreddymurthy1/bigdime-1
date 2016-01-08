/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.alert;

import java.util.List;

/**
 * Defines an object that defines response for an {@link AlertServiceRequest}
 * object.
 *
 * @author Neeraj Jain
 *
 * @param <T>
 *            the type of Alert that this response object contains
 */
public class AlertServiceResponse<T extends Alert> {

	/**
	 * Number of messages found for given alert request.
	 */
	private int numFound;

	/**
	 * List of the alerts.
	 */
	private List<T> alerts;

	/**
	 * Get the number of messages found for the given alert request.
	 *
	 * @return
	 */
	public int getNumFound() {
		return numFound;
	}

	/**
	 * Set the numFound.
	 *
	 * @param numFound
	 *            numFound to set on this object
	 */
	public void setNumFound(int numFound) {
		this.numFound = numFound;
	}

	/**
	 * Get the alerts found for the given alert request.
	 *
	 * @return alertMessage
	 */
	public List<T> getAlerts() {
		return alerts;
	}

	/**
	 * Set alerts
	 *
	 * @param alerts
	 *            alerts to set on this object
	 */
	public void setAlerts(List<T> alertMessages) {
		this.alerts = alertMessages;
	}

}
