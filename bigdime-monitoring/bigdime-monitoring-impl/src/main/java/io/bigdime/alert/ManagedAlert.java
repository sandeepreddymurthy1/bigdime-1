/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.alert;

import io.bigdime.alert.ManagedAlertService.ALERT_STATUS;

/**
 * Defines an extension of {@link Alert} object with additional properties.
 *
 * @author Neeraj Jain
 *
 */
public class ManagedAlert extends Alert {

	private String comment;
	private ALERT_STATUS alertStatus;

	public String getComment() {
		return comment;
	}

	public void setComment(String comment) {
		this.comment = comment;
	}

	public ALERT_STATUS getAlertStatus() {
		return alertStatus;
	}

	public void setAlertStatus(ALERT_STATUS alertStatus) {
		this.alertStatus = alertStatus;
	}

}
