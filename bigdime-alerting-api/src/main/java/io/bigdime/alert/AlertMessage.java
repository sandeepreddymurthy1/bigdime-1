/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.alert;

import io.bigdime.alert.Logger.ALERT_CAUSE;
import io.bigdime.alert.Logger.ALERT_SEVERITY;
import io.bigdime.alert.Logger.ALERT_TYPE;

/**
 * AlertMessage object contains fields that need to be logged with an alert.
 * 
 * @author Neeraj Jain
 * 
 */
public class AlertMessage {

	/**
	 * Name of the adaptor that surfaced the alert situation.
	 */
	private String adaptorName;
	/**
	 * Context in which the alert situation occurred, e.g. during reading phase,
	 * validation phase, writing phase etc.
	 */
	private String messageContext;
	/**
	 * Type of the alert, contains alert code and corresponding description.
	 */
	private ALERT_TYPE type;
	/**
	 * Reason for alert.
	 */
	private ALERT_CAUSE cause;
	/**
	 * Severity of the alert.
	 */
	private ALERT_SEVERITY severity;
	/**
	 * Detailed message describing the exception situation.
	 */
	private String message;

	/**
	 * Gets the name of adaptor set on this object.
	 * 
	 * @return adaptorName
	 */
	public String getAdaptorName() {
		return adaptorName;
	}

	/**
	 * Sets the adaptorName field on this object.
	 * 
	 * @param adaptorName
	 *            adaptorName to set
	 */
	public void setAdaptorName(String adaptorName) {
		this.adaptorName = adaptorName;
	}

	/**
	 * Gets the messageContext(context in which error occured) field set on this
	 * object.
	 * 
	 * @return messageContext field set on this object
	 */
	public String getMessageContext() {
		return messageContext;
	}

	/**
	 * Sets the messageContext field on this object.
	 * 
	 * @param messageContext
	 *            messageContext to set
	 */
	public void setMessageContext(String messageContext) {
		this.messageContext = messageContext;
	}

	/**
	 * Gets the type of alert set on this object.
	 * 
	 * @return type field set on this object
	 */
	public ALERT_TYPE getType() {
		return type;
	}

	/**
	 * Sets the type field on this object.
	 * 
	 * @param type
	 *            type to set
	 */
	public void setType(ALERT_TYPE type) {
		this.type = type;
	}

	/**
	 * Gets the cause of the alert set on this object.
	 * 
	 * @return cause field set on this object
	 */
	public ALERT_CAUSE getCause() {
		return cause;
	}

	/**
	 * Sets the cause of the alert on this object.
	 * 
	 * @param cause
	 *            cause to set
	 */
	public void setCause(ALERT_CAUSE cause) {
		this.cause = cause;
	}

	/**
	 * Gets the severity of the alert set on this object.
	 * 
	 * @return severity field set on this object
	 */
	public ALERT_SEVERITY getSeverity() {
		return severity;
	}

	/**
	 * Sets the severity or alert on this object.
	 * 
	 * @param severity
	 *            severity to set
	 */
	public void setSeverity(ALERT_SEVERITY severity) {
		this.severity = severity;
	}

	/**
	 * Gets the detailed message for the alert set on this object.
	 * 
	 * @return message field set on this object
	 */
	public String getMessage() {
		return message;
	}

	/**
	 * Sets the detailed message for alert on this object.
	 * 
	 * @param message
	 *            message to set
	 */
	public void setMessage(String message) {
		this.message = message;
	}

}
