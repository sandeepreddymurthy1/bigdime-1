/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core.commons;

import io.bigdime.alert.AlertMessage;
import io.bigdime.alert.Logger;
import io.bigdime.alert.Logger.ALERT_CAUSE;
import io.bigdime.alert.Logger.ALERT_SEVERITY;
import io.bigdime.alert.Logger.ALERT_TYPE;
import io.bigdime.core.AdaptorContextImpl;

public class AdaptorLogger {
	private Logger logger;

	public AdaptorLogger(Logger logger) {
		this.logger = logger;
	}

	public void debug(String shortMessage, String message) {
		logger.debug(getAdaptorName(), shortMessage, message);
	}

	public void debug(String shortMessage, String format, Object... o) {
		logger.debug(getAdaptorName(), shortMessage, format, o);
	}

	public void info(String shortMessage, String message) {
		logger.info(getAdaptorName(), shortMessage, message);
	}

	public void info(String shortMessage, String format, Object... o) {
		logger.info(getAdaptorName(), shortMessage, format, o);
	}

	public void warn(String shortMessage, String message, Throwable t) {
		logger.warn(getAdaptorName(), shortMessage, message, t);
	}

	public void warn(String shortMessage, String message) {
		logger.warn(getAdaptorName(), shortMessage, message);
	}

	public void warn(String shortMessage, String format, Object... o) {
		logger.warn(getAdaptorName(), shortMessage, format, o);
	}

	public void alert(ALERT_TYPE alertType, ALERT_CAUSE alertCause, ALERT_SEVERITY alertSeverity, String message) {
		logger.alert(getAdaptorName(), alertType, alertCause, alertSeverity, message);
	}

	public void alert(ALERT_TYPE alertType, ALERT_CAUSE alertCause, ALERT_SEVERITY alertSeverity, String message,
			Throwable e) {
		logger.alert(getAdaptorName(), alertType, alertCause, alertSeverity, message, e);
	}

	public void alert(ALERT_TYPE alertType, ALERT_CAUSE alertCause, ALERT_SEVERITY alertSeverity, String format,
			Object... o) {
		logger.alert(getAdaptorName(), alertType, alertCause, alertSeverity, format, o);
	}

	public void alert(AlertMessage alertMessage) {
		logger.alert(alertMessage);
	}

	private String getAdaptorName() {
		return AdaptorContextImpl.getInstance().getAdaptorName();
	}
}
