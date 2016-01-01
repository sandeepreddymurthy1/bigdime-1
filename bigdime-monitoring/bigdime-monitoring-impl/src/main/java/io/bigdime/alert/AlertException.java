/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.alert;

/**
 * This exception is thrown in case of any issues while interacting with alert
 * stores.
 *
 * @author Neeraj Jain
 *
 */
public class AlertException extends Exception {

	/**
	 *
	 */
	private static final long serialVersionUID = 1L;

	public AlertException(String message) {
		super(message);
	}

	public AlertException(String message, Throwable throwable) {
		super(message, throwable);
	}

}
