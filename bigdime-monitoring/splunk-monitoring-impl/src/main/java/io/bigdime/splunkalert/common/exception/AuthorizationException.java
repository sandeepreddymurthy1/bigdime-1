/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.splunkalert.common.exception;

import io.bigdime.alert.AlertException;

import java.io.IOException;
/**
 * Authorization Exception is thrown when the application is not able to authenticate to splunk
 * @author Sandeep Reddy,Murthy
 *
 */
public class AuthorizationException extends AlertException {

	private static final long serialVersionUID = -3280434915691627977L;
    /**
     * 	AuthorizationException Constructor that would be used to throw it exception with a reason.
     * @param message
     */
	public AuthorizationException (String message) {
		super(message);
	}
	 /**
     * 	AuthorizationException Constructor that would be used to throw it with a reason along with throwable object.
     * @param message
     */
	public AuthorizationException(String message, Throwable throwable) {
		super(message, throwable);
	}
}