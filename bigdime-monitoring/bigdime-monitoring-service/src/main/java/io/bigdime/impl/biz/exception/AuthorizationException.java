/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.impl.biz.exception;

import javax.ws.rs.WebApplicationException;

/**
 * Authorization Exception is thrown when the application is not able not
 * requested in a proper fashion
 * 
 * @author Sandeep Reddy,Murthy
 * 
 */
public class AuthorizationException extends WebApplicationException {

	/**
	 * AuthorizationException Constructor that would be used to throw it
	 * exception with a reason.
	 * 
	 * @param message
	 */
	public AuthorizationException(String message) {
		super(message);
	}

	/**
	 * AuthorizationException Constructor that would be used to throw it with a
	 * reason along with throwable object.
	 * 
	 * @param message
	 */
	public AuthorizationException(String message, Throwable throwable) {
		super(message, throwable);
	}
}