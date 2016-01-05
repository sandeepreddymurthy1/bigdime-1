/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.common.exception;

import java.io.IOException;

public class AuthorizationException extends IOException {

	private static final long serialVersionUID = -3280434915691627977L;
	
	public AuthorizationException (String message) {
		super(message);
	}
	public AuthorizationException(String message, Throwable throwable) {
		super(message, throwable);
	}
}