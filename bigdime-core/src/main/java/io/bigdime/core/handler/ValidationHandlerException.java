/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core.handler;

import io.bigdime.core.HandlerException;

public class ValidationHandlerException extends HandlerException {

	public ValidationHandlerException(String message) {
		super(message);
	}

	public ValidationHandlerException(String message, Throwable t) {
		super(message, t);
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

}
