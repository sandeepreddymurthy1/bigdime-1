/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core.handler;

import io.bigdime.core.HandlerException;

/**
 * This exception is thrown when an attempt has been made to transition adaptor
 * to a state that can't be attained from the state that the adaptor currently
 * is in.
 *
 * @author Neeraj Jain
 *
 */
public class IllegalHandlerStateException extends HandlerException {

	/**
	 *
	 */
	private static final long serialVersionUID = 1L;

	public IllegalHandlerStateException(String message) {
		super(message);
	}
}
