/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core;

/**
 * This exception is thrown when the handler unable to process data because it's
 * missing some fields or data is not of expected type.
 * 
 * @author Neeraj Jain
 *
 */
public class InvalidDataException extends HandlerException {
	private static final long serialVersionUID = 1L;

	public InvalidDataException(String message) {
		super(message);
	}

}
