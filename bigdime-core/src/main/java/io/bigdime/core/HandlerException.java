/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core;

/**
 * TODO add comments
 * 
 * @author Neeraj Jain
 * 
 */

public class HandlerException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public HandlerException(String message) {
		super(message);
	}

	public HandlerException(Throwable t) {
		super(t);
	}

	public HandlerException(String message, Throwable t) {
		super(message, t);
	}

}
