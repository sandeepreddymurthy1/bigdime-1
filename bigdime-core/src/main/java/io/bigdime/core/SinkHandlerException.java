/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core;

/**
 * Throw this HandlerException if handler is unable to write data to
 * {@link Sink}.
 * 
 * @author Neeraj Jain
 *
 */
public class SinkHandlerException extends HandlerException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public SinkHandlerException(String message) {
		super(message);
	}

	/**
	 * Constructor with message and a Throwable object that contains more
	 * information about error while attempting to write to {@link Sink}.
	 * 
	 * @param message
	 * @param t
	 */
	public SinkHandlerException(String message, Throwable t) {
		super(message, t);
	}

}
