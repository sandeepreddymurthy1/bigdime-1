/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.ha.event.client.exception;

/**
 * @author jbrinnand
 */
public class ZKEventHandlerClientException extends Exception{
	private static final long serialVersionUID = 1L;
	
	public ZKEventHandlerClientException(String message) {
		super(message);
	}
	public ZKEventHandlerClientException(String message, Throwable throwable) {
		super(message, throwable);
	}
}
