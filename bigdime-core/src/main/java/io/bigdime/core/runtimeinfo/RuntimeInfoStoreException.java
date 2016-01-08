/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core.runtimeinfo;

public class RuntimeInfoStoreException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public RuntimeInfoStoreException(Throwable t) {
		super(t);
	}

	public RuntimeInfoStoreException(String msg) {
		super(msg);
	}

	public RuntimeInfoStoreException(String msg, Throwable t) {
		super(msg, t);

	}

}
