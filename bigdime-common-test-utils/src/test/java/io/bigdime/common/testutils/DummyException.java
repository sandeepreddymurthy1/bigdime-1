/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.common.testutils;

public class DummyException extends Exception {
	private static final long serialVersionUID = 1L;

	public DummyException(Throwable t) {
		super(t);
	}

	public DummyException(String message) {
		super(message);
	}

	public DummyException(String message, Throwable t) {
		super(message, t);
	}

}
