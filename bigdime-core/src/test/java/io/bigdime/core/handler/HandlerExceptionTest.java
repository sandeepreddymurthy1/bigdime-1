/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core.handler;

import org.testng.annotations.Test;

import io.bigdime.common.testutils.ExceptionTester;
import io.bigdime.core.HandlerException;

public class HandlerExceptionTest extends ExceptionTester {

	@Test
	public void testConstructorWithString() throws Throwable {
		super.testConstructorWithString(HandlerException.class);
	}

	@Test
	public void testConstructorWithStringAndThrowable() throws Throwable {
		super.testConstructorWithStringAndThrowable(HandlerException.class);
	}
	@Test
	public void testConstructorWithThrowable() throws Throwable {
		super.testConstructorWithThrowable(HandlerException.class);
	}
}
