/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core.handler;

import java.lang.reflect.InvocationTargetException;

import org.testng.annotations.Test;

import io.bigdime.common.testutils.ExceptionTester;

public class IllegalAdaptorStateExceptionTest extends ExceptionTester {

	/**
	 * Assert that an Exception object can be constructed using the
	 * {@code IllegalHandlerStateException#IllegalHandlerStateException(String)}
	 * constructor. Assert that getMessage method returns the same string that
	 * was passed during object construction.
	 * 
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws IllegalArgumentException
	 * @throws InvocationTargetException
	 * @throws NoSuchMethodException
	 * @throws SecurityException
	 */
	@Test
	public void testConstructorWithString() throws InstantiationException, IllegalAccessException,
			IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException {
		super.testConstructorWithString(IllegalHandlerStateException.class);
	}

}
