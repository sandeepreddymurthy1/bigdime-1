/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core.handler;

import java.lang.reflect.InvocationTargetException;

import org.testng.annotations.Test;

import io.bigdime.common.testutils.ExceptionTester;

public class ValidationHandlerExceptionTest extends ExceptionTester {

	/**
	 * Assert that an Exception object can be constructed using the
	 * {@code ValidationHandlerException#ValidationHandlerException(String)}
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
	public void testConstructorWithStringArg() throws InstantiationException, IllegalAccessException,
			IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException {
		super.testConstructorWithString(ValidationHandlerException.class);
	}

	/**
	 * Assert that an Exception object can be constructed using the
	 * {@code ValidationHandlerException#ValidationHandlerException(String, Throwable))}
	 * constructor. Assert that getMessage method returns the same string and
	 * same Throwable object that were passed during object construction.
	 * 
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws IllegalArgumentException
	 * @throws InvocationTargetException
	 * @throws NoSuchMethodException
	 * @throws SecurityException
	 */
	@Test
	public void testConstructorWithStringAndThrowableArg() throws InstantiationException, IllegalAccessException,
			IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException {
		super.testConstructorWithStringAndThrowable(ValidationHandlerException.class);
	}
}
