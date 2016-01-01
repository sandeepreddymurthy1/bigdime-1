/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core;

import java.lang.reflect.InvocationTargetException;

import org.testng.annotations.Test;

import io.bigdime.common.testutils.ExceptionTester;

public class InvalidDataExceptionTest extends  ExceptionTester {

	@Test
	public void testConstructor() throws InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException {
		testConstructorWithString(InvalidDataException.class);
	}
}
