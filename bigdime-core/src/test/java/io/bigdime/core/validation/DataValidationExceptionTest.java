/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core.validation;

import java.lang.reflect.InvocationTargetException;

import org.testng.annotations.Test;

import io.bigdime.common.testutils.ExceptionTester;

public class DataValidationExceptionTest extends ExceptionTester {

	@Test
	public void testConstructorWithString() throws InstantiationException, IllegalAccessException,
			IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException {
		testConstructorWithString(DataValidationException.class);
	}
}
