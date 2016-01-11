package io.bigdime.validation.common;

import org.testng.annotations.Test;

public class AbstractValidatorTest {
	
	@Test(expectedExceptions = IllegalArgumentException.class)
	public void validateNullStringsTest() {
		AbstractValidator abstractValidator = new AbstractValidator();
		abstractValidator.checkNullStrings("key", null);
	}

}
