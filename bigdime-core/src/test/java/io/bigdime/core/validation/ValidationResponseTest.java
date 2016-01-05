/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core.validation;

import org.testng.Assert;
import org.testng.annotations.Test;

import io.bigdime.core.validation.ValidationResponse.ValidationResult;

public class ValidationResponseTest {

	@Test
	public void testGettersAndSetters() {
		ValidationResponse validationResponse = new ValidationResponse();
		validationResponse.setComment("unit-test-comment");
		validationResponse.setValidationResult(ValidationResult.PASSED);

		Assert.assertEquals(validationResponse.getComment(), "unit-test-comment");
		Assert.assertEquals(validationResponse.getValidationResult(), ValidationResult.PASSED);

	}
}
