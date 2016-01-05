/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core.validation;

import io.bigdime.core.ActionEvent;

/**
 * Validator interface is implemented for different validation types; e.g.
 * raw_checksum, record_count, column_type etc. Only one implementation for each
 * validation type is supported.
 * 
 * @author Neeraj Jain
 *
 */
public interface Validator {

	/**
	 * Implement validation logic and returns the {@link ValidationResponse}
	 * object containing the result of validation.
	 * 
	 * @return ValidationResponse object encapsulating result of validation
	 * @throws DataValidationException
	 *             in case there was any problem in performing validation
	 */
	public ValidationResponse validate(ActionEvent actionEvent) throws DataValidationException;

	public String getName();
}
