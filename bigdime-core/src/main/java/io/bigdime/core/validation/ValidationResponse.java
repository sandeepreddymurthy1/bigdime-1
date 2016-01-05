/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core.validation;

/**
 * Validators encapsulate the validation results in {@link ValidationResponse}
 * object.
 * 
 * @author Neeraj Jain
 *
 */
public class ValidationResponse {

	public enum ValidationResult {
		/**
		 * Validation passed.
		 */
		PASSED,

		/**
		 * Validation failed due to mismatch in column count. May not indicate a
		 * fatal error.
		 */
		COLUMN_COUNT_MISMATCH,

		/**
		 * Validation failed due to mismatch in data type. May not indicate a
		 * fatal error.
		 */
		COLUMN_TYPE_MISMATCH,

		/**
		 * Validation failed.
		 */
		FAILED,

		/**
		 * Validation was not attempted, writing to file still in progress. May
		 * not indicate an error.
		 */
		NOT_READY,

		/**
		 * Validation was not attempted, either the db, table, file did not
		 * exist. May not indicate a fatal error.
		 */
		INCOMPLETE_SETUP;
	}

	private ValidationResult validationResult;
	private String comment;

	/**
	 * Gets the result of validation.
	 * 
	 * @return true if validation was successful, false otherwise
	 */
	public ValidationResult getValidationResult() {
		return validationResult;
	}

	/**
	 * Sets the validationPassed flag on this object.
	 * 
	 * @param validationResult
	 *            validationResult to set
	 */
	public void setValidationResult(ValidationResult validationResult) {
		this.validationResult = validationResult;
	}

	/**
	 * Gets the optional comments set by validator as a result of performing
	 * validation. This could contain as to why validation failed.
	 * 
	 * @return comment
	 */
	public String getComment() {
		return comment;
	}

	/**
	 * Sets the comment on this object.
	 * 
	 * @param comment
	 *            comment to set
	 */
	public void setComment(String comment) {
		this.comment = comment;
	}
}
