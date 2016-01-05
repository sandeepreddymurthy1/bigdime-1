/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.adaptor.metadata;

/**
 * Metadata Access Exception is thrown when the application is not able to
 * access the metadata store to retrieve the metadata.
 * 
 * @author Neeraj Jain
 * 
 */
public class MetadataAccessException extends Exception {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private Throwable sourceException;

	public MetadataAccessException(Throwable sourceException) {
		this.sourceException = sourceException;

	}

	public MetadataAccessException(String errorMessage) {
		super(errorMessage);

	}

}
