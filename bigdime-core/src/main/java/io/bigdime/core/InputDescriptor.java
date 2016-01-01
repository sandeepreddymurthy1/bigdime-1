/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core;

import java.util.List;

/**
 * Object encapsulating fields that compose the input descriptor:
 * @formatter:off 
 * For file, it'll be filePath and fileName. 
 * For kafka, it'll be topic and partition.
 * For sql, it'll be just the tableName or tableName:rownum.
 * @formatter:on
 * @author Neeraj Jain
 *
 * @param <T>
 */
public interface InputDescriptor<T> {
	/**
	 * Get the next descriptor to process based on available input descriptors
	 * and last processed descriptor. The toString method of this object is
	 * supposed to concatenate the fields into a string.
	 * 
	 * @param availableInputDescriptors
	 *            list of all descriptors to be processed, in the order they are
	 *            supposed to be processed.
	 * @param lastInputDescriptor
	 *            last processed descriptor as a String.
	 * @return next descriptor to be processed
	 */
	public T getNext(List<T> availableInputDescriptors, String lastInputDescriptor);

	/**
	 * Parses individual fields from the string and populates them.
	 * 
	 * @param descriptor
	 *            string that contains the descriptor fields concatenated
	 */
	public void parseDescriptor(String descriptor);

}
