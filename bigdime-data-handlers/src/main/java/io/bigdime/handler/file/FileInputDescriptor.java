/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.handler.file;

import java.io.File;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import io.bigdime.core.InputDescriptor;

/**
 * Encapsulates the fields(filePath, fileName) that are needed to read a file.
 * 
 * @author Neeraj Jain
 *
 */
@Component
public class FileInputDescriptor implements InputDescriptor<String> {

	private String path;
	private String fileName;
	private String entityName;

	/**
	 * Gets the next file to process from the list of available files.
	 * 
	 * @param availableInputs
	 *            list of available files, assumed to be in the order in which
	 *            they need to be processed
	 * @param lastInput
	 *            absolute path of the file that was last processed,
	 *            successfully or unsuccessfully
	 * @return absolute path of the file to process next
	 */
	public String getNext(List<String> availableInputs, String lastInput) {
		isValid(availableInputs, lastInput);

		int indexOfLastInput = availableInputs.indexOf(lastInput);
		if (availableInputs.size() > indexOfLastInput + 1) {
			return availableInputs.get(indexOfLastInput + 1);
		}
		return null;

	}

	private boolean isValid(List<String> availableInputs, String lastInput) {
		if (availableInputs == null) {
			throw new IllegalArgumentException();
		}
		if (StringUtils.isBlank(lastInput))
			throw new IllegalArgumentException();
		return true;
	}

	public String[] parseSourceDescriptor(String descriptor) {
		if (descriptor.contains(":")) {
			String[] stringParts = descriptor.split(":");
			if (stringParts != null && stringParts.length == 2) {
				return stringParts;
			}
		}
		throw new IllegalArgumentException("descriptor must be in entityName:fileNamePrefix format");
	}

	@Override
	public void parseDescriptor(String descriptor) {
		if (StringUtils.isBlank(descriptor)) {
			throw new IllegalArgumentException("descriptor can't be null or empty");
		}

		// String fileLocation = null;
		// if (descriptor.contains(":")) {
		// String[] stringParts = descriptor.split(":");
		// if (stringParts == null || stringParts.length != 2) {
		// throw new IllegalArgumentException("descriptor must be in
		// entityName:fileNamePrefix format");
		// } else {
		// entityName = stringParts[0].trim();
		// fileLocation = stringParts[1];
		// }
		// } else {
		// fileLocation = descriptor;
		// }
		final File file = new File(descriptor);
		if (file.isDirectory()) {
			path = file.getAbsolutePath();
		} else {
			path = file.getParent();
			fileName = file.getName();
		}
		// if (path == null) {
		// throw new IllegalArgumentException("invalid descriptor, must contain
		// the complete file path");
		// }
		if (!path.endsWith(File.separator))
			path = path + File.separator;
	}

	/**
	 * Returns descriptor as path+"/"+fileName.
	 */
	@Override
	public String toString() {
		return path + "/" + fileName;
	}

	public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = path;
	}

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public String getEntityName() {
		return entityName;
	}

	public void setEntityName(String entityName) {
		this.entityName = entityName;
	}

}