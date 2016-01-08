/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.handler.file;

/**
 * 
 * @author Neeraj Jain
 *
 */

public final class FileArchiveHandlerConstants {
	private static final FileArchiveHandlerConstants instance = new FileArchiveHandlerConstants();

	private FileArchiveHandlerConstants() {
	}

	public static FileArchiveHandlerConstants getInstance() {
		return instance;
	}

	public static final String ARCHIVE_PATH = "archive-path";
}