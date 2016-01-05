/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.handler.file;

public final class FileInputStreamReaderHandlerConstants {
	private static final FileInputStreamReaderHandlerConstants instance = new FileInputStreamReaderHandlerConstants();

	private FileInputStreamReaderHandlerConstants() {
	}

	public static FileInputStreamReaderHandlerConstants getInstance() {
		return instance;
	}

	public static final String FILE_NAME_PATTERN = "file-name-pattern";
	public static final String INPUT_DESC = "input-desc";
	public static final String BUFFER_SIZE = "buffer-size";

	public static final String BASE_PATH = "base-path";
	public static final String PRESERVE_BASE_PATH = "preserve-base-path";
	public static final String PRESERVE_RELATIVE_PATH = "preserve-relative-path";
}