/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.libs.hdfs;

import java.io.IOException;

public class WebHDFSSinkException extends IOException {
	private static final long serialVersionUID = 1L;

	public WebHDFSSinkException(String message) {
		super(message);
	}

	public WebHDFSSinkException(String message, Exception e) {
		super(message, e);
	}
}
