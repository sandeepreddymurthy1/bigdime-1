/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.handler.webhdfs;

import org.apache.commons.lang3.StringUtils;

import com.google.common.base.Preconditions;

public class HdfsFileNameBuilder {
	private String prefix;
	private String channelDesc;
	private String sourceFileName;
	private String extension;

	public HdfsFileNameBuilder withPrefix(String prefix) {
		this.prefix = prefix;
		return this;
	}

	public HdfsFileNameBuilder withChannelDesc(String channelDesc) {
		this.channelDesc = channelDesc;
		return this;
	}

	public HdfsFileNameBuilder withSourceFileName(String sourceFileName) {
		this.sourceFileName = sourceFileName;
		return this;
	}

	public HdfsFileNameBuilder withExtension(String extension) {
		this.extension = extension;
		return this;
	}

	public String build() {
		Preconditions.checkNotNull(prefix, "prefix can't be null");
		Preconditions.checkNotNull(channelDesc, "channelDesc can't be null");
		Preconditions.checkNotNull(extension, "extension can't be null");
		if (StringUtils.isBlank(sourceFileName)) {
			return prefix + channelDesc + extension;
		}
		return prefix + sourceFileName + extension;
	}
}