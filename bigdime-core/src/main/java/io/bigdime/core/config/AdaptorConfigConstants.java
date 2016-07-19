/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core.config;

public class AdaptorConfigConstants {

	public static class SourceConfigConstants {
		public static final String NAME = "name";
		public static final String DESCRIPTION = "description";
		public static final String DATA_HANDLERS = "data-handlers";
		public static final String SRC_DESC = "src-desc";
		public static final String SOURCE_TYPE = "source-type";
	}

	public static class ChannelConfigConstants {
		public static final String NAME = "name";
		public static final String DESCRIPTION = "description";
		public static final String CHANNEL_CLASS = "channel-class";
		public static final String PROPERTIES = "properties";
		public static final String CONCURRENCY = "concurrency";
		public static final String PRINT_STATS = "print-stats";
		public static final String CHANNEL_CAPACITY = "channel-capacity";
		public static final String PRINT_STATS_DURATION_IN_SECONDS = "print-stats-duration-in-seconds";
	}

	public static class SinkConfigConstants {
		public static final String NAME = "name";
		public static final String DESCRIPTION = "description";
		public static final String DATA_HANDLERS = "data-handlers";
		public static final String CHANNEL_DESC = "channel-desc";
	}

	public static class HandlerConfigConstants {
		public static final String NAME = "name";
		public static final String DESCRIPTION = "description";
		public static final String HANDLER_CLASS = "handler-class";
		public static final String PROPERTIES = "properties";
		public static final String CHANNEL_MAP = "channel-map";
	}

	public static class ValidationHandlerConfigConstants {
		public static final String VALIDATION_TYPE = "validation-type";
	}
}
