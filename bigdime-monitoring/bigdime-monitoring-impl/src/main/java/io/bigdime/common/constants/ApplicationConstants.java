/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.common.constants;

import org.apache.hadoop.hbase.util.Bytes;

public final class ApplicationConstants {

	public static final String FROM_DATE = "fromDate";
	public static final String TO_DATE = "toDate";
	public static final byte[] ALERT_COLUMN_FAMILY_NAME = Bytes.toBytes("cf");
	public static final byte[] ALERT_METADATA_COLUMN = Bytes.toBytes("data");
	public static final byte[] ALERT_COMMENT_COLUMN = Bytes.toBytes("comment");
//	public static final String ALERT_HBASE_TABLE_NAME = "users_test3";
	public static final byte[] ALERT_STATUS = Bytes.toBytes("alertstatus");

	// Constants
	public static final String EMPTYSTRING = "";
	public static final String NOCOMMENT = "No Comment";

	// logger Constants
	public static final String SOURCE_NAME = "BIGDIME_MONITORING_IMPL";
	
	
	public static final byte[] ALERT_ADAPTOR_NAME_COLUMN = Bytes.toBytes("an");
	public static final byte[] ALERT_MESSAGE_CONTEXT_COLUMN = Bytes.toBytes("cx");
	public static final byte[] ALERT_ALERT_CODE_COLUMN = Bytes.toBytes("cd");
	public static final byte[] ALERT_ALERT_NAME_COLUMN = Bytes.toBytes("al");
	public static final byte[] ALERT_ALERT_CAUSE_COLUMN = Bytes.toBytes("er");
	public static final byte[] ALERT_ALERT_SEVERITY_COLUMN = Bytes.toBytes("sv");
	public static final byte[] ALERT_ALERT_LOG_LEVEL_COLUMN = Bytes.toBytes("lv");
	public static final byte[] ALERT_ALERT_MESSAGE_COLUMN = Bytes.toBytes("ms");
	public static final byte[] ALERT_ALERT_DATE_COLUMN = Bytes.toBytes("dt");
	public static final byte[] ALERT_ALERT_EXCEPTION_COLUMN = Bytes.toBytes("ex");

}
