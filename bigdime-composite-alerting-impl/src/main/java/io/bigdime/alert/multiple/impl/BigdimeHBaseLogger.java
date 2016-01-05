/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.alert.multiple.impl;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.helpers.FormattingTuple;
import org.slf4j.helpers.MessageFormatter;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import io.bigdime.alert.AlertMessage;
import io.bigdime.alert.Logger;
import io.bigdime.alert.multiple.impl.constants.HBaseAlertSchema.ColumnFamily;
import io.bigdime.alert.multiple.impl.constants.HBaseAlertSchema.ColumnQualifier;
import io.bigdime.hbase.client.DataInsertionSpecification;
import io.bigdime.hbase.client.HbaseManager;
import io.bigdime.hbase.client.exception.HBaseClientException;

/**
 * Class with default visibility, does not really need to be accessible outside
 * this package.
 * 
 * @author Neeraj Jain
 *
 */
public class BigdimeHBaseLogger implements Logger {

	private static final ConcurrentMap<String, BigdimeHBaseLogger> loggerMap = new ConcurrentHashMap<>();
	private HbaseManager hbaseManager;
	private String hbaseTableName;
	private String hbaseAlertLevel;

	public static final String FROM_DATE = "fromDate";
	public static final String TO_DATE = "toDate";
	public static final byte[] ALERT_COLUMN_FAMILY_NAME = Bytes.toBytes(ColumnFamily.ALERT_COLUMN_FAMILY);
	public static final byte[] ALERT_ADAPTOR_NAME_COLUMN = Bytes.toBytes(ColumnQualifier.ALERT_ADAPTOR_NAME);
	public static final byte[] ALERT_MESSAGE_CONTEXT_COLUMN = Bytes.toBytes(ColumnQualifier.ALERT_MESSAGE_CONTEXT);
	public static final byte[] ALERT_ALERT_CODE_COLUMN = Bytes.toBytes(ColumnQualifier.ALERT_ALERT_CODE);
	public static final byte[] ALERT_ALERT_NAME_COLUMN = Bytes.toBytes(ColumnQualifier.ALERT_ALERT_NAME);
	public static final byte[] ALERT_ALERT_CAUSE_COLUMN = Bytes.toBytes(ColumnQualifier.ALERT_ALERT_CAUSE);
	public static final byte[] ALERT_ALERT_SEVERITY_COLUMN = Bytes.toBytes(ColumnQualifier.ALERT_ALERT_SEVERITY);
	public static final byte[] ALERT_ALERT_MESSAGE_COLUMN = Bytes.toBytes(ColumnQualifier.ALERT_ALERT_MESSAGE);
	public static final byte[] ALERT_ALERT_DATE_COLUMN = Bytes.toBytes(ColumnQualifier.ALERT_ALERT_DATE);
	public static final byte[] ALERT_ALERT_EXCEPTION_COLUMN = Bytes.toBytes(ColumnQualifier.ALERT_ALERT_EXCEPTION);
	public static final byte[] LOG_LEVEL = Bytes.toBytes(ColumnQualifier.LOG_LEVEL);

	// Constants
	public static final String EMPTYSTRING = "";
	private static final String APPLICATION_CONTEXT_PATH = "META-INF/application-context-monitoring.xml";
	public static final String HBASE_TABLE_NAME_PROPERTY = "${hbase.table.name}";
	public static final String HBASE_ALERT_LEVEL_PROPERTY = "${hbase.alert.level}";

	private boolean debugEnabled = false;
	private boolean infoEnabled = false;
	private boolean warnEnabled = false;

	private BigdimeHBaseLogger() {
	}

	public static Logger getLogger(String loggerName) {
		BigdimeHBaseLogger logger = loggerMap.get(loggerName);
		if (logger == null) {
			logger = new BigdimeHBaseLogger();
			loggerMap.put(loggerName, logger);

			ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext(APPLICATION_CONTEXT_PATH);
			logger.hbaseManager = context.getBean(HbaseManager.class);
			logger.hbaseTableName = context.getBeanFactory().resolveEmbeddedValue(HBASE_TABLE_NAME_PROPERTY);
			logger.hbaseAlertLevel = context.getBeanFactory().resolveEmbeddedValue(HBASE_ALERT_LEVEL_PROPERTY);
			if (logger.hbaseAlertLevel != null) {
				if (logger.hbaseAlertLevel.equalsIgnoreCase("debug")) {
					logger.debugEnabled = true;
					logger.infoEnabled = true;
					logger.warnEnabled = true;
				} else if (logger.hbaseAlertLevel.equalsIgnoreCase("info")) {
					logger.infoEnabled = true;
					logger.warnEnabled = true;
				} else if (logger.hbaseAlertLevel.equalsIgnoreCase("warn")) {
					logger.warnEnabled = true;
				}
			}
			System.out
					.println("hbaseTableName=" + logger.hbaseTableName + ", hbaseAlertLevel=" + logger.hbaseAlertLevel);
			context.close();
		}
		return logger;
	}

	private boolean isDebugEnabled() {
		return debugEnabled;
	}

	private boolean isInfoEnabled() {
		return infoEnabled;
	}

	private boolean isWarnEnabled() {
		return warnEnabled;
	}

	@Override
	public void debug(String source, String shortMessage, String message) {
		if (isDebugEnabled()) {
			logToHBase(source, shortMessage, message, "debug");
		}
	}

	@Override
	public void debug(String source, String shortMessage, String format, Object... o) {
		if (isDebugEnabled()) {
			FormattingTuple ft = MessageFormatter.arrayFormat(format, o);
			logToHBase(source, shortMessage, ft.getMessage(), "debug");
		}
	}

	@Override
	public void info(String source, String shortMessage, String message) {
		if (isInfoEnabled())
			logToHBase(source, shortMessage, message, "info");
	}

	@Override
	public void info(String source, String shortMessage, String format, Object... o) {
		if (isInfoEnabled()) {
			FormattingTuple ft = MessageFormatter.arrayFormat(format, o);
			logToHBase(source, shortMessage, ft.getMessage(), "info");
		}
	}

	@Override
	public void warn(String source, String shortMessage, String message) {
		if (isWarnEnabled()) {
			logToHBase(source, shortMessage, message, "warn");
		}
	}

	@Override
	public void warn(String source, String shortMessage, String format, Object... o) {
		if (isWarnEnabled()) {
			FormattingTuple ft = MessageFormatter.arrayFormat(format, o);
			logToHBase(source, shortMessage, ft.getMessage(), "warn");
		}
	}

	@Override
	public void warn(String source, String shortMessage, String message, Throwable t) {
		if (isWarnEnabled()) {
			logToHBase(source, shortMessage, message, "warn", t);
		}
	}

	@Override
	public void alert(String source, ALERT_TYPE alertType, ALERT_CAUSE alertCause, ALERT_SEVERITY alertSeverity,
			String message) {
		AlertMessage alertMessage = new AlertMessage();
		alertMessage.setAdaptorName(source);
		alertMessage.setCause(alertCause);
		alertMessage.setMessage(message);
		alertMessage.setMessageContext("");
		alertMessage.setSeverity(alertSeverity);
		alertMessage.setType(alertType);
		logToHBase(alertMessage, "error");
	}

	@Override
	public void alert(String source, ALERT_TYPE alertType, ALERT_CAUSE alertCause, ALERT_SEVERITY alertSeverity,
			String message, Throwable t) {
		AlertMessage alertMessage = new AlertMessage();
		alertMessage.setAdaptorName(source);
		alertMessage.setCause(alertCause);
		alertMessage.setMessage(message);
		alertMessage.setMessageContext("");
		alertMessage.setSeverity(alertSeverity);
		alertMessage.setType(alertType);
		logToHBase(alertMessage, "error", t);
	}

	@Override
	public void alert(String source, ALERT_TYPE alertType, ALERT_CAUSE alertCause, ALERT_SEVERITY alertSeverity,
			String format, Object... o) {

		AlertMessage alertMessage = new AlertMessage();
		alertMessage.setAdaptorName(source);
		alertMessage.setCause(alertCause);
		FormattingTuple ft = MessageFormatter.arrayFormat(format, o);
		alertMessage.setMessage(ft.getMessage());
		alertMessage.setMessageContext(EMPTYSTRING);
		alertMessage.setSeverity(alertSeverity);
		alertMessage.setType(alertType);
		logToHBase(alertMessage, "error");
	}

	@Override
	public void alert(final AlertMessage message) {
		alert(message.getAdaptorName(), message.getType(), message.getCause(), message.getSeverity(),
				message.getMessage());
	}

	private byte[] toBytes(String message) {
		// if (message == null)
		// return Bytes.toBytes(EMPTYSTRING);
		// else
		return Bytes.toBytes(message);
	}

	private void addToPut(Put put, byte[] q, String value) {
		if (value != null)
			put.add(ALERT_COLUMN_FAMILY_NAME, q, toBytes(value));
	}

	private void addToPut(Put put, byte[] q, Throwable t) {
		if (t != null) {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			PrintStream ps = new PrintStream(baos);
			t.printStackTrace(ps);
			put.add(ALERT_COLUMN_FAMILY_NAME, q, baos.toByteArray());
		}
	}

	private void logToHBase(final String source, final String shortMessage, final String message, final String level) {
		logToHBase(source, shortMessage, message, level, null);
	}

	private byte[] createRowKey(final String source) {
		return toBytes(source + "." + System.currentTimeMillis());
	}

	private void logToHBase(final String source, final String shortMessage, final String message, final String level,
			final Throwable t) {
		new Thread() { // TODO use task
			public void run() {
				Put put = new Put(createRowKey(source));
				addToPut(put, ALERT_ADAPTOR_NAME_COLUMN, source);
				addToPut(put, ALERT_MESSAGE_CONTEXT_COLUMN, shortMessage);
				addToPut(put, ALERT_ALERT_MESSAGE_COLUMN, message);
				addToPut(put, ALERT_ALERT_DATE_COLUMN, new Date().toString());
				addToPut(put, ALERT_ALERT_EXCEPTION_COLUMN, t);
				addToPut(put, LOG_LEVEL, level);

				DataInsertionSpecification.Builder dataInsertionSpecificationBuilder = new DataInsertionSpecification.Builder();
				DataInsertionSpecification dataInsertionSpecification = dataInsertionSpecificationBuilder
						.withTableName(hbaseTableName).withtPut(put).build();
				try {
					hbaseManager.insertData(dataInsertionSpecification);
				} catch (IOException | HBaseClientException e) {
					e.printStackTrace();
				}
			}
		}.start();
	}

	private void logToHBase(final AlertMessage message, final String level) {
		logToHBase(message, level, null);
	}

	private void logToHBase(final AlertMessage message, final String level, final Throwable t) {
		new Thread() { // TODO use task
			public void run() {
				Put put = new Put(createRowKey(message.getAdaptorName()));
				addToPut(put, ALERT_ADAPTOR_NAME_COLUMN, message.getAdaptorName());
				addToPut(put, ALERT_MESSAGE_CONTEXT_COLUMN, message.getMessageContext());
				if (message.getType() != null) {
					addToPut(put, ALERT_ALERT_CODE_COLUMN, message.getType().getMessageCode());
					addToPut(put, ALERT_ALERT_NAME_COLUMN, message.getType().getDescription());
				}
				if (message.getCause() != null) {
					addToPut(put, ALERT_ALERT_CAUSE_COLUMN, message.getCause().getDescription());
				}
				if (message.getSeverity() != null) {
					addToPut(put, ALERT_ALERT_SEVERITY_COLUMN, message.getSeverity().toString());
				}
				addToPut(put, ALERT_ALERT_MESSAGE_COLUMN, message.getMessage());
				addToPut(put, ALERT_ALERT_DATE_COLUMN, new Date().toString());
				addToPut(put, ALERT_ALERT_EXCEPTION_COLUMN, t);
				addToPut(put, LOG_LEVEL, "error");

				DataInsertionSpecification.Builder dataInsertionSpecificationBuilder = new DataInsertionSpecification.Builder();
				DataInsertionSpecification dataInsertionSpecification = dataInsertionSpecificationBuilder
						.withTableName(hbaseTableName).withtPut(put).build();
				try {
					hbaseManager.insertData(dataInsertionSpecification);
				} catch (IOException | HBaseClientException e) {
					e.printStackTrace();// what else to do
				}
			}
		}.start();

	}
}
