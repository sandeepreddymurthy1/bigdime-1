/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.alert;

import org.testng.Assert;
import org.testng.annotations.Test;

import io.bigdime.alert.Logger.ALERT_CAUSE;
import io.bigdime.alert.Logger.ALERT_SEVERITY;
import io.bigdime.alert.Logger.ALERT_TYPE;

public class AlertTest {

	@Test
	public void testAlertType() {

		Assert.assertSame(ALERT_TYPE.valueOf("ADAPTOR_FAILED_TO_START"), ALERT_TYPE.ADAPTOR_FAILED_TO_START);
		Assert.assertSame(ALERT_TYPE.valueOf("INGESTION_FAILED"), ALERT_TYPE.INGESTION_FAILED);
		Assert.assertSame(ALERT_TYPE.valueOf("INGESTION_DID_NOT_RUN"), ALERT_TYPE.INGESTION_DID_NOT_RUN);
		Assert.assertSame(ALERT_TYPE.valueOf("INGESTION_STOPPED"), ALERT_TYPE.INGESTION_STOPPED);
		Assert.assertSame(ALERT_TYPE.valueOf("DATA_FORMAT_CHANGED"), ALERT_TYPE.DATA_FORMAT_CHANGED);
		Assert.assertSame(ALERT_TYPE.valueOf("OTHER_ERROR"), ALERT_TYPE.OTHER_ERROR);
	}

	@Test
	public void testAlertCause() {
		Assert.assertSame(ALERT_CAUSE.valueOf("INVALID_ADAPTOR_CONFIGURATION"),
				ALERT_CAUSE.INVALID_ADAPTOR_CONFIGURATION);
		Assert.assertSame(ALERT_CAUSE.valueOf("VALIDATION_ERROR"), ALERT_CAUSE.VALIDATION_ERROR);
		Assert.assertSame(ALERT_CAUSE.valueOf("MESSAGE_TOO_BIG"), ALERT_CAUSE.MESSAGE_TOO_BIG);
		Assert.assertSame(ALERT_CAUSE.valueOf("UNSUPPORTED_DATA_TYPE"), ALERT_CAUSE.UNSUPPORTED_DATA_TYPE);
		Assert.assertSame(ALERT_CAUSE.valueOf("INPUT_DATA_SCHEMA_CHANGED"), ALERT_CAUSE.INPUT_DATA_SCHEMA_CHANGED);
		Assert.assertSame(ALERT_CAUSE.valueOf("INPUT_ERROR"), ALERT_CAUSE.INPUT_ERROR);
		Assert.assertSame(ALERT_CAUSE.valueOf("APPLICATION_INTERNAL_ERROR"), ALERT_CAUSE.APPLICATION_INTERNAL_ERROR);
	}

	@Test
	public void testAlertSeverity() {
		Assert.assertSame(ALERT_SEVERITY.valueOf("BLOCKER"), ALERT_SEVERITY.BLOCKER);
		Assert.assertSame(ALERT_SEVERITY.valueOf("MAJOR"), ALERT_SEVERITY.MAJOR);
		Assert.assertSame(ALERT_SEVERITY.valueOf("NORMAL"), ALERT_SEVERITY.NORMAL);
	}
}
