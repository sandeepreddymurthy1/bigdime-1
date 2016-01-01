/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core.commons;

import org.mockito.Mockito;
import org.testng.annotations.Test;

import io.bigdime.alert.AlertMessage;
import io.bigdime.alert.Logger;

public class AdaptorLoggerTest {

	@Test
	public void testWarn() {
		Logger logger = Mockito.mock(Logger.class);
		AdaptorLogger adaptorLogger = new AdaptorLogger(logger);
		adaptorLogger.warn("unit-shortMessage", "unit-longMessage", new Throwable());
		Mockito.verify(logger, Mockito.times(1)).warn(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(),
				Mockito.any(Throwable.class));
	}

	@Test
	public void testAlert() {
		Logger logger = Mockito.mock(Logger.class);
		AdaptorLogger adaptorLogger = new AdaptorLogger(logger);
		AlertMessage alertMessage = new AlertMessage();
		adaptorLogger.alert(alertMessage);
		Mockito.verify(logger, Mockito.times(1)).alert(alertMessage);
	}

}
