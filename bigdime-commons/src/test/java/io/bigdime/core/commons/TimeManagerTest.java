/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core.commons;

import org.joda.time.DateTime;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TimeManagerTest {
	@Test
	public void testTimeManagerUtils(){
		DateTime utcTime = TimeManager.getInstance().getCurrentUTCDate();
		DateTime locaTime = TimeManager.getInstance().getLocalDateTime();
		Assert.assertNotNull(utcTime);
		Assert.assertNotNull(locaTime);
		String  dateFormat = TimeManager.getInstance().format(TimeManager.FORMAT_YYYYMMDD, locaTime);
		Assert.assertNotNull(dateFormat);
	}
}
