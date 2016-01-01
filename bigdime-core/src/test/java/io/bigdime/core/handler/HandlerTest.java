/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core.handler;

import org.testng.Assert;
import org.testng.annotations.Test;

import io.bigdime.core.Handler;

public class HandlerTest {

	@Test
	public void testHandler() {
		Assert.assertEquals(Handler.State.INIT.getValue(), Integer.valueOf(1));
		Assert.assertEquals(Handler.State.RUNNING.getValue(), Integer.valueOf(3));
		Assert.assertEquals(Handler.State.TERMINATED.getValue(), Integer.valueOf(10));
		Assert.assertNotNull(Handler.State.values());
		Assert.assertNotNull(Handler.State.valueOf("INIT"));
		Assert.assertNotNull(Handler.State.valueOf("RUNNING"));
		Assert.assertNotNull(Handler.State.valueOf("TERMINATED"));
	}

}
