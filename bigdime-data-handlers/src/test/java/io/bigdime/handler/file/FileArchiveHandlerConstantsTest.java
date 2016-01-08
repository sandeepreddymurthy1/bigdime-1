/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.handler.file;

import org.testng.Assert;
import org.testng.annotations.Test;

public class FileArchiveHandlerConstantsTest {

	@Test
	public void testGetInstance() {
		Assert.assertNotNull(FileArchiveHandlerConstants.getInstance());
	}
}
