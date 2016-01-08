/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.handler.file;

import org.testng.Assert;
import org.testng.annotations.Test;

public class FileInputStreamReaderHandlerConstantsTest {

	@Test
	public void testGetInstance() {
		Assert.assertNotNull(FileInputStreamReaderHandlerConstants.getInstance());

	}
}
