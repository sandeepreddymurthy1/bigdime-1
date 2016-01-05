/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.handler.file;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * 
 * @author Neeraj Jain
 *
 */

public class PartitionNamesParserHandlerConstantsTest {
	@Test
	public void testGetInstance() {
		Assert.assertNotNull(PartitionNamesParserHandlerConstants.getInstance());
	}
}
