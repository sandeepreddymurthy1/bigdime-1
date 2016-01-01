/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core.adaptor;

import org.testng.Assert;
import org.testng.annotations.Test;

import io.bigdime.core.config.ADAPTOR_TYPE;

public class AdaptorTest {

	/**
	 * Test getByValue method of ADAPTOR_TYPE enum.
	 */
	@Test
	public void testAdaptorTypeGetByValue() {
		Assert.assertEquals(ADAPTOR_TYPE.getByValue("batch"), ADAPTOR_TYPE.BATCH,
				"getByValue should return \"batch\" for ADAPTOR_TYPE.BATCH");
		Assert.assertEquals(ADAPTOR_TYPE.getByValue("streaming"), ADAPTOR_TYPE.STREAMING,
				"getByValue should return \"streaming\" for ADAPTOR_TYPE.STREAMING");

		Assert.assertNull(ADAPTOR_TYPE.getByValue(null));
		Assert.assertNull(ADAPTOR_TYPE.getByValue("unit-test"));
		ADAPTOR_TYPE.BATCH.name();
	}

	/**
	 * Test valueOf of ADAPTOR_TYPE enum.
	 */
	@Test
	public void testAdaptorTypeValueOf() {
		Assert.assertEquals(ADAPTOR_TYPE.valueOf("BATCH"), ADAPTOR_TYPE.BATCH,
				"valueOf should return \"batch\" for ADAPTOR_TYPE.BATCH");
		Assert.assertEquals(ADAPTOR_TYPE.valueOf("STREAMING"), ADAPTOR_TYPE.STREAMING,
				"valueOf should return \"streaming\" for ADAPTOR_TYPE.STREAMING");
	}
}
