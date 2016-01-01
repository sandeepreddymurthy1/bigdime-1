/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core.config;

import org.testng.Assert;
import org.testng.annotations.Test;

public class HandlerConfigTest {

	@Test
	public void testBean() {
		HandlerConfig handlerConfig = new HandlerConfig();
		handlerConfig.setName("unit-handler-config-name");
		handlerConfig.setDescription("unit-handler-config-description");
		Assert.assertEquals(handlerConfig.getName(), "unit-handler-config-name");
		Assert.assertEquals(handlerConfig.getDescription(), "unit-handler-config-description");
	}
	
	/**
	 * Assert that equals method returns true when compared against the same
	 * object.
	 */
	@Test
	public void testEqualsWithSameObject() {
		HandlerConfig handlerConfig = new HandlerConfig();
		handlerConfig.setName("unit-handler-config-name");
		handlerConfig.setDescription("unit-handler-config-description");
		Assert.assertTrue(handlerConfig.equals(handlerConfig));
	}

	/**
	 * Assert that equals method returns true when both objects have the same
	 * name.
	 */
	@Test
	public void testEqualsAndHashCodeWithSameName() {
		HandlerConfig handlerConfig = new HandlerConfig();
		handlerConfig.setName("unit-handler-config-name");
		handlerConfig.setDescription("unit-handler-config-description");

		HandlerConfig handlerConfig1 = new HandlerConfig();
		handlerConfig1.setName("unit-handler-config-name");
		handlerConfig1.setDescription("unit-handler-config-description-1");
		Assert.assertTrue(handlerConfig.equals(handlerConfig1));
		Assert.assertTrue(handlerConfig.hashCode() == handlerConfig1.hashCode());
	}

	/**
	 * Assert that equals method returns false when both objects have a
	 * different name.
	 */
	@Test
	public void testEqualsWithDifferentName() {
		HandlerConfig handlerConfig = new HandlerConfig();
		handlerConfig.setName("unit-handler-config-name-1");
		handlerConfig.setDescription("unit-handler-config-description");

		HandlerConfig handlerConfig1 = new HandlerConfig();
		handlerConfig1.setName("unit-handler-config-name");
		handlerConfig1.setDescription("unit-handler-config-description");
		Assert.assertFalse(handlerConfig.equals(handlerConfig1));
	}

	/**
	 * Assert that equals method returns false when compared against null.
	 */
	@Test
	public void testEqualsWithNull() {
		HandlerConfig handlerConfig = new HandlerConfig();
		handlerConfig.setName("unit-handler-config-name");
		handlerConfig.setDescription("unit-handler-config-description");
		Assert.assertFalse(handlerConfig.equals(null));
	}

	/**
	 * Assert that equals method returns false when compared against an object
	 * of different type.
	 */
	@Test
	public void testEqualsWithDifferentObjectType() {
		HandlerConfig handlerConfig = new HandlerConfig();
		handlerConfig.setName("unit-handler-config-name");
		handlerConfig.setDescription("unit-handler-config-description");
		Assert.assertFalse(handlerConfig.equals(new Object()));
	}

	/**
	 * Assert that equals method returns false when the calling object's name is
	 * null while other object's name is not null.
	 */
	@Test
	public void testEqualsWithNameAsNullInThis() {
		HandlerConfig handlerConfig = new HandlerConfig();
		handlerConfig.setName(null);
		handlerConfig.setDescription("unit-handler-config-description");

		HandlerConfig handlerConfig1 = new HandlerConfig();
		handlerConfig1.setName("unit-handler-config-name");
		handlerConfig1.setDescription("unit-handler-config-description-1");
		Assert.assertFalse(handlerConfig.equals(handlerConfig1));
	}

	/**
	 * Assert that equals method returns true when both object's name is null.
	 */
	@Test
	public void testEqualsHashCodeWithNameAsNullInBothObjects() {
		HandlerConfig handlerConfig = new HandlerConfig();
		handlerConfig.setName(null);
		handlerConfig.setDescription("unit-handler-config-description");

		HandlerConfig handlerConfig1 = new HandlerConfig();
		handlerConfig1.setName(null);
		handlerConfig1.setDescription("unit-handler-config-description-1");
		Assert.assertTrue(handlerConfig.equals(handlerConfig1));
		Assert.assertTrue(handlerConfig.hashCode() == handlerConfig1.hashCode());
	}

}
