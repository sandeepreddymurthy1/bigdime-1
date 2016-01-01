/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core.config;

import org.testng.Assert;
import org.testng.annotations.Test;

public class ChannelConfigTest {

	@Test
	public void testBean() {
		ChannelConfig channelConfig = new ChannelConfig();
		channelConfig.setName("unit-channel-config-name");
		channelConfig.setDescription("unit-channel-config-description");
		Assert.assertEquals(channelConfig.getName(), "unit-channel-config-name");
		Assert.assertEquals(channelConfig.getDescription(), "unit-channel-config-description");
	}

	/**
	 * Assert that equals method returns true when compared against the same
	 * object.
	 */
	@Test
	public void testEqualsWithSameObject() {
		ChannelConfig channelConfig = new ChannelConfig();
		channelConfig.setName("unit-channel-config-name");
		channelConfig.setDescription("unit-channel-config-description");
		Assert.assertTrue(channelConfig.equals(channelConfig));
	}

	/**
	 * Assert that equals method returns true when both objects have the same
	 * name.
	 */
	@Test
	public void testEqualsAndHashCodeWithSameName() {
		ChannelConfig channelConfig = new ChannelConfig();
		channelConfig.setName("unit-channel-config-name");
		channelConfig.setDescription("unit-channel-config-description");

		ChannelConfig channelConfig1 = new ChannelConfig();
		channelConfig1.setName("unit-channel-config-name");
		channelConfig1.setDescription("unit-channel-config-description-1");
		Assert.assertTrue(channelConfig.equals(channelConfig1));
		Assert.assertTrue(channelConfig.hashCode() == channelConfig1.hashCode());
	}

	/**
	 * Assert that equals method returns false when both objects have a
	 * different name.
	 */
	@Test
	public void testEqualsWithDifferentName() {
		ChannelConfig channelConfig = new ChannelConfig();
		channelConfig.setName("unit-channel-config-name-1");
		channelConfig.setDescription("unit-channel-config-description");

		ChannelConfig channelConfig1 = new ChannelConfig();
		channelConfig1.setName("unit-channel-config-name");
		channelConfig1.setDescription("unit-channel-config-description");
		Assert.assertFalse(channelConfig.equals(channelConfig1));
	}

	/**
	 * Assert that equals method returns false when compared against null.
	 */
	@Test
	public void testEqualsWithNull() {
		ChannelConfig channelConfig = new ChannelConfig();
		channelConfig.setName("unit-channel-config-name");
		channelConfig.setDescription("unit-channel-config-description");
		Assert.assertFalse(channelConfig.equals(null));
	}

	/**
	 * Assert that equals method returns false when compared against an object
	 * of different type.
	 */
	@Test
	public void testEqualsWithDifferentObjectType() {
		ChannelConfig channelConfig = new ChannelConfig();
		channelConfig.setName("unit-channel-config-name");
		channelConfig.setDescription("unit-channel-config-description");
		Assert.assertFalse(channelConfig.equals(new Object()));
	}

	/**
	 * Assert that equals method returns false when the calling object's name is
	 * null while other object's name is not null.
	 */
	@Test
	public void testEqualsWithNameAsNullInThis() {
		ChannelConfig channelConfig = new ChannelConfig();
		channelConfig.setName(null);
		channelConfig.setDescription("unit-channel-config-description");

		ChannelConfig channelConfig1 = new ChannelConfig();
		channelConfig1.setName("unit-channel-config-name");
		channelConfig1.setDescription("unit-channel-config-description-1");
		Assert.assertFalse(channelConfig.equals(channelConfig1));
	}

	/**
	 * Assert that equals method returns true when both object's name is null.
	 */
	@Test
	public void testEqualsHashCodeWithNameAsNullInBothObjects() {
		ChannelConfig channelConfig = new ChannelConfig();
		channelConfig.setName(null);
		channelConfig.setDescription("unit-channel-config-description");

		ChannelConfig channelConfig1 = new ChannelConfig();
		channelConfig1.setName(null);
		channelConfig1.setDescription("unit-channel-config-description-1");
		Assert.assertTrue(channelConfig.equals(channelConfig1));
		Assert.assertTrue(channelConfig.hashCode() == channelConfig1.hashCode());
	}

}
