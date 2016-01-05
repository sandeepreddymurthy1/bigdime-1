/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core.config;

import org.testng.Assert;
import org.testng.annotations.Test;

public class SinkConfigTest {

	/**
	 * Assert that equals method returns true when compared against the same
	 * object.
	 */
	@Test
	public void testEqualsWithSameObject() {
		SinkConfig sinkConfig = new SinkConfig();
		sinkConfig.setName("unit-sink-config-name");
		sinkConfig.setDescription("unit-sink-config-description");
		Assert.assertTrue(sinkConfig.equals(sinkConfig));
	}

	/**
	 * Assert that equals method returns true when both objects have the same
	 * name.
	 */
	@Test
	public void testEqualsAndHashCodeWithSameName() {
		SinkConfig sinkConfig = new SinkConfig();
		sinkConfig.setName("unit-sink-config-name");
		sinkConfig.setDescription("unit-sink-config-description");

		SinkConfig sinkConfig1 = new SinkConfig();
		sinkConfig1.setName("unit-sink-config-name");
		sinkConfig1.setDescription("unit-sink-config-description-1");
		Assert.assertTrue(sinkConfig.equals(sinkConfig1));
		Assert.assertTrue(sinkConfig.hashCode() == sinkConfig1.hashCode());
	}

	/**
	 * Assert that equals method returns false when both objects have a
	 * different name.
	 */
	@Test
	public void testEqualsWithDifferentName() {
		SinkConfig sinkConfig = new SinkConfig();
		sinkConfig.setName("unit-sink-config-name-1");
		sinkConfig.setDescription("unit-sink-config-description");

		SinkConfig sinkConfig1 = new SinkConfig();
		sinkConfig1.setName("unit-sink-config-name");
		sinkConfig1.setDescription("unit-sink-config-description");
		Assert.assertFalse(sinkConfig.equals(sinkConfig1));
	}

	/**
	 * Assert that equals method returns false when compared against null.
	 */
	@Test
	public void testEqualsWithNull() {
		SinkConfig sinkConfig = new SinkConfig();
		sinkConfig.setName("unit-sink-config-name");
		sinkConfig.setDescription("unit-sink-config-description");
		Assert.assertFalse(sinkConfig.equals(null));
	}

	/**
	 * Assert that equals method returns false when compared against an object
	 * of different type.
	 */
	@Test
	public void testEqualsWithDifferentObjectType() {
		SinkConfig sinkConfig = new SinkConfig();
		sinkConfig.setName("unit-sink-config-name");
		sinkConfig.setDescription("unit-sink-config-description");
		Assert.assertFalse(sinkConfig.equals(new Object()));
	}

	/**
	 * Assert that equals method returns false when the calling object's name is
	 * null while other object's name is not null.
	 */
	@Test
	public void testEqualsWithNameAsNullInThis() {
		SinkConfig sinkConfig = new SinkConfig();
		sinkConfig.setName(null);
		sinkConfig.setDescription("unit-sink-config-description");

		SinkConfig sinkConfig1 = new SinkConfig();
		sinkConfig1.setName("unit-sink-config-name");
		sinkConfig1.setDescription("unit-sink-config-description-1");
		Assert.assertFalse(sinkConfig.equals(sinkConfig1));
	}

	/**
	 * Assert that equals method returns true when both object's name is null.
	 */
	@Test
	public void testEqualsHashCodeWithNameAsNullInBothObjects() {
		SinkConfig sinkConfig = new SinkConfig();
		sinkConfig.setName(null);
		sinkConfig.setDescription("unit-sink-config-description");

		SinkConfig sinkConfig1 = new SinkConfig();
		sinkConfig1.setName(null);
		sinkConfig1.setDescription("unit-sink-config-description-1");
		Assert.assertTrue(sinkConfig.equals(sinkConfig1));
		Assert.assertTrue(sinkConfig.hashCode() == sinkConfig1.hashCode());
	}

}
