/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.libs.hive.database;

import io.bigdime.libs.hive.database.DatabaseSpecification;
import io.bigdime.libs.hive.database.DatabaseSpecification.Builder;

import org.testng.Assert;
import org.testng.annotations.Test;

public class DatabaseSpecificationTest {
	@Test
	public void testSpecficationBuild(){
		DatabaseSpecification.Builder databaseSpecificationBuilder = new DatabaseSpecification.Builder("mockdb");
		DatabaseSpecification  databaseSpecification = databaseSpecificationBuilder
											.host("localhost")
											.location("/temp")
											.scheme("hdfs://")
											.comment("testcomment")
											.build();

		Assert.assertEquals(databaseSpecification.location, "/temp");
		Assert.assertEquals(databaseSpecification.databaseName, "mockdb");
		Assert.assertEquals(databaseSpecification.host, "localhost");
		Assert.assertEquals(databaseSpecification.scheme, "hdfs://");
		Assert.assertEquals(databaseSpecification.comment, "testcomment");
	}
}
