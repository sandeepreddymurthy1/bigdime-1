/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigidme.hbase.test.client;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import io.bigdime.alert.Logger;
import io.bigdime.alert.LoggerFactory;
import io.bigdime.hbase.client.DataDeletionSpecification;
import io.bigdime.hbase.client.DataDeletionSpecification.Builder;

import org.apache.hadoop.hbase.client.Delete;
import org.powermock.modules.testng.PowerMockTestCase;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import static io.bigdime.constants.TestConstants.TEST;
/**
 * 
 * @author Sandeep Reddy,Murthy
 *
 */
public class DataDeletionSpecificationTest extends PowerMockTestCase{
	private static final Logger logger = LoggerFactory.getLogger(DataDeletionSpecificationTest.class);
	
	DataDeletionSpecification dataDeletionSpecification;
	DataDeletionSpecification.Builder dataDeletionSpecificationBuilder;
	
	@BeforeClass
	 public void init()		{
		dataDeletionSpecificationBuilder=new DataDeletionSpecification.Builder();
	}
	@BeforeTest
	public void setup() {
		System.setProperty("env",TEST);	
	}
	@Test
	public void builderDeleteTest(){
		Delete delete=new Delete(TEST.getBytes(StandardCharsets.UTF_8));
		delete.setAttribute(TEST, TEST.getBytes(StandardCharsets.UTF_8));
		List<Delete> list=new ArrayList<Delete>();
		list.add(delete);
		dataDeletionSpecification=dataDeletionSpecificationBuilder.withTableName(TEST).withDeletes(list).build();
		Assert.assertEquals(dataDeletionSpecification.getTableName(), TEST);
		for(Delete deletedItems:dataDeletionSpecification.getDeletes()){
			Assert.assertEquals(new String(deletedItems.getAttribute(TEST),StandardCharsets.UTF_8), TEST);
		}
	}
	
}
