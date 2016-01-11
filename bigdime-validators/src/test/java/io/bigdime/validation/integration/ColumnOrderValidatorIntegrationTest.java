/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.validation.integration;

import io.bigdime.core.ActionEvent;
import io.bigdime.core.config.AdaptorConfig;
import io.bigdime.core.constants.ActionEventHeaderConstants;
import io.bigdime.core.validation.DataValidationException;
import io.bigdime.core.validation.ValidationResponse.ValidationResult;
import io.bigdime.validation.ColumnOrderValidator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

@ContextConfiguration(locations = "classpath:META-INF/application-context.xml")
public class ColumnOrderValidatorIntegrationTest extends AbstractTestNGSpringContextTests{
	
	@Autowired
	ColumnOrderValidator columnOrderValidator;
	
	ActionEvent actionEvent = new ActionEvent();
	
	private static final Logger logger = LoggerFactory.getLogger(ColumnOrderValidatorIntegrationTest.class);
	
	@BeforeTest
	public void setup() {
		logger.info("Setting the environment");
		AdaptorConfig.getInstance().getAdaptorContext().setAdaptorName("UnitAdaptor");
	}
	
	@Test(priority = 18, expectedExceptions = IllegalArgumentException.class)
	public void testNullHiveHostName() throws DataValidationException{
	   	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_HOST_NAME, "");
	   	columnOrderValidator.validate(actionEvent);
	}
	
	@Test(priority = 19, expectedExceptions = IllegalArgumentException.class)
	public void testNullHivePortName() throws DataValidationException{
		actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_HOST_NAME, "sandbox.hortonworks.com");
	   	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_PORT, "");
	   	columnOrderValidator.validate(actionEvent);
	}
	
	@Test(priority = 20, expectedExceptions = IllegalArgumentException.class)
	public void testHivePortParseToInt() throws DataValidationException{
		actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_HOST_NAME, "sandbox.hortonworks.com");
	   	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_PORT, "port");
	   	columnOrderValidator.validate(actionEvent);
	}
		
	@Test(priority = 21, expectedExceptions = IllegalArgumentException.class)
	public void testNullHiveDBName() throws DataValidationException{
		actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_HOST_NAME, "sandbox.hortonworks.com");
	   	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_PORT, "9083");
	   	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_DB_NAME, "");
	   	columnOrderValidator.validate(actionEvent);
	}
	
	@Test(priority = 22, expectedExceptions = IllegalArgumentException.class)
	public void testNullHiveTableName() throws DataValidationException{
		actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_HOST_NAME, "sandbox.hortonworks.com");
	   	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_PORT, "9083");
	   	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_DB_NAME, "test");
	   	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_TABLE_NAME, "");
	   	columnOrderValidator.validate(actionEvent);
	}
	
	@Test(priority = 23, expectedExceptions = DataValidationException.class)
	public void testHiveTableNotCreated() throws DataValidationException{
		actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_HOST_NAME, "sandbox.hortonworks.com");
	   	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_PORT, "9083");
	   	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_DB_NAME, "test");
	   	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_TABLE_NAME, "tests");
	   	Assert.assertEquals(columnOrderValidator.validate(actionEvent).getValidationResult(), ValidationResult.INCOMPLETE_SETUP);		
	}
	
	@Test(priority = 24)
	public void testNullMetadataEntitee() throws DataValidationException{
		actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_HOST_NAME, "sandbox.hortonworks.com");
	   	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_PORT, "9083");
	   	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_DB_NAME, "test");
	   	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_TABLE_NAME, "sample_test");
	   	Assert.assertEquals(columnOrderValidator.validate(actionEvent).getValidationResult(), ValidationResult.INCOMPLETE_SETUP);		
	}
	
	@Test(priority = 25)
	public void testColumnOrderMatch() throws DataValidationException{
		actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_HOST_NAME, "sandbox.hortonworks.com");
	   	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_PORT, "9083");
	   	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_DB_NAME, "test");
	   	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_TABLE_NAME, "ENTITYNAME_1");
	   	Assert.assertEquals(columnOrderValidator.validate(actionEvent).getValidationResult(), ValidationResult.PASSED);
	}
	
	@Test(priority = 26)
	public void testColumnOrderMismatch() throws DataValidationException{
		actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_HOST_NAME, "sandbox.hortonworks.com");
	   	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_PORT, "9083");
	   	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_DB_NAME, "test");
	   	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_TABLE_NAME, "ENTITYNAME_2");
	   	Assert.assertEquals(columnOrderValidator.validate(actionEvent).getValidationResult(), ValidationResult.FAILED);
	}
	
	@Test(priority = 27)
	public void testSettersAndGetters(){
		columnOrderValidator.setName("testName");
		Assert.assertEquals(columnOrderValidator.getName(), "testName");
	}
	
//	@AfterTest
//	public void cleanup() throws HCatException{
//		hiveTableManager.dropTable("test","ENTITYNAME_1");
//		hiveTableManager.dropTable("test","ENTITYNAME_2");
//		hiveTableManager.dropTable("test","ENTITYNAME_3");
//	}
}
