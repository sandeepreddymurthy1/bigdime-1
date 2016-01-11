/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.validation.integration;


import io.bigdime.core.ActionEvent;
import io.bigdime.core.config.AdaptorConfig;
import io.bigdime.core.constants.ActionEventHeaderConstants;
import io.bigdime.core.validation.DataValidationException;
import io.bigdime.core.validation.ValidationResponse.ValidationResult;
import io.bigdime.validation.ColumnCountValidator;

//import org.apache.hive.hcatalog.common.HCatException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

@ContextConfiguration(locations = "classpath:META-INF/application-context.xml")
public class ColumnCountValidatorIntegrationTest extends AbstractTestNGSpringContextTests {
	
	@Autowired
	ColumnCountValidator columnCountValidator;
	
	ActionEvent actionEvent = new ActionEvent();
	
	private static final Logger logger = LoggerFactory.getLogger(ColumnCountValidatorIntegrationTest.class);
	
	@BeforeTest
	public void setup() {
		logger.info("Setting the environment");
		AdaptorConfig.getInstance().getAdaptorContext().setAdaptorName("UnitAdaptor");
	}
	
	@Test(priority = 7, expectedExceptions = IllegalArgumentException.class)
	public void testNullHiveHostName() throws DataValidationException{
	   	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_HOST_NAME, "");
	   	columnCountValidator.validate(actionEvent);
	}
	
	@Test(priority = 8, expectedExceptions = IllegalArgumentException.class)
	public void testNullHivePortName() throws DataValidationException{
		actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_HOST_NAME, "sandbox.hortonworks.com");
	   	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_PORT, "");
	   	columnCountValidator.validate(actionEvent);
	}
	
	@Test(priority = 9, expectedExceptions = IllegalArgumentException.class)
	public void testHivePortParseToInt() throws DataValidationException{
		actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_HOST_NAME, "sandbox.hortonworks.com");
	   	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_PORT, "port");
	   	columnCountValidator.validate(actionEvent);
	}
		
	@Test(priority = 10, expectedExceptions = IllegalArgumentException.class)
	public void testNullHiveDBName() throws DataValidationException{
		actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_HOST_NAME, "sandbox.hortonworks.com");
	   	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_PORT, "9083");
	   	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_DB_NAME, "");
	   	columnCountValidator.validate(actionEvent);
	}
	
	@Test(priority = 11, expectedExceptions = IllegalArgumentException.class)
	public void testNullHiveTableName() throws DataValidationException{
		actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_HOST_NAME, "sandbox.hortonworks.com");
	   	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_PORT, "9083");
	   	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_DB_NAME, "test");
	   	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_TABLE_NAME, "");
	   	columnCountValidator.validate(actionEvent);
	}
	
	@Test(priority = 12, expectedExceptions = DataValidationException.class)
	public void testHiveTableNotCreated() throws DataValidationException{
		actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_HOST_NAME, "sandbox.hortonworks.com");
	   	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_PORT, "9083");
	   	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_DB_NAME, "test");
	   	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_TABLE_NAME, "testTable");
	   	Assert.assertEquals(columnCountValidator.validate(actionEvent).getValidationResult(), ValidationResult.INCOMPLETE_SETUP);		
	}
	
	@Test(priority = 13)
	public void testNullMetadataEntitee() throws DataValidationException{
		actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_HOST_NAME, "sandbox.hortonworks.com");
	   	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_PORT, "9083");
	   	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_DB_NAME, "test");
	   	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_TABLE_NAME, "sample_test");
	   	Assert.assertEquals(columnCountValidator.validate(actionEvent).getValidationResult(), ValidationResult.INCOMPLETE_SETUP);		
	}
	
	@Test(priority = 14)
	public void testColumnCountMatch() throws DataValidationException{
		actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_HOST_NAME, "sandbox.hortonworks.com");
	   	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_PORT, "9083");
	   	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_DB_NAME, "test");
	   	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_TABLE_NAME, "ENTITYNAME_1");
	   	Assert.assertEquals(columnCountValidator.validate(actionEvent).getValidationResult(), ValidationResult.PASSED);
	}	
	
	@Test(priority = 15)
	public void testHiveColumnCountMoreThanSource() throws DataValidationException{
		actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_HOST_NAME, "sandbox.hortonworks.com");
	   	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_PORT, "9083");
	   	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_DB_NAME, "test");
	   	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_TABLE_NAME, "ENTITYNAME_2");
	   	Assert.assertEquals(columnCountValidator.validate(actionEvent).getValidationResult(), ValidationResult.FAILED);
	}
	
	@Test(priority = 16)
	public void testSourceColumnCountMoreThanHive() throws DataValidationException{
		actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_HOST_NAME, "sandbox.hortonworks.com");
	   	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_PORT, "9083");
	   	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_DB_NAME, "test");
	   	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_TABLE_NAME, "ENTITYNAME_3");
	   	Assert.assertEquals(columnCountValidator.validate(actionEvent).getValidationResult(), ValidationResult.COLUMN_COUNT_MISMATCH);
	}
	
	@Test(priority = 17)
	public void testSettersAndGetters(){
		columnCountValidator.setName("testName");
		Assert.assertEquals(columnCountValidator.getName(), "testName");
	}
	
//	@AfterTest
//	public void cleanup() throws HCatException, MetadataAccessException{
//		hiveTableManager.dropTable("test","ENTITYNAME_1");
//		hiveTableManager.dropTable("test","ENTITYNAME_2");
//		hiveTableManager.dropTable("test","ENTITYNAME_3");
//	}

}
