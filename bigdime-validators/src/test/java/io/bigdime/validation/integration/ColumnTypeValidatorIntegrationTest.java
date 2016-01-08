package io.bigdime.validation.integration;

import io.bigdime.core.ActionEvent;
import io.bigdime.core.config.AdaptorConfig;
import io.bigdime.core.constants.ActionEventHeaderConstants;
import io.bigdime.core.validation.DataValidationException;
import io.bigdime.core.validation.ValidationResponse.ValidationResult;
import io.bigdime.validation.ColumnTypeValidator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

@ContextConfiguration(locations = "classpath:META-INF/application-context.xml")
public class ColumnTypeValidatorIntegrationTest extends AbstractTestNGSpringContextTests  {
	
	@Autowired
	ColumnTypeValidator columnTypeValidator;
	
	ActionEvent actionEvent = new ActionEvent();
	
	private static final Logger logger = LoggerFactory.getLogger(ColumnTypeValidatorIntegrationTest.class);
	
	@BeforeTest
	public void setup() {
		logger.info("Setting the environment");
		AdaptorConfig.getInstance().getAdaptorContext().setAdaptorName("UnitAdaptor");
	}
	
	@Test(priority = 28, expectedExceptions = IllegalArgumentException.class)
	public void testNullHiveHostName() throws DataValidationException{
	   	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_HOST_NAME, "");
	   	columnTypeValidator.validate(actionEvent);
	}
	
	@Test(priority = 29, expectedExceptions = IllegalArgumentException.class)
	public void testNullHivePortName() throws DataValidationException{
		actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_HOST_NAME, "sandbox.hortonworks.com");
	   	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_PORT, "");
	   	columnTypeValidator.validate(actionEvent);
	}
	
	@Test(priority = 30, expectedExceptions = IllegalArgumentException.class)
	public void testHivePortParseToInt() throws DataValidationException{
		actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_HOST_NAME, "sandbox.hortonworks.com");
	   	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_PORT, "port");
	   	columnTypeValidator.validate(actionEvent);
	}
		
	@Test(priority = 31, expectedExceptions = IllegalArgumentException.class)
	public void testNullHiveDBName() throws DataValidationException{
		actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_HOST_NAME, "sandbox.hortonworks.com");
	   	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_PORT, "9083");
	   	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_DB_NAME, "");
	   	columnTypeValidator.validate(actionEvent);
	}
	
	@Test(priority = 32, expectedExceptions = IllegalArgumentException.class)
	public void testNullHiveTableName() throws DataValidationException{
		actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_HOST_NAME, "sandbox.hortonworks.com");
	   	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_PORT, "9083");
	   	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_DB_NAME, "test");
	   	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_TABLE_NAME, "");
	   	columnTypeValidator.validate(actionEvent);
	}
	
	@Test(priority = 33)
	public void testHiveTableNotCreated() throws DataValidationException{
		actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_HOST_NAME, "sandbox.hortonworks.com");
	   	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_PORT, "9083");
	   	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_DB_NAME, "test");
	   	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_TABLE_NAME, "tests");
	   	Assert.assertEquals(columnTypeValidator.validate(actionEvent).getValidationResult(), ValidationResult.INCOMPLETE_SETUP);		
	}
	
	@Test(priority = 34)
	public void testNullMetadataEntitee() throws DataValidationException{
		actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_HOST_NAME, "sandbox.hortonworks.com");
	   	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_PORT, "9083");
	   	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_DB_NAME, "test");
	   	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_TABLE_NAME, "sample_test");
	   	Assert.assertEquals(columnTypeValidator.validate(actionEvent).getValidationResult(), ValidationResult.INCOMPLETE_SETUP);		
	}
	
	@Test(priority = 35)
	public void testColumnTypeMatch() throws DataValidationException{
		actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_HOST_NAME, "sandbox.hortonworks.com");
	   	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_PORT, "9083");
	   	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_DB_NAME, "test");
	   	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_TABLE_NAME, "ENTITYNAME_1");
	   	Assert.assertEquals(columnTypeValidator.validate(actionEvent).getValidationResult(), ValidationResult.PASSED);
	}
	
	@Test(priority = 36)
	public void testColumnTypeMismatchHiveColumnMore() throws DataValidationException{
		actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_HOST_NAME, "sandbox.hortonworks.com");
	   	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_PORT, "9083");
	   	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_DB_NAME, "test");
	   	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_TABLE_NAME, "ENTITYNAME_2");
	   	Assert.assertEquals(columnTypeValidator.validate(actionEvent).getValidationResult(), ValidationResult.COLUMN_TYPE_MISMATCH);
	}
	
	@Test(priority = 37)
	public void testColumnTypeMismatchSourceColumnMore() throws DataValidationException{
		actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_HOST_NAME, "sandbox.hortonworks.com");
	   	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_PORT, "9083");
	   	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_DB_NAME, "test");
	   	actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_TABLE_NAME, "ENTITYNAME_3");
	   	Assert.assertEquals(columnTypeValidator.validate(actionEvent).getValidationResult(), ValidationResult.COLUMN_TYPE_MISMATCH);
	}
	
	@Test(priority = 38)
	public void testSettersAndGetters(){
		columnTypeValidator.setName("testName");
		Assert.assertEquals(columnTypeValidator.getName(), "testName");
	}
	
//	@AfterTest
//	public void cleanup() throws HCatException{
//		hiveTableManager.dropTable("test","ENTITYNAME_1");
//		hiveTableManager.dropTable("test","ENTITYNAME_2");
//		hiveTableManager.dropTable("test","ENTITYNAME_3");
//	}
}
