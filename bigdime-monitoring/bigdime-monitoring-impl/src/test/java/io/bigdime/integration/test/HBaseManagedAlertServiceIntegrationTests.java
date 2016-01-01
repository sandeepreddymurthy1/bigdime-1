/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.integration.test;

import io.bigdime.alert.AlertException;
import io.bigdime.alert.AlertServiceRequest;
import io.bigdime.alert.AlertServiceResponse;
import io.bigdime.alert.HBaseManagedAlertService;
import io.bigdime.alert.ManagedAlert;
import io.bigdime.alert.ManagedAlertService.ALERT_STATUS;
import io.bigdime.hbase.client.exception.HBaseClientException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;



@ContextConfiguration(locations = {"classpath:context/beans.xml","classpath:hbase-client-context/applicationContext.xml"})
public class HBaseManagedAlertServiceIntegrationTests extends AbstractTestNGSpringContextTests {
	private static Logger logger = LoggerFactory.getLogger(HBaseManagedAlertServiceIntegrationTests.class);

//	@Autowired  HBaseFluentClient hBaseClient;
	@Autowired HBaseManagedAlertService managedAlertService;

//    @BeforeClass
//	public void init() throws IOException, HBaseClientException {
//
//		List<HColumnDescriptor> columFamilies = new ArrayList<HColumnDescriptor>();
//		HColumnDescriptor descrpitor = new HColumnDescriptor(Bytes.toBytes("cf"));
//		columFamilies.add(descrpitor);
//		hBaseClient.createTable()
//		.tableName("users_test3")
//		.columnsFamilies(columFamilies)
//		.build();		
//	}

	@BeforeTest
	public void setup() {
		logger.info("Setting the environment");
		System.setProperty("env", "test");	
	}
	
//	@Test (priority=1)
//	public void insertAlertRawTest() throws IOException, HBaseClientException {
//		List<Put> puts = new ArrayList<Put>();
//		byte[] row = Bytes.toBytes("123461");
//		Put put=new Put(row);
//		byte[] value;
//		value= Bytes.toBytes("{\"TEST_ALERT\":\"hello\"}");
//		put.add(Bytes.toBytes("cf"), Bytes.toBytes("data"), value);
//		byte[] comment = Bytes.toBytes("This is a comment");
//		put.add(Bytes.toBytes("cf"),Bytes.toBytes("comment"), comment);
//        puts.add(put);	
//		hBaseClient.insertData().tableName("users_test3").insertAll(puts).build();
//	}
//	
	@Test (priority=2)
	public void insertAlertTest() throws  NotImplementedException, JsonGenerationException, JsonMappingException, AlertException, IOException {
		ManagedAlert managedAlert =new ManagedAlert();
		managedAlert.setApplicationName("test");
		managedAlert.setAlertStatus(ALERT_STATUS.ACKNOWLEDGED);
		managedAlert.setMessage("This is a Test Alert Message");
//		managedAlert.setDateTime(new Date());
		managedAlertService.updateAlert(managedAlert, ALERT_STATUS.ACKNOWLEDGED, "This is comment");
			
	}
	@Test(priority=3)
	public void getAlerts() throws AlertException{
		AlertServiceRequest alertServiceRequest=new AlertServiceRequest();
		alertServiceRequest.setAlertId("test");
	    alertServiceRequest.setFromDate(new Date(1448308085598l));
	    alertServiceRequest.setToDate(new Date(1448308135586l));
	    AlertServiceResponse<ManagedAlert> alertServiceResponse = managedAlertService.getAlerts(alertServiceRequest);
	    List<ManagedAlert> list= alertServiceResponse.getAlerts();
	    for( ManagedAlert managedAlert:list){
		   logger.info("Alert Message new "+managedAlert.getMessage());
	    }		
	}

}
