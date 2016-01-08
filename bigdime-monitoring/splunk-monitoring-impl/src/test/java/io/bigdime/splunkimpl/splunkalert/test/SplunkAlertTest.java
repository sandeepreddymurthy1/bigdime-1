/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.splunkimpl.splunkalert.test;

import io.bigdime.alert.Logger;
import io.bigdime.alert.LoggerFactory;
import io.bigdime.splunkalert.SplunkAlert;

import org.powermock.modules.testng.PowerMockTestCase;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.Test;

import static io.bigdime.splunkimpl.splunkalert.constants.TestResourceConstants.SOURCETYPE;
import static io.bigdime.splunkimpl.splunkalert.constants.TestResourceConstants.HOST;
import static io.bigdime.splunkimpl.splunkalert.constants.TestResourceConstants.INTERPRETATION;
import static io.bigdime.splunkimpl.splunkalert.constants.TestResourceConstants.RAW;
import static io.bigdime.splunkimpl.splunkalert.constants.TestResourceConstants.SOURCE_TYPE;
/**
 * 
 * @author Sandeep Reddy,Murthy
 *
 */
public class SplunkAlertTest extends PowerMockTestCase{
	private static final Logger logger = LoggerFactory.getLogger(SplunkAlertTest.class);
	
	@Test
	public void validateSplunkAlertGettersandSetters() throws Exception {
	logger.info(SOURCE_TYPE,"Test Phase","validateSplunkAlertGettersandSetters");
	SplunkAlert splunkAlert=new SplunkAlert();
	splunkAlert.setSourceType(SOURCETYPE);
	splunkAlert.setHost(HOST);
	splunkAlert.setInterpretation(INTERPRETATION);
	splunkAlert.set_raw(RAW);
	
	Assert.assertEquals(splunkAlert.getSourceType(), SOURCETYPE);
	Assert.assertEquals(splunkAlert.getHost(),HOST);
	Assert.assertEquals(splunkAlert.getInterpretation(), INTERPRETATION);
	Assert.assertEquals(splunkAlert.get_raw(), RAW);
		
}

}
