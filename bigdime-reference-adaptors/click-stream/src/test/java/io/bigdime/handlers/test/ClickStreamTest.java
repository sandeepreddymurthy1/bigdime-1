/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.handlers.test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

import io.bigdime.core.adaptor.DataAdaptorJob;

//@Configuration
//@ContextConfiguration({ "classpath*:application-context.xml",
//		"classpath*:META-INF/application-context.xml" })
public class ClickStreamTest extends AbstractTestNGSpringContextTests {
	@Autowired
	DataAdaptorJob d;
	
	//@Test
	public void testClicStream() throws InterruptedException{
		System.out.println(d);
	}
}