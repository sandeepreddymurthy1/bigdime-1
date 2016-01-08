/**
 * Copyright (C) 2015 Stubhub.
 */

package io.bigdime.common.testutils.test;

import static io.bigdime.common.testutils.test.TestUtilsConstants.DEV;
import static io.bigdime.common.testutils.test.TestUtilsConstants.ENV;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

import io.bigdime.common.testutils.ITomcatRunnable;
import io.bigdime.common.testutils.TomcatRunnable;
import io.bigdime.common.testutils.factory.EmbeddedTomcatFactory;

import javax.servlet.ServletException;

import org.apache.catalina.LifecycleException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;
/**
 * 
 * @author mnamburi
 *
 */
public class EmbeddedTomcatFactoryTest  {
	private Logger logger = LoggerFactory.getLogger(EmbeddedTomcatFactoryTest.class); 
	 EmbeddedTomcatFactory tomcatFactory = null;
	 
	@BeforeTest
	public void beforeClass() {
		logger.info("Setting the environment");
		System.setProperty(ENV, DEV);
		tomcatFactory = EmbeddedTomcatFactory.getInstance(); 
	}

	@Test
	public void testEmbededTomcat() throws ServletException, 
	LifecycleException, InterruptedException, IOException {
		
		 ITomcatRunnable tomcat = mock(TomcatRunnable.class);
		 ExecutorService executor = mock(ExecutorService.class);
		
		ReflectionTestUtils.setField(tomcatFactory, "tomcat", tomcat);
		ReflectionTestUtils.setField(tomcatFactory, "executor", executor);
		ReflectionTestUtils.setField(tomcatFactory, "tomcatStarted", Boolean.TRUE);
		
		when(tomcat.isRunning()).thenReturn(true).thenReturn(false);
		tomcatFactory.startTomcat();
		Assert.assertFalse(tomcatFactory.isRunning());
		Assert.assertNotNull(tomcatFactory.getContext());
		
		ReflectionTestUtils.setField(tomcatFactory, "tomcat", tomcat);
		when(tomcat.isRunning()).thenReturn(true).thenReturn(false);
		tomcatFactory.shutdownTomcat();
		Assert.assertFalse(tomcat.isRunning());
	}
}
