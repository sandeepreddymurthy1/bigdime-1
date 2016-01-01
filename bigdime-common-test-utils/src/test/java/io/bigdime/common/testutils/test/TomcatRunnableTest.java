/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.common.testutils.test;

import javax.servlet.ServletException;

import io.bigdime.common.testutils.ITomcatRunnable;
import io.bigdime.common.testutils.TomcatRunnable;

import org.apache.catalina.Context;
import org.apache.catalina.LifecycleException;
import org.apache.catalina.Server;
import org.apache.catalina.loader.WebappLoader;
import org.apache.catalina.startup.Tomcat;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


public class TomcatRunnableTest {
	TomcatRunnable tomcatRunnable  = null;
	Tomcat tomcat = null;
	Server server = null;
	@BeforeTest
	public void beforeTest() throws ServletException, LifecycleException{
		tomcat = mock(Tomcat.class);
		server = mock(Server.class); 
		WebappLoader webappLoader = mock(WebappLoader.class);
		Context context = mock(Context.class);
		tomcatRunnable =  new TomcatRunnable(0,null);
		ReflectionTestUtils.setField(tomcatRunnable, "tomcat", tomcat);
		ReflectionTestUtils.setField(tomcatRunnable, "context", context);
		ReflectionTestUtils.setField(tomcatRunnable, "loader",webappLoader);
	}
	@Test
	public void testRun() throws ServletException, LifecycleException{
		when(tomcat.getServer()).thenReturn(server);
		tomcatRunnable.run();
		Assert.assertNotNull(tomcatRunnable.getContext());
		doThrow(LifecycleException.class).when(tomcat).start();
		tomcatRunnable.run();

	}

	@Test
	public void testStop() throws LifecycleException{
		tomcatRunnable.stop();
		Assert.assertEquals(false, tomcatRunnable.isRunning());
		doThrow(LifecycleException.class).when(tomcat).stop();
		tomcatRunnable.stop();

	}
}


