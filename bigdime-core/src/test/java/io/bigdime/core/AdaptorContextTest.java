/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core;

import java.io.IOException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.Test;

import io.bigdime.core.adaptor.DataAdaptor;
import io.bigdime.core.handler.HandlerFactoryTest;

@Configuration
@ContextConfiguration({ "classpath*:application-context.xml", "classpath*:META-INF/application-context.xml" })

public class AdaptorContextTest extends AbstractTestNGSpringContextTests {
	@Autowired
	private DataAdaptor dataAdaptor;

	public AdaptorContextTest() throws IOException {
		HandlerFactoryTest.initHandlerFactory();
	}

	/**
	 * Assert that adaptor has a not-null name.
	 * 
	 * @throws AdaptorConfigurationException
	 */
	@Test
	public void testGetAndSetNameFromContext() throws AdaptorConfigurationException {
		String actualName = dataAdaptor.getAdaptorContext().getAdaptorName();
		Assert.assertNotNull(actualName);
		dataAdaptor.getAdaptorConfig().setName("another-name");
		actualName = dataAdaptor.getAdaptorContext().getAdaptorName();
		Assert.assertNotNull(actualName);
	}
}
