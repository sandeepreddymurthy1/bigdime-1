/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.testng;

import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;

/**
 * Extend this class to load the application context.
 * 
 * @author Neeraj Jain
 *
 */
@Configuration
@ContextConfiguration({ "classpath*:application-context.xml", "classpath*:META-INF/application-context.xml" })
public class BasicTest extends AbstractTestNGSpringContextTests {

}
