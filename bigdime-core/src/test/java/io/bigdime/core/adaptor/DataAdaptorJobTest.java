/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core.adaptor;

import org.mockito.Mockito;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.SchedulerException;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import io.bigdime.core.AdaptorContext;
import io.bigdime.core.DataAdaptorException;
import io.bigdime.core.config.ADAPTOR_TYPE;
import io.bigdime.core.config.AdaptorConfig;

@Configuration
@ContextConfiguration({ "classpath:META-INF/application-context-no-component-scan.xml" })
public class DataAdaptorJobTest extends AbstractTestNGSpringContextTests {

	private DataAdaptorJob dataAdaptorJob;
	private Adaptor dataAdaptor;
	private AdaptorConfig adaptorConfig = AdaptorConfig.getInstance();

	@BeforeMethod
	public void setup() {
		dataAdaptorJob = new DataAdaptorJob();
		dataAdaptor = Mockito.mock(Adaptor.class);
		AdaptorContext adaptorContext = Mockito.mock(AdaptorContext.class);
		Mockito.when(dataAdaptor.getAdaptorContext()).thenReturn(adaptorContext);
		Mockito.when(adaptorContext.getAdaptorName()).thenReturn("unit-DataAdaptorJobTest-1");
		Mockito.when(dataAdaptor.getAdaptorConfig()).thenReturn(adaptorConfig);
	}

	/**
	 * Assert that start method on Adaptor is invoked exactly 2 times in more
	 * than 1 seconds, if the cron expression is set to run the job every
	 * second.
	 * 
	 * @throws SchedulerException
	 * @throws InterruptedException
	 * @throws DataAdaptorException
	 */
	@Test
	public void testStartBatchJob() throws SchedulerException, InterruptedException, DataAdaptorException {
		AdaptorConfig config = AdaptorConfig.getInstance();
		config.setType(ADAPTOR_TYPE.BATCH);
		config.setCronExpression("0/1 * * * * ? *");
		dataAdaptorJob.scheduleBatchJob(dataAdaptor);
		Thread.sleep(1100);
		Mockito.verify(dataAdaptor, Mockito.atLeast(2)).start();
		Mockito.verify(dataAdaptor, Mockito.atMost(3)).start();
	}

	@Test
	public void testStartStreamingJob() throws SchedulerException, InterruptedException, DataAdaptorException {
		AdaptorConfig config = AdaptorConfig.getInstance();
		config.setType(ADAPTOR_TYPE.STREAMING);
		config.setCronExpression("0/1 * * * * ? *");
		dataAdaptorJob.scheduleStreamingJob(dataAdaptor);
		Thread.sleep(1100);
		Mockito.verify(dataAdaptor, Mockito.times(1)).start();
	}

	/**
	 * Assert that executing the job invokes the start method on Adaptor.
	 * 
	 * @throws JobExecutionException
	 * @throws DataAdaptorException
	 */
	@Test
	public void testExecute() throws JobExecutionException, DataAdaptorException {
		JobExecutionContext jobExecutionContext = Mockito.mock(JobExecutionContext.class);
		JobDetail jobDetail = Mockito.mock(JobDetail.class);
		JobDataMap jobDataMap = Mockito.mock(JobDataMap.class);
		Adaptor dataAdaptor = Mockito.mock(Adaptor.class);
		Mockito.when(jobDataMap.get("dataAdaptor")).thenReturn(dataAdaptor);

		Mockito.when(jobDetail.getJobDataMap()).thenReturn(jobDataMap);
		Mockito.when(jobExecutionContext.getJobDetail()).thenReturn(jobDetail);

		dataAdaptorJob.execute(jobExecutionContext);
		Mockito.verify(dataAdaptor, Mockito.times(1)).start();
	}

	/**
	 * Assert that the job wraps DataAdaptorException inside
	 * JobExecutionException if the Adaptor.start method throws a
	 * DataAdaptorException.
	 * 
	 * @throws Throwable
	 */
	@Test(expectedExceptions = DataAdaptorException.class)
	public void testExecuteWithDataAdaptorException() throws Throwable {
		JobExecutionContext jobExecutionContext = Mockito.mock(JobExecutionContext.class);
		JobDetail jobDetail = Mockito.mock(JobDetail.class);
		JobDataMap jobDataMap = Mockito.mock(JobDataMap.class);
		Adaptor dataAdaptor = Mockito.mock(Adaptor.class);
		Mockito.doThrow(DataAdaptorException.class).when(dataAdaptor).start();
		Mockito.when(jobDataMap.get("dataAdaptor")).thenReturn(dataAdaptor);

		Mockito.when(jobDetail.getJobDataMap()).thenReturn(jobDataMap);
		Mockito.when(jobExecutionContext.getJobDetail()).thenReturn(jobDetail);
		try {
			dataAdaptorJob.execute(jobExecutionContext);
		} catch (JobExecutionException e) {
			throw e.getCause();
		}
	}
}
