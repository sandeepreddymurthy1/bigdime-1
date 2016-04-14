/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.handler.file;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;

import org.testng.Assert;
import org.testng.annotations.Test;

import io.bigdime.core.ActionEvent;
import io.bigdime.core.ActionEvent.Status;
import io.bigdime.core.AdaptorConfigurationException;
import io.bigdime.core.constants.ActionEventHeaderConstants;
import io.bigdime.core.handler.HandlerContext;

public class PartitionParserHandlerTest {

	@Test
	public void testBuild() throws AdaptorConfigurationException {
		PartitionParserHandler partitionNamesParserHandler = getPartitionNamesParserHandler();

		Assert.assertEquals(partitionNamesParserHandler.getRegex(), "(.*)_(\\d{2}(?:-?)\\d{2}(?:-?)\\d{4}).log$");
		Assert.assertEquals(partitionNamesParserHandler.getHeaderName(), ActionEventHeaderConstants.SOURCE_FILE_NAME);
		// Assert.assertEquals(partitionNamesParserHandler.getPartitionNames(),
		// "account, timestamp");
		// Assert.assertEquals(partitionNamesParserHandler.getTruncateCharacters(),
		// "-:");
	}

	@Test
	public void testBuildWithDifferentInputOutputOrder() throws AdaptorConfigurationException {

		PartitionParserHandler partitionNamesParserHandler = new PartitionParserHandler();

		Map<String, Object> propertyMap = new HashMap<>();
		propertyMap.put(PartitionNamesParserHandlerConstants.HEADER_NAME, ActionEventHeaderConstants.SOURCE_FILE_NAME);
		propertyMap.put(PartitionNamesParserHandlerConstants.REGEX, "(.*)_(\\d{2}(?:-?)\\d{2}(?:-?)\\d{4}).log$");
		propertyMap.put(PartitionNamesParserHandlerConstants.PARTITION_NAMES, "account, timestamp");
		propertyMap.put(PartitionNamesParserHandlerConstants.DATE_PARTITION_NAME, "timestamp");
		propertyMap.put(PartitionNamesParserHandlerConstants.DATE_PARTITION_INPUT_FORMAT, "MM-dd-yyyy");
		propertyMap.put(PartitionNamesParserHandlerConstants.DATE_PARTITION_OUTPUT_FORMAT, "yyyyMMdd");
		propertyMap.put(PartitionNamesParserHandlerConstants.PARTITION_NAMES_OUTPUT_ORDER, "timestamp, account");

		partitionNamesParserHandler.setPropertyMap(propertyMap);
		partitionNamesParserHandler.build();

		Assert.assertEquals(partitionNamesParserHandler.getRegex(), "(.*)_(\\d{2}(?:-?)\\d{2}(?:-?)\\d{4}).log$");
		Assert.assertEquals(partitionNamesParserHandler.getHeaderName(), ActionEventHeaderConstants.SOURCE_FILE_NAME);
	}

	@Test
	public void testBuildWithoutHeaderNames() throws AdaptorConfigurationException {

		PartitionParserHandler partitionNamesParserHandler = new PartitionParserHandler();

		Map<String, Object> propertyMap = new HashMap<>();
		propertyMap.put(PartitionNamesParserHandlerConstants.REGEX, "(.*)_(\\d{2}(?:-?)\\d{2}(?:-?)\\d{4}).log$");
		propertyMap.put(PartitionNamesParserHandlerConstants.PARTITION_NAMES, "account, timestamp");
		partitionNamesParserHandler.setPropertyMap(propertyMap);
		partitionNamesParserHandler.build();

		Assert.assertEquals(partitionNamesParserHandler.getRegex(), "(.*)_(\\d{2}(?:-?)\\d{2}(?:-?)\\d{4}).log$");
		Assert.assertEquals(partitionNamesParserHandler.getHeaderName(), ActionEventHeaderConstants.SOURCE_FILE_NAME);
		// Assert.assertEquals(partitionNamesParserHandler.getPartitionNames(),
		// "account, timestamp");
		Assert.assertEquals(partitionNamesParserHandler.getTruncateCharacters(), null);
	}

	private PartitionParserHandler getPartitionNamesParserHandler() throws AdaptorConfigurationException {
		PartitionParserHandler partitionNamesParserHandler = new PartitionParserHandler();

		Map<String, Object> propertyMap = new HashMap<>();
		propertyMap.put(PartitionNamesParserHandlerConstants.HEADER_NAME, ActionEventHeaderConstants.SOURCE_FILE_NAME);
		propertyMap.put(PartitionNamesParserHandlerConstants.REGEX, "(.*)_(\\d{2}(?:-?)\\d{2}(?:-?)\\d{4}).log$");
		propertyMap.put(PartitionNamesParserHandlerConstants.PARTITION_NAMES, "account, timestamp");
		propertyMap.put(PartitionNamesParserHandlerConstants.DATE_PARTITION_NAME, "timestamp");
		propertyMap.put(PartitionNamesParserHandlerConstants.DATE_PARTITION_INPUT_FORMAT, "MM-dd-yyyy");
		propertyMap.put(PartitionNamesParserHandlerConstants.DATE_PARTITION_OUTPUT_FORMAT, "yyyyMMdd");

		partitionNamesParserHandler.setPropertyMap(propertyMap);
		partitionNamesParserHandler.build();
		return partitionNamesParserHandler;
	}

	private PartitionParserHandler getPartitionNamesParserHandlerWithDifferentPartitionOrder()
			throws AdaptorConfigurationException {
		PartitionParserHandler partitionNamesParserHandler = new PartitionParserHandler();
		Map<String, Object> propertyMap = new HashMap<>();
		propertyMap.put(PartitionNamesParserHandlerConstants.HEADER_NAME, ActionEventHeaderConstants.SOURCE_FILE_NAME);
		propertyMap.put(PartitionNamesParserHandlerConstants.REGEX, "(.*)_(\\d{2}(?:-?)\\d{2}(?:-?)\\d{4}).log$");
		propertyMap.put(PartitionNamesParserHandlerConstants.PARTITION_NAMES, "account, timestamp");
		propertyMap.put(PartitionNamesParserHandlerConstants.DATE_PARTITION_NAME, "timestamp");
		propertyMap.put(PartitionNamesParserHandlerConstants.DATE_PARTITION_INPUT_FORMAT, "MM-dd-yyyy");
		propertyMap.put(PartitionNamesParserHandlerConstants.DATE_PARTITION_OUTPUT_FORMAT, "yyyyMMdd");
		propertyMap.put(PartitionNamesParserHandlerConstants.PARTITION_NAMES_OUTPUT_ORDER, "timestamp, account");

		partitionNamesParserHandler.setPropertyMap(propertyMap);
		partitionNamesParserHandler.build();
		return partitionNamesParserHandler;
	}

	@Test
	public void testProcess() throws Throwable {
		FutureTask<Status> futureTask = new FutureTask<>(new Callable<Status>() {
			@Override
			public Status call() throws Exception {
				PartitionParserHandler partitionNamesParserHandler = getPartitionNamesParserHandler();
				HandlerContext context = HandlerContext.get();
				ActionEvent actionEvent = new ActionEvent();
				actionEvent.getHeaders().put(ActionEventHeaderConstants.SOURCE_FILE_NAME,
						"NetworkActivity_7937_3791772_11-03-2015.log");
				context.createSingleItemEventList(actionEvent);
				Status status = partitionNamesParserHandler.process();
				ActionEvent outputEvent = context.getEventList().get(0);
				Assert.assertEquals(outputEvent.getHeaders().get(ActionEventHeaderConstants.HIVE_PARTITION_NAMES),
						"account, timestamp");
				return status;
			}
		});

		ExecutorService executorService = Executors.newFixedThreadPool(1);
		executorService.execute(futureTask);
		try {
			futureTask.get();
		} catch (ExecutionException ex) {
			throw ex.getCause();
		}
	}

	@Test
	public void testProcessWithDifferentPartitionOrder() throws Throwable {
		FutureTask<Status> futureTask = new FutureTask<>(new Callable<Status>() {
			@Override
			public Status call() throws Exception {
				PartitionParserHandler partitionNamesParserHandler = getPartitionNamesParserHandlerWithDifferentPartitionOrder();
				HandlerContext context = HandlerContext.get();
				ActionEvent actionEvent = new ActionEvent();
				actionEvent.getHeaders().put(ActionEventHeaderConstants.SOURCE_FILE_NAME,
						"NetworkActivity_7937_3791772_11-03-2015.log");
				context.createSingleItemEventList(actionEvent);
				Status status = partitionNamesParserHandler.process();
				ActionEvent outputEvent = context.getEventList().get(0);
				Assert.assertEquals(outputEvent.getHeaders().get(ActionEventHeaderConstants.HIVE_PARTITION_VALUES),
						"20151103,NetworkActivity_7937_3791772");
				Assert.assertEquals(outputEvent.getHeaders().get(ActionEventHeaderConstants.HIVE_PARTITION_NAMES),
						"timestamp, account");
				return status;
			}
		});

		ExecutorService executorService = Executors.newFixedThreadPool(1);
		executorService.execute(futureTask);
		try {
			futureTask.get();
		} catch (ExecutionException ex) {
			throw ex.getCause();
		}
	}

}
