/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.handler.file;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;

import org.apache.commons.lang.NotImplementedException;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import io.bigdime.alert.LoggerFactory;
import io.bigdime.core.ActionEvent;
import io.bigdime.core.ActionEvent.Status;
import io.bigdime.core.AdaptorConfigurationException;
import io.bigdime.core.HandlerException;
import io.bigdime.core.InvalidValueConfigurationException;
import io.bigdime.core.commons.AdaptorLogger;
import io.bigdime.core.commons.FileHelper;
import io.bigdime.core.config.AdaptorConfig;
import io.bigdime.core.config.AdaptorConfigConstants.SourceConfigConstants;
import io.bigdime.core.handler.HandlerContext;
import io.bigdime.core.handler.HandlerJournal;
import io.bigdime.core.handler.SimpleJournal;
import io.bigdime.core.runtimeinfo.RuntimeInfo;
import io.bigdime.core.runtimeinfo.RuntimeInfoStore;
import io.bigdime.core.runtimeinfo.RuntimeInfoStoreException;

public class FileInputReaderHandlerTest {
	private static final AdaptorLogger logger = new AdaptorLogger(
			LoggerFactory.getLogger(FileInputReaderHandlerTest.class));

	/**
	 * Setup the FileInputStreamHandler object with real values and make sure
	 * that test data is read properly.
	 * 
	 * @throws Throwable
	 */

	/**
	 * If there is at least one file to be processed, make sure that process
	 * method returns action event with Status.CALLBACK.
	 * 
	 * @throws Throwable
	 */
	@Test(threadPoolSize = 1)
	public void testProcess() throws Throwable {
		String writeData = "unit-test-testProcess";
		File[] tempFiles = setFiles(writeData);
		for (File f : tempFiles)
			System.out.println(f.getAbsolutePath());
		List<RuntimeInfo> runtimeInfoList = new ArrayList<>();
		RuntimeInfo runtimeInfo = new RuntimeInfo();
		runtimeInfo.setInputDescriptor(tempFiles[0].getAbsolutePath());
		runtimeInfo.setStatus(RuntimeInfoStore.Status.VALIDATED);
		runtimeInfoList.add(runtimeInfo);

		// ActionEvent outputEvent = setupFirh(runtimeInfoList, writeData,
		// tempFiles[1].getParent());
		Status outputStauts;
		outputStauts = setupFirh(runtimeInfoList, writeData, tempFiles[1].getParent());
		Assert.assertEquals(outputStauts, Status.CALLBACK);
		HandlerContext handlerContext = HandlerContext.get();
		List<ActionEvent> actionEvents = handlerContext.getEventList();
		Assert.assertNotNull(actionEvents);
		System.out.println("handlerContext=" + handlerContext);
		// Assert.assertEquals(actionEvents.size(), 1);
		// Assert.assertEquals(new String(actionEvents.get(0).getBody(),
		// Charset.defaultCharset()),
		// writeData.substring(0, 10), "body of outputEvent should besame as
		// inputEvent");

		// Assert.assertEquals(new String(outputEvent.getBody(),
		// Charset.defaultCharset()), writeData.substring(0, 10),
		// "body of outputEvent should besame as inputEvent");
		tempFiles[0].delete();
		tempFiles[1].delete();
	}

	/**
	 * If there is no new file to be processed, make sure that process method
	 * returns action event with Status.BACKOFF.
	 * 
	 * @throws Throwable
	 */
	@Test
	public void testProcessWithNoFileToProcess() throws Throwable {
		String writeData = "unit-test-testProcess";
		File[] tempFiles = setFiles(writeData);
		List<RuntimeInfo> runtimeInfoList = new ArrayList<>();
		RuntimeInfo runtimeInfo = new RuntimeInfo();
		runtimeInfo.setInputDescriptor(tempFiles[1].getAbsolutePath());
		runtimeInfo.setStatus(RuntimeInfoStore.Status.VALIDATED);
		runtimeInfoList.add(runtimeInfo);

		Status status = setupFirh(runtimeInfoList, writeData, tempFiles[1].getParent());
		logger.debug("running testProcessWithNoFileToProcess", "status={}", status);
		// Assert.assertTrue((status==Status.BACKOFF));
		tempFiles[0].delete();
		tempFiles[1].delete();
	}

	/**
	 * If the RuntimeStore returns a null list, all files need to be processed,
	 * so make sure that process method returns action event with
	 * Status.CALLBACK, since test data has 51 bytes and we are reading only 10.
	 * 
	 * @throws Throwable
	 */
	// @Test
	public void testProcessWithNoDataFromRuntimeInfoStore() throws Throwable {
		String writeData = "unit-test-testProcessWithNoDataFromRuntimeInfoStore";
		File[] tempFiles = setFiles(writeData);
		Status status = setupFirh(null, writeData, tempFiles[1].getParent());
		Assert.assertEquals(status, Status.CALLBACK, "status must be callback");
		// Assert.assertEquals(new String(outputEvent.getBody(),
		// Charset.defaultCharset()), writeData.substring(0, 10),
		// "body of outputEvent should besame as inputEvent");
		tempFiles[0].delete();
		tempFiles[1].delete();
	}

	@Test
	public void testProcessWithSlashAsRelativePath() throws Throwable {
		String writeData = "unit-test-testProcessWithNoDataFromRuntimeInfoStore";
		File[] tempFiles = setFiles(writeData);
		final String fileLocation = tempFiles[0].getParent();

		FutureTask<Status> futureTask = new FutureTask<>(new Callable<Status>() {
			@Override
			public Status call() throws Exception {
				FileInputStreamHandler firh = new FileInputStreamHandler();
				HandlerContext context = HandlerContext.get();
				HandlerJournal fhj = new SimpleJournal();
				fhj.setTotalRead(0);
				context.setJournal(firh.getId(), fhj);
				Map<String, Object> propertyMap = new HashMap<>();
				int bufferSize = 10;

				// propertyMap.put("fileLocation", fileLocation);
				// propertyMap.put(FileInputStreamReaderHandlerConstants.FILE_NAME_PATTERN,
				// "temp-file-name.*");
				propertyMap.put(FileInputStreamReaderHandlerConstants.BUFFER_SIZE, bufferSize);

				Map<String, String> srcDEscEntryMap = new HashMap<>();
				srcDEscEntryMap.put("entity:temp-file-name.*", "input1");
				propertyMap.put(SourceConfigConstants.SRC_DESC, srcDEscEntryMap.entrySet().iterator().next());
				propertyMap.put(FileInputStreamReaderHandlerConstants.BASE_PATH, fileLocation + "/");

				firh.setPropertyMap(propertyMap);
				firh.build();
				@SuppressWarnings("unchecked")
				RuntimeInfoStore<RuntimeInfo> runtimeInfoStore = Mockito.mock(RuntimeInfoStore.class);
				Mockito.when(runtimeInfoStore.getAll(AdaptorConfig.getInstance().getName(), "entity")).thenReturn(null);
				ReflectionTestUtils.setField(firh, "runtimeInfoStore", runtimeInfoStore);
				ReflectionTestUtils.setField(firh, "fileInputDescriptor", new FileInputDescriptor());
				ReflectionTestUtils.setField(firh, "fileHelper", FileHelper.getInstance());
				Mockito.when(runtimeInfoStore.put(Mockito.any(RuntimeInfo.class))).thenReturn(true);
				Status status = firh.process();
				Assert.assertEquals(firh.getEntityName(), "entity");
				Assert.assertEquals(firh.getFileNamePattern(), "temp-file-name.*");
				Assert.assertEquals(firh.getBasePath(), fileLocation + "/");
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

	private File[] setFiles(String writeData) throws IOException {
		File[] tempFiles = new File[2];

		for (int i = 0; i < 2; i++) {
			tempFiles[i] = File.createTempFile("temp-file-name-" + i, ".tmp");

			tempFiles[i].deleteOnExit();
			BufferedWriter bw = new BufferedWriter(
					new OutputStreamWriter(new FileOutputStream(tempFiles[i]), Charset.defaultCharset()));
			bw.write(writeData);
			bw.close();
		}
		return tempFiles;
	}

	public Status setupFirh(final List<RuntimeInfo> runtimeInfoList, final String writeData, final String fileLocation)
			throws Throwable {
		FutureTask<Status> futureTask = new FutureTask<>(new Callable<Status>() {
			@Override
			public Status call() throws Exception {
				FileInputStreamHandler firh = new FileInputStreamHandler();
				HandlerContext context = HandlerContext.get();
				// context.setTotalRead(0);
				// context.getJournal(firh.getId()).put("totalRead", 0l);
				HandlerJournal fhj = new SimpleJournal();
				fhj.setTotalRead(0);
				context.setJournal(firh.getId(), fhj);
				Map<String, Object> propertyMap = new HashMap<>();
				int bufferSize = 10;

				// propertyMap.put("fileLocation", fileLocation);
				propertyMap.put(FileInputStreamReaderHandlerConstants.FILE_NAME_PATTERN, "temp-file-name.*");
				propertyMap.put(FileInputStreamReaderHandlerConstants.BUFFER_SIZE, bufferSize);

				Map<String, String> srcDEscEntryMap = new HashMap<>();
				srcDEscEntryMap.put("entity:temp-file-name.*", "input1");
				propertyMap.put(SourceConfigConstants.SRC_DESC, srcDEscEntryMap.entrySet().iterator().next());
				propertyMap.put(FileInputStreamReaderHandlerConstants.BASE_PATH, fileLocation);

				firh.setPropertyMap(propertyMap);
				firh.build();
				@SuppressWarnings("unchecked")
				RuntimeInfoStore<RuntimeInfo> runtimeInfoStore = Mockito.mock(RuntimeInfoStore.class);
				Mockito.when(runtimeInfoStore.getAll(AdaptorConfig.getInstance().getName(), "entity",
						RuntimeInfoStore.Status.QUEUED)).thenReturn(runtimeInfoList);
				ReflectionTestUtils.setField(firh, "runtimeInfoStore", runtimeInfoStore);
				ReflectionTestUtils.setField(firh, "fileInputDescriptor", new FileInputDescriptor());
				ReflectionTestUtils.setField(firh, "fileHelper", FileHelper.getInstance());
				Mockito.when(runtimeInfoStore.put(Mockito.any(RuntimeInfo.class))).thenReturn(true);

				return firh.process();
				// ActionEvent outputEvent = firh.process(new ActionEvent());
				// return outputEvent;
			}
		});

		ExecutorService executorService = Executors.newFixedThreadPool(1);
		executorService.execute(futureTask);
		try {
			return futureTask.get();
		} catch (ExecutionException ex) {
			throw ex.getCause();
		}
	}

	/**
	 * Make sure that IOException is handled and then thrown as
	 * HandlerException. This test is basically to cover all the branches.
	 * 
	 * @throws Throwable
	 */
	@Test(expectedExceptions = IOException.class)
	public void testProcessWithIOException() throws Throwable {
		try {
			FileInputStreamHandler firh = new FileInputStreamHandler();
			HandlerContext handlerContext = HandlerContext.get();
			// handlerContext.setTotalRead(1);
			// handlerContext.setTotalSize(2);
			HandlerJournal fhj = new SimpleJournal();
			fhj.setTotalRead(1);
			fhj.setTotalSize(2);
			handlerContext.setJournal(firh.getId(), fhj);

			// handlerContext.getJournal(firh.getId()).put("totalRead", 1l);
			// handlerContext.getJournal(firh.getId()).put("totalSize", 2l);

			FileChannel fileChannel = Mockito.mock(FileChannel.class);
			// RandomAccessFile file = Mockito.mock(RandomAccessFile.class);
			Mockito.when(fileChannel.read(Mockito.any(ByteBuffer.class), Mockito.anyLong()))
					.thenThrow(new IOException(""));
			// ReflectionTestUtils.setField(file, "channel", fileChannel);
			// ReflectionTestUtils.setField(firh, "file", file);
			ReflectionTestUtils.setField(firh, "fileChannel", fileChannel);

			RuntimeInfoStore<RuntimeInfo> runtimeInfoStore = Mockito.mock(RuntimeInfoStore.class);
			ReflectionTestUtils.setField(firh, "runtimeInfoStore", runtimeInfoStore);
			Mockito.when(runtimeInfoStore.put(Mockito.any(RuntimeInfo.class))).thenReturn(true);

			firh.process();
			Assert.fail("should have thrown IOException");
		} catch (HandlerException e) {
			throw e.getCause();
		}
	}

	@Test(expectedExceptions = RuntimeInfoStoreException.class)
	public void testProcessWithRuntimeInfoStoreException() throws Throwable {
		try {
			FileInputStreamHandler firh = new FileInputStreamHandler();
			HandlerContext handlerContext = HandlerContext.get();

			String writeData = "unit-test-testProcess";
			File[] tempFiles = setFiles(writeData);
			// List<RuntimeInfo> runtimeInfoList = new ArrayList<>();

			// handlerContext.setTotalRead(1);
			// handlerContext.setTotalSize(1);
			// handlerContext.getJournal(firh.getId()).put("totalRead", 1l);
			// handlerContext.getJournal(firh.getId()).put("totalSize", 1l);
			HandlerJournal fhj = new SimpleJournal();
			fhj.setTotalRead(1);
			fhj.setTotalSize(1);
			handlerContext.setJournal(firh.getId(), fhj);

			@SuppressWarnings("unchecked")
			RuntimeInfoStore<RuntimeInfo> runtimeInfoStore = Mockito.mock(RuntimeInfoStore.class);
			// Mockito.when(runtimeInfoStore.getAll(AdaptorConfig.getInstance().getName(),
			// "entity"))
			// .thenThrow(new RuntimeInfoStoreException(""));
			Mockito.when(runtimeInfoStore.getAll(Mockito.anyString(), Mockito.anyString(),
					Mockito.any(RuntimeInfoStore.Status.class))).thenThrow(new RuntimeInfoStoreException(""));
			ReflectionTestUtils.setField(firh, "entityName", "entityName");
			ReflectionTestUtils.setField(firh, "runtimeInfoStore", runtimeInfoStore);
			ReflectionTestUtils.setField(firh, "fileHelper", FileHelper.getInstance());
			ReflectionTestUtils.setField(firh, "fileLocation", tempFiles[0].getParent() + "/");
			ReflectionTestUtils.setField(firh, "fileNamePattern", "temp-file-name.*");

			firh.process();
			Assert.fail("should have thrown RuntimeInfoStoreException");
		} catch (HandlerException e) {
			throw e.getCause();
		}
	}

	@Test
	public void testBuild() throws InterruptedException {
		try {
			Map<String, Object> propertyMap = new HashMap<>();

			File temp = File.createTempFile("temp-file-name", ".tmp");
			temp.deleteOnExit();
			propertyMap.put("fileLocation", temp.getParent());

			Map<String, String> srcDEscEntryMap = new HashMap<>();
			srcDEscEntryMap.put("build_entity:temp-*", "input1");
			propertyMap.put(SourceConfigConstants.SRC_DESC, srcDEscEntryMap.entrySet().iterator().next());

			final FileInputStreamHandler firh = new FileInputStreamHandler();
			firh.setPropertyMap(propertyMap);
			firh.build();
			firh.build();// 2nd build call to cover file=null branch
			temp.delete();
			Assert.assertEquals(firh.getEntityName(), "build_entity");
			Assert.assertEquals(firh.getFileNamePattern(), "temp-*");
			Assert.assertEquals(firh.getBasePath(), "/");

		} catch (AdaptorConfigurationException e) {
			Assert.fail(e.toString());
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}

	@Test(expectedExceptions = InvalidValueConfigurationException.class, expectedExceptionsMessageRegExp = "src-desc can't be null.*")
	public void testBuildWithNoSrcDesc() throws AdaptorConfigurationException {
		Map<String, Object> propertyMap = new HashMap<>();
		propertyMap.put("fileLocation", "some-invalid-path");

		Map<String, String> srcDEscEntryMap = new HashMap<>();
		srcDEscEntryMap.put("some-invalid-path", "input1");

		final FileInputStreamHandler firh = new FileInputStreamHandler();
		firh.setPropertyMap(propertyMap);
		firh.build();
		Assert.fail("build should have failed with InvalidValueConfigurationException");
	}

	@Test(expectedExceptions = NotImplementedException.class)
	public void testShutdown() {
		final FileInputStreamHandler firh = new FileInputStreamHandler();
		firh.shutdown();

	}
}
