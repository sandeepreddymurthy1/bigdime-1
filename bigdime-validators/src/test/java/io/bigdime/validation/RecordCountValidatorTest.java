/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.validation;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.Map;

import io.bigdime.core.ActionEvent;
import io.bigdime.core.validation.DataValidationException;
import io.bigdime.core.validation.ValidationResponse.ValidationResult;
import io.bigdime.validation.DataValidationConstants;
import io.bigdime.validation.RecordCountValidator;
import io.bigdime.libs.hdfs.WebHdfs;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.ClientProtocolException;
import org.testng.Assert;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.annotations.Test;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

public class RecordCountValidatorTest {

	@Test(priority = 1, expectedExceptions = IllegalArgumentException.class)
	public void validateNullHostTest() throws DataValidationException {
		RecordCountValidator recordCountValidator = new RecordCountValidator();
		ActionEvent mockActionEvent = Mockito.mock(ActionEvent.class);
		@SuppressWarnings("unchecked")
		Map<String, String> mockMap = Mockito.mock(Map.class);
		when(mockActionEvent.getHeaders()).thenReturn(mockMap);
		when(mockMap.get(anyString())).thenReturn("");
		recordCountValidator.validate(mockActionEvent);
	}

	@Test(priority = 2, expectedExceptions = IllegalArgumentException.class)
	public void validateNullPortNumberTest() throws DataValidationException {
		RecordCountValidator recordCountValidator = new RecordCountValidator();
		ActionEvent mockActionEvent = Mockito.mock(ActionEvent.class);
		@SuppressWarnings("unchecked")
		Map<String, String> mockMap = Mockito.mock(Map.class);
		when(mockActionEvent.getHeaders()).thenReturn(mockMap);
		when(mockMap.get(anyString())).thenReturn("host").thenReturn(null);
		recordCountValidator.validate(mockActionEvent);
	}
	
	@Test(priority = 3, expectedExceptions = IllegalArgumentException.class)
	public void validateNullUserNameTest() throws DataValidationException {
		RecordCountValidator recordCountValidator = new RecordCountValidator();
		ActionEvent mockActionEvent = Mockito.mock(ActionEvent.class);
		@SuppressWarnings("unchecked")
		Map<String, String> mockMap = Mockito.mock(Map.class);
		when(mockActionEvent.getHeaders()).thenReturn(mockMap);
		when(mockMap.get(anyString())).thenReturn("host").thenReturn("111").thenReturn(null);
		recordCountValidator.validate(mockActionEvent);
	}
	
	@Test(priority = 4, expectedExceptions = NumberFormatException.class)
	public void validatePortNumberFormatTest() throws DataValidationException {
		RecordCountValidator recordCountValidator = new RecordCountValidator();
		ActionEvent mockActionEvent = Mockito.mock(ActionEvent.class);
		@SuppressWarnings("unchecked")
		Map<String, String> mockMap = Mockito.mock(Map.class);
		when(mockActionEvent.getHeaders()).thenReturn(mockMap);
		when(mockMap.get(anyString())).thenReturn("host").thenReturn("Hello");
		recordCountValidator.validate(mockActionEvent);
	}

	@Test(priority = 5, expectedExceptions = IllegalArgumentException.class)
	public void validateNullSrcRecordCountTest() throws DataValidationException {
		RecordCountValidator recordCountValidator = new RecordCountValidator();
		ActionEvent mockActionEvent = Mockito.mock(ActionEvent.class);
		@SuppressWarnings("unchecked")
		Map<String, String> mockMap = Mockito.mock(Map.class);
		when(mockActionEvent.getHeaders()).thenReturn(mockMap);
		when(mockMap.get(anyString())).thenReturn("host").thenReturn("123").thenReturn("");
		recordCountValidator.validate(mockActionEvent);
	}

	@Test(priority = 6, expectedExceptions = NumberFormatException.class)
	public void validateSrcRCParseToIntTest() throws DataValidationException {
		RecordCountValidator recordCountValidator = new RecordCountValidator();
		ActionEvent mockActionEvent = Mockito.mock(ActionEvent.class);
		@SuppressWarnings("unchecked")
		Map<String, String> mockMap = Mockito.mock(Map.class);
		when(mockActionEvent.getHeaders()).thenReturn(mockMap);
		when(mockMap.get(anyString())).thenReturn("host").thenReturn("123").thenReturn("src");
		recordCountValidator.validate(mockActionEvent);
	}

	@Test(priority = 7, expectedExceptions = IllegalArgumentException.class)
	public void validateNullHdfsPathTest() throws DataValidationException {
		RecordCountValidator recordCountValidator = new RecordCountValidator();
		ActionEvent mockActionEvent = Mockito.mock(ActionEvent.class);
		@SuppressWarnings("unchecked")
		Map<String, String> mockMap = Mockito.mock(Map.class);
		when(mockActionEvent.getHeaders()).thenReturn(mockMap);
		when(mockMap.get(anyString())).thenReturn("host").thenReturn("123").thenReturn("2342").thenReturn("");
		recordCountValidator.validate(mockActionEvent);
	}

	@Test(priority = 8, expectedExceptions = IllegalArgumentException.class)
	public void validateNullHdfsFileNameTest() throws DataValidationException {
		RecordCountValidator recordCountValidator = new RecordCountValidator();
		ActionEvent mockActionEvent = Mockito.mock(ActionEvent.class);
		@SuppressWarnings("unchecked")
		Map<String, String> mockMap = Mockito.mock(Map.class);
		when(mockActionEvent.getHeaders()).thenReturn(mockMap);
		when(mockMap.get(anyString())).thenReturn("host").thenReturn("123").thenReturn("453").thenReturn("path")
				.thenReturn(null);
		recordCountValidator.validate(mockActionEvent);
	}

	@Test(priority = 9)
	public void recordCountWithoutPartitionDiffTest()
			throws DataValidationException, ClientProtocolException, IOException {
		RecordCountValidator recordCountValidator = new RecordCountValidator();
		ActionEvent mockActionEvent = Mockito.mock(ActionEvent.class);
		@SuppressWarnings("unchecked")
		Map<String, String> mockMap = Mockito.mock(Map.class);
		when(mockActionEvent.getHeaders()).thenReturn(mockMap);
		when(mockMap.get(anyString())).thenReturn("host").thenReturn("123").thenReturn("2342")
				.thenReturn("/webhdfs/v1/path/").thenReturn("fileName").thenReturn("").thenReturn("user");
		WebHdfs mockWebHdfs = Mockito.mock(WebHdfs.class);
		ReflectionTestUtils.setField(recordCountValidator, "webHdfs", mockWebHdfs);
		HttpResponse mockResponse = Mockito.mock(HttpResponse.class);
		Mockito.when(mockWebHdfs.openFile(any(String.class))).thenReturn(mockResponse);
		HttpEntity mockHttpEntity = Mockito.mock(HttpEntity.class);
		Mockito.when(mockResponse.getEntity()).thenReturn(mockHttpEntity);
		InputStream inputStream = new ByteArrayInputStream(
				"testString \n new line in file".getBytes(Charset.forName("UTF-8")));
		Mockito.when(mockHttpEntity.getContent()).thenReturn(inputStream);
		doNothing().when(mockWebHdfs).releaseConnection();

		HttpResponse mockResponse1 = Mockito.mock(HttpResponse.class);
		when(mockWebHdfs.fileStatus(anyString())).thenReturn(mockResponse1);
		StatusLine mockSL1 = Mockito.mock(StatusLine.class);
		when(mockResponse1.getStatusLine()).thenReturn(mockSL1);
		when(mockSL1.getStatusCode()).thenReturn(404);
		doNothing().when(mockWebHdfs).releaseConnection();

		HttpResponse mockResponse2 = Mockito.mock(HttpResponse.class);
		when(mockWebHdfs.mkdir(anyString())).thenReturn(mockResponse2);
		StatusLine mockSL2 = Mockito.mock(StatusLine.class);
		when(mockResponse2.getStatusLine()).thenReturn(mockSL2);
		when(mockSL2.getStatusCode()).thenReturn(200);
		doNothing().when(mockWebHdfs).releaseConnection();
		Assert.assertEquals(recordCountValidator.validate(mockActionEvent).getValidationResult(),
				ValidationResult.FAILED);
	}

	@Test(priority = 10)
	public void recordCountWithPartitionMatchTest()
			throws DataValidationException, ClientProtocolException, IOException {
		RecordCountValidator recordCountValidator = new RecordCountValidator();
		ActionEvent mockActionEvent = Mockito.mock(ActionEvent.class);
		@SuppressWarnings("unchecked")
		Map<String, String> mockMap = Mockito.mock(Map.class);
		String hivePartitions = "dt, hr";

		when(mockActionEvent.getHeaders()).thenReturn(mockMap);
		when(mockMap.get(anyString())).thenReturn("host").thenReturn("123").thenReturn("2").thenReturn("path")
				.thenReturn("fileName").thenReturn("user").thenReturn(hivePartitions);

		WebHdfs mockWebHdfs = Mockito.mock(WebHdfs.class);
		ReflectionTestUtils.setField(recordCountValidator, "webHdfs", mockWebHdfs);
		HttpResponse mockResponse = Mockito.mock(HttpResponse.class);
		Mockito.when(mockWebHdfs.openFile(any(String.class))).thenReturn(mockResponse);
		HttpEntity mockHttpEntity = Mockito.mock(HttpEntity.class);
		Mockito.when(mockResponse.getEntity()).thenReturn(mockHttpEntity);
		InputStream inputStream = new ByteArrayInputStream(
				"testString \n new line in file".getBytes(Charset.forName("UTF-8")));
		Mockito.when(mockHttpEntity.getContent()).thenReturn(inputStream);
		Assert.assertEquals(recordCountValidator.validate(mockActionEvent).getValidationResult(),
				ValidationResult.PASSED);
	}

	@Test(priority = 11)
	public void recordCountWithRCErrorDirExistsDiffTest()
			throws DataValidationException, ClientProtocolException, IOException {
		RecordCountValidator recordCountValidator = new RecordCountValidator();
		ActionEvent mockActionEvent = Mockito.mock(ActionEvent.class);
		@SuppressWarnings("unchecked")
		Map<String, String> mockMap = Mockito.mock(Map.class);
		when(mockActionEvent.getHeaders()).thenReturn(mockMap);
		when(mockMap.get(anyString())).thenReturn("host").thenReturn("123").thenReturn("2342")
				.thenReturn("/webhdfs/v1/path/").thenReturn("fileName").thenReturn("").thenReturn("user");
		WebHdfs mockWebHdfs = Mockito.mock(WebHdfs.class);
		ReflectionTestUtils.setField(recordCountValidator, "webHdfs", mockWebHdfs);
		HttpResponse mockResponse = Mockito.mock(HttpResponse.class);
		Mockito.when(mockWebHdfs.openFile(any(String.class))).thenReturn(mockResponse);
		HttpEntity mockHttpEntity = Mockito.mock(HttpEntity.class);
		Mockito.when(mockResponse.getEntity()).thenReturn(mockHttpEntity);
		InputStream inputStream = new ByteArrayInputStream(
				"testString \n new line in file".getBytes(Charset.forName("UTF-8")));
		Mockito.when(mockHttpEntity.getContent()).thenReturn(inputStream);
		doNothing().when(mockWebHdfs).releaseConnection();

		HttpResponse mockResponse1 = Mockito.mock(HttpResponse.class);
		when(mockWebHdfs.fileStatus(anyString())).thenReturn(mockResponse1);
		StatusLine mockSL1 = Mockito.mock(StatusLine.class);
		when(mockResponse1.getStatusLine()).thenReturn(mockSL1);
		when(mockSL1.getStatusCode()).thenReturn(200);
		doNothing().when(mockWebHdfs).releaseConnection();
		Assert.assertEquals(recordCountValidator.validate(mockActionEvent).getValidationResult(),
				ValidationResult.FAILED);
	}

	@Test(priority = 12)
	public void getInstanceTest() {
		Assert.assertNotNull(DataValidationConstants.getInstance());
	}

	@Test(priority = 13)
	public void gettersAndSettersTest() {
		RecordCountValidator recordCountValidator = new RecordCountValidator();
		recordCountValidator.setName("testName");
		Assert.assertEquals(recordCountValidator.getName(), "testName");
	}

}
