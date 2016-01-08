/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.libs.hdfs;

import java.io.IOException;
import java.io.InputStream;

import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.ClientProtocolException;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.annotations.Test;

public class WebHdfsWriterTest {
	WebHdfs webHdfs;

	@Test
	public void testWriteAppendMkdir200() throws InterruptedException, ClientProtocolException, IOException {

		gen(200, 200, 200);
		WebHdfsWriter webHdfsWriter = new WebHdfsWriter();
		webHdfsWriter.write(webHdfs, "unit-base", "unit-payload".getBytes(), "unit-hdfsfilename");

		Mockito.verify(webHdfs, Mockito.times(1)).fileStatus(Mockito.anyString());
		Mockito.verify(webHdfs, Mockito.times(1)).mkdir(Mockito.anyString());
		Mockito.verify(webHdfs, Mockito.times(1)).append(Mockito.anyString(), Mockito.any(InputStream.class));
		Mockito.verify(webHdfs, Mockito.times(0)).createAndWrite(Mockito.anyString(), Mockito.any(InputStream.class));
	}

	@Test
	public void testWriteAppendMkdir201() throws InterruptedException, ClientProtocolException, IOException {

		gen(201, 200, 200);
		WebHdfsWriter webHdfsWriter = new WebHdfsWriter();
		webHdfsWriter.write(webHdfs, "unit-base", "unit-payload".getBytes(), "unit-hdfsfilename");

		Mockito.verify(webHdfs, Mockito.times(1)).fileStatus(Mockito.anyString());
		Mockito.verify(webHdfs, Mockito.times(1)).mkdir(Mockito.anyString());
		Mockito.verify(webHdfs, Mockito.times(1)).append(Mockito.anyString(), Mockito.any(InputStream.class));
		Mockito.verify(webHdfs, Mockito.times(0)).createAndWrite(Mockito.anyString(), Mockito.any(InputStream.class));
	}

	@Test
	public void testWriteCreate() throws InterruptedException, ClientProtocolException, IOException {
		gen(200, 404, 200);
		WebHdfsWriter webHdfsWriter = new WebHdfsWriter();
		webHdfsWriter.write(webHdfs, "unit-base", "unit-payload".getBytes(), "unit-hdfsfilename");

		Mockito.verify(webHdfs, Mockito.times(1)).fileStatus(Mockito.anyString());
		Mockito.verify(webHdfs, Mockito.times(1)).mkdir(Mockito.anyString());
		Mockito.verify(webHdfs, Mockito.times(0)).append(Mockito.anyString(), Mockito.any(InputStream.class));
		Mockito.verify(webHdfs, Mockito.times(1)).createAndWrite(Mockito.anyString(), Mockito.any(InputStream.class));
	}

	/**
	 * If the command to check file's existence returns anything other than
	 * 200,201,404, then append or createAndWrite should not be invoked, since
	 * it's an exception condition.
	 * 
	 * @throws InterruptedException
	 * @throws ClientProtocolException
	 * @throws IOException
	 */
	@Test(expectedExceptions = IOException.class)
	public void testWriteCreateWithFileExistsReturning403()
			throws InterruptedException, ClientProtocolException, IOException {
		gen(200, 403, 200);
		WebHdfsWriter webHdfsWriter = new WebHdfsWriter();
		webHdfsWriter.write(webHdfs, "unit-base", "unit-payload".getBytes(), "unit-hdfsfilename");

		Mockito.verify(webHdfs, Mockito.times(1)).fileStatus(Mockito.anyString());
		Mockito.verify(webHdfs, Mockito.times(1)).mkdir(Mockito.anyString());
		Mockito.verify(webHdfs, Mockito.times(0)).append(Mockito.anyString(), Mockito.any(InputStream.class));
		Mockito.verify(webHdfs, Mockito.times(0)).createAndWrite(Mockito.anyString(), Mockito.any(InputStream.class));
	}

	@Test
	public void testWriteCreateWith201() throws InterruptedException, ClientProtocolException, IOException {
		gen(200, 404, 201);
		WebHdfsWriter webHdfsWriter = new WebHdfsWriter();
		webHdfsWriter.write(webHdfs, "unit-base", "unit-payload".getBytes(), "unit-hdfsfilename");

		Mockito.verify(webHdfs, Mockito.times(1)).fileStatus(Mockito.anyString());
		Mockito.verify(webHdfs, Mockito.times(1)).mkdir(Mockito.anyString());
		Mockito.verify(webHdfs, Mockito.times(0)).append(Mockito.anyString(), Mockito.any(InputStream.class));
		Mockito.verify(webHdfs, Mockito.times(1)).createAndWrite(Mockito.anyString(), Mockito.any(InputStream.class));
	}

	@Test(expectedExceptions = IOException.class)
	public void testWriteCreateFailsWith404() throws InterruptedException, ClientProtocolException, IOException {
		gen(200, 404, 404);
		WebHdfsWriter webHdfsWriter = new WebHdfsWriter();
		ReflectionTestUtils.setField(webHdfsWriter, "sleepTime", 1);
		webHdfsWriter.write(webHdfs, "unit-base", "unit-payload".getBytes(), "unit-hdfsfilename");

		Mockito.verify(webHdfs, Mockito.times(1)).fileStatus(Mockito.anyString());
		Mockito.verify(webHdfs, Mockito.times(1)).mkdir(Mockito.anyString());
		Mockito.verify(webHdfs, Mockito.times(0)).append(Mockito.anyString(), Mockito.any(InputStream.class));
		Mockito.verify(webHdfs, Mockito.times(4)).createAndWrite(Mockito.anyString(), Mockito.any(InputStream.class));
	}

	@Test(expectedExceptions = IOException.class)
	public void testWriteCreateFailsThrowsException()
			throws InterruptedException, ClientProtocolException, IOException {
		gen(200, 404, 0, true);
		WebHdfsWriter webHdfsWriter = new WebHdfsWriter();
		ReflectionTestUtils.setField(webHdfsWriter, "sleepTime", 1);
		webHdfsWriter.write(webHdfs, "unit-base", "unit-payload".getBytes(), "unit-hdfsfilename");

		Mockito.verify(webHdfs, Mockito.times(1)).fileStatus(Mockito.anyString());
		Mockito.verify(webHdfs, Mockito.times(1)).mkdir(Mockito.anyString());
		Mockito.verify(webHdfs, Mockito.times(0)).append(Mockito.anyString(), Mockito.any(InputStream.class));
		Mockito.verify(webHdfs, Mockito.times(4)).createAndWrite(Mockito.anyString(), Mockito.any(InputStream.class));
	}

	@Test(expectedExceptions = IOException.class)
	public void testWriteMkdirFails() throws InterruptedException, ClientProtocolException, IOException {
		gen(404, 0, 0);
		WebHdfsWriter webHdfsWriter = new WebHdfsWriter();
		ReflectionTestUtils.setField(webHdfsWriter, "sleepTime", 1);
		webHdfsWriter.write(webHdfs, "unit-base", "unit-payload".getBytes(), "unit-hdfsfilename");

		Mockito.verify(webHdfs, Mockito.times(1)).mkdir(Mockito.anyString());
		Mockito.verify(webHdfs, Mockito.times(0)).fileStatus(Mockito.anyString());
		Mockito.verify(webHdfs, Mockito.times(0)).append(Mockito.anyString(), Mockito.any(InputStream.class));
		Mockito.verify(webHdfs, Mockito.times(0)).createAndWrite(Mockito.anyString(), Mockito.any(InputStream.class));
	}

	private void gen(int mkdirStatusCode, int fileStatusStatusCode, int appendCreateStatusCode, boolean throwException)
			throws ClientProtocolException, IOException, InterruptedException {
		webHdfs = Mockito.mock(WebHdfs.class);

		HttpResponse mkdirHttpResponse = Mockito.mock(HttpResponse.class);
		Mockito.when(webHdfs.mkdir(Mockito.anyString())).thenReturn(mkdirHttpResponse);
		StatusLine mkdirStatusLine = Mockito.mock(StatusLine.class);
		Mockito.when(mkdirHttpResponse.getStatusLine()).thenReturn(mkdirStatusLine);
		Mockito.when(mkdirStatusLine.getStatusCode()).thenReturn(mkdirStatusCode);

		HttpResponse httpResponse = Mockito.mock(HttpResponse.class);
		StatusLine statusLine = Mockito.mock(StatusLine.class);
		Mockito.when(httpResponse.getStatusLine()).thenReturn(statusLine);
		Mockito.when(statusLine.getStatusCode()).thenReturn(fileStatusStatusCode);

		HttpResponse appendOrCreateHttpResponse = Mockito.mock(HttpResponse.class);
		StatusLine statusLine1 = Mockito.mock(StatusLine.class);
		Mockito.when(appendOrCreateHttpResponse.getStatusLine()).thenReturn(statusLine1);
		Mockito.when(statusLine1.getStatusCode()).thenReturn(appendCreateStatusCode);

		Mockito.when(webHdfs.fileStatus(Mockito.anyString())).thenReturn(httpResponse);
		if (throwException)
			Mockito.when(webHdfs.createAndWrite(Mockito.anyString(), Mockito.any(InputStream.class)))
					.thenThrow(Mockito.mock(IOException.class));
		else
			Mockito.when(webHdfs.createAndWrite(Mockito.anyString(), Mockito.any(InputStream.class)))
					.thenReturn(appendOrCreateHttpResponse);
		Mockito.when(webHdfs.append(Mockito.anyString(), Mockito.any(InputStream.class)))
				.thenReturn(appendOrCreateHttpResponse);
	}

	private void gen(int mkdirStatusCode, int fileStatusStatusCode, int appendCreateStatusCode)
			throws ClientProtocolException, IOException, InterruptedException {
		gen(mkdirStatusCode, fileStatusStatusCode, appendCreateStatusCode, false);
	}

}
