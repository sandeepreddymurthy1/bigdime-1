/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.libs.hdfs;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.http.HttpResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This component can be used to write to hdfs.
 * 
 * @author mnamburi, Neeraj Jain, jbrinnand
 *
 */

public class WebHdfsWriter {
	private static final Logger logger = LoggerFactory.getLogger(WebHdfsWriter.class);

	private long sleepTime = 3000;
	public static final String FORWARD_SLASH = "/";

	/**
	 * Check to see whether the file exists or not.
	 * 
	 * @param filePath
	 *            absolute filepath
	 * @return true if the file exists, false otherwise
	 * @throws WebHDFSSinkException
	 */
	private boolean fileExists(WebHdfs webHdfs, String filePath) throws WebHDFSSinkException {
		try {
			HttpResponse response = webHdfs.fileStatus(filePath);
			webHdfs.releaseConnection();
			if (response.getStatusLine().getStatusCode() == 200 || response.getStatusLine().getStatusCode() == 201) {
				logger.debug("file exists", "responseCode={} filePath={} responseMessage={}",
						response.getStatusLine().getStatusCode(), filePath, response.getStatusLine().getReasonPhrase());
				return true;
			} else if (response.getStatusLine().getStatusCode() == 404) {
				logger.debug("file does not exist", "responseCode={} filePath={} responseMessage={}",
						response.getStatusLine().getStatusCode(), filePath, response.getStatusLine().getReasonPhrase());
				return false;
			} else {
				logger.warn("file existence not known, responseCode={} filePath={} responseMessage={}",
						response.getStatusLine().getStatusCode(), filePath, response.getStatusLine().getReasonPhrase());
				throw new WebHDFSSinkException("file existence not known, responseCode="
						+ response.getStatusLine().getStatusCode() + ", filePath=" + filePath);
			}
		} catch (Exception e) {
			logger.warn("file creation",
					"_message=\"WebHdfs File Status Failed: The Sink or Data Writer could not check the status of the file:\" retry={} error={}",
					e);
			throw new WebHDFSSinkException("could not get the file status", e);
		}
	}

	/**
	 * Create the directory specifed by folderPath parameter.
	 * 
	 * @param webHdfs
	 *            {@link WebHdfs} object that actually invokes the call to write
	 *            to hdfs
	 * @param folderPath
	 *            absolute path representing the directory that needs to be
	 *            created
	 * @throws IOException
	 *             if there was any problem in executing mkdir command or if the
	 *             command returned anything by 200 or 201
	 */
	public void createDirectory(WebHdfs webHdfs, String folderPath) throws IOException {
		HttpResponse response = webHdfs.mkdir(folderPath);
		webHdfs.releaseConnection();
		if (response.getStatusLine().getStatusCode() == 201 || response.getStatusLine().getStatusCode() == 200) {
			return;
		} else {
			throw new WebHDFSSinkException("unable to create directory:" + folderPath + ", reasonCode="
					+ response.getStatusLine().getStatusCode()+", reason="+ response.getStatusLine().getReasonPhrase());	
		}
	}

	private void writeToWebHDFS(WebHdfs webHdfs, String filePath, byte[] payload, boolean appendMode)
			throws IOException {
		HttpResponse response = null;
		boolean isSuccess = false;

		int retry = 0;
		String exceptionReason = null;
		try {
			do {
				InputStream is = null;
				retry++;
				try {
					is = new ByteArrayInputStream(payload);

					if (appendMode) {
						response = webHdfs.append(filePath, is);
					} else {
						response = webHdfs.createAndWrite(filePath, is);
					}
					if (response.getStatusLine().getStatusCode() == 201
							|| response.getStatusLine().getStatusCode() == 200) {
						isSuccess = true;
					} else {
						exceptionReason = response.getStatusLine().getStatusCode() + ":"
								+ response.getStatusLine().getReasonPhrase();
						logger.warn(
								"_message=\"WebHdfs Data Write Failed\" status_code={} reason={} retry={} filePath={} host={}",
								response.getStatusLine().getStatusCode(), response.getStatusLine().getReasonPhrase(),
								retry, filePath,webHdfs.getHost());
						Thread.sleep(sleepTime * (retry + 1));
					}
				} catch (Exception e) {
					exceptionReason = e.getMessage();
					logger.warn(
							"_message=\"WebHdfs Data Write Failed: The Sink or Data Writer could not write the data to HDFS.:\" retry={} filePath={} host ={} error={}",
							retry, filePath, webHdfs.getHost() ,e);
				} finally {
					if (is != null)
						is.close();
				}
			} while (!isSuccess && retry <= 3);
		} finally {
			webHdfs.releaseConnection();
		}
		if (!isSuccess) {
			logger.error(
					"_message=\"WebHdfs Data Write Failed After 3 retries : The Sink or Data Writer could not write the data to HDFS.:\"");
			throw new WebHDFSSinkException(exceptionReason);
		}
	}

	/**
	 * Facade method to write data to a file in hdfs. If the file or directory
	 * does not exist, this method creates them.
	 * 
	 * @param baseDir
	 *            directory hosting the file where payload needs to be written
	 *            to
	 * @param payload
	 *            data to be written to file
	 * @param hdfsFileName
	 *            name of the file on hdfs
	 * @throws IOException
	 *             if there was any problem in writing the data
	 */
	public void write(WebHdfs webHdfs, String baseDir, byte[] payload, String hdfsFileName) throws IOException {
		createDirectory(webHdfs, baseDir);
		String filePath = baseDir + FORWARD_SLASH + hdfsFileName;

		boolean fileCreated = fileExists(webHdfs, filePath);
		writeToWebHDFS(webHdfs, filePath, payload, fileCreated);
	}

}
