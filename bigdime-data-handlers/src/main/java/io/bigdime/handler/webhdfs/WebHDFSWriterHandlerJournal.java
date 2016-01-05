/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.handler.webhdfs;

import java.util.List;

import io.bigdime.core.ActionEvent;
import io.bigdime.core.handler.SimpleJournal;

public class WebHDFSWriterHandlerJournal extends SimpleJournal {
	private String currentHdfsPath = "";
	private String currentHdfsPathWithName = "";
	private String currentHdfsFileName = "";
	private int recordCount = -1;
	private List<ActionEvent> eventList;

	public String getCurrentHdfsPath() {
		return currentHdfsPath;
	}

	public void setCurrentHdfsPath(String currentHdfsPath) {
		this.currentHdfsPath = currentHdfsPath;
	}

	public int getRecordCount() {
		return recordCount;
	}

	public void setRecordCount(int recordCount) {
		this.recordCount = recordCount;
	}

	public void incrementRecordCount() {
		this.recordCount++;
	}

	public List<ActionEvent> getEventList() {
		return eventList;
	}

	public void setEventList(List<ActionEvent> eventList) {
		this.eventList = eventList;
	}

	public void reset() {
		setRecordCount(0);
		setEventList(null);
		setCurrentHdfsPath("");
	}

	public String getCurrentHdfsPathWithName() {
		return currentHdfsPathWithName;
	}

	public void setCurrentHdfsPathWithName(String currentHdfsPathWithName) {
		this.currentHdfsPathWithName = currentHdfsPathWithName;
	}

	public String getCurrentHdfsFileName() {
		return currentHdfsFileName;
	}

	public void setCurrentHdfsFileName(String currentHdfsFileName) {
		this.currentHdfsFileName = currentHdfsFileName;
	}

	@Override
	public String toString() {
		return "WebHDFSWriterHandlerJournal [currentHdfsPath=" + currentHdfsPath + ", recordCount=" + "]";
	}
}