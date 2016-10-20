/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.handler.file;

import io.bigdime.core.handler.SimpleJournal;

/**
 * 
 * @author Rita Liu
 *
 */
public class ZipFileHandlerJournal extends SimpleJournal {

	private int totalEntries;
	private int readEntries;
	private String entryName;
	private String srcFileName;
	private String zipFileName;

	public int getTotalEntries() {
		return totalEntries;
	}

	public void setTotalEntries(int totalEntries) {
		this.totalEntries = totalEntries;
	}

	public int getReadEntries() {
		return readEntries;
	}

	public void setReadEntries(int readEntries) {
		this.readEntries = readEntries;
	}
	
	public String getEntryName() {
		return entryName;
	}

	public void setEntryName(String entryName) {
		this.entryName = entryName;
	}
	
	public String getSrcFileName() {
		return srcFileName;
	}

	public void setSrcFileName(String srcFileName) {
		this.srcFileName = srcFileName;
	}
	
	public String getZipFileName() {
		return zipFileName;
	}

	public void setZipFileName(String zipFileName) {
		this.zipFileName = zipFileName;
	}
	
	public void reset() {
		setTotalRead(0);
		setEventList(null);
		setTotalSize(0);
		setTotalEntries(0);
		setReadEntries(0);
		setEntryName(null);
		setSrcFileName(null);
		setZipFileName(null);
	}

}
