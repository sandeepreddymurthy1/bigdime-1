/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.management.common;

public class ApplicationProperties {
	
	private String devHost;
	private String qaHost;
	private String prodHost;
	private String devPort;
	private String qaPort;
	private String prodPort;
	
	public String getDevHost() {
		return devHost;
	}
	public void setDevHost(String devHost) {
		this.devHost = devHost;
	}
	public String getQaHost() {
		return qaHost;
	}
	public void setQaHost(String qaHost) {
		this.qaHost = qaHost;
	}
	public String getProdHost() {
		return prodHost;
	}
	public void setProdHost(String prodHost) {
		this.prodHost = prodHost;
	}
	public String getDevPort() {
		return devPort;
	}
	public void setDevPort(String devPort) {
		this.devPort = devPort;
	}
	public String getQaPort() {
		return qaPort;
	}
	public void setQaPort(String qaPort) {
		this.qaPort = qaPort;
	}
	public String getProdPort() {
		return prodPort;
	}
	public void setProdPort(String prodPort) {
		this.prodPort = prodPort;
	}
	
	

}
