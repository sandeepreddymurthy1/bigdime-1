/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.runtimeinfo.DTO;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.UniqueConstraint;


@Entity
@Table(name = "RUNTIME_PROPERTY",uniqueConstraints = @UniqueConstraint(columnNames ={ "RUNTIME_INFO_ID","PROPERTY_NAME"}))
public class RuntimePropertyDTO {
	
	@Column(name = "RUNTIME_PROPERTY_ID")
	@Id
	@GeneratedValue(strategy = GenerationType.AUTO)
	private int runtimePropertyId;
	@Column(name = "PROPERTY_NAME")
	private String key;
	@Column(name = "PROPERTY_VALUE")
	private String value;
	
	
	public RuntimePropertyDTO() {
		super();
		// TODO Auto-generated constructor stub
	}


	public RuntimePropertyDTO(String key, String value) {
		super();
		this.key = key;
		this.value = value;
	}


	public int getRuntimePropertyId() {
		return runtimePropertyId;
	}


	public void setRuntimePropertyId(int runtimePropertyId) {
		this.runtimePropertyId = runtimePropertyId;
	}


	public String getKey() {
		return key;
	}


	public void setKey(String key) {
		this.key = key;
	}


	public String getValue() {
		return value;
	}


	public void setValue(String value) {
		this.value = value;
	}


	@Override
	public String toString() {
		return "RuntimeProperty [runtimePropertyId=" + runtimePropertyId
				+ ", key=" + key + ", value=" + value + "]";
	}
	
	
	

}
