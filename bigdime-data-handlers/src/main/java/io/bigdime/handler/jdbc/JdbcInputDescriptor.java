/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.handler.jdbc;

import java.util.ArrayList;
import java.util.List;

import io.bigdime.alert.LoggerFactory;
import io.bigdime.core.InputDescriptor;
import io.bigdime.core.commons.AdaptorLogger;

import org.apache.commons.lang.NotImplementedException;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

/**
 * This class process the src-desc to parse input json node
 * format sql query
 * 
 * @author Pavan Sabinikari, Rita Liu
 *
 */
@Component
public class JdbcInputDescriptor implements InputDescriptor<String>{

	private static final AdaptorLogger logger = new AdaptorLogger(LoggerFactory.getLogger(JdbcInputDescriptor.class));

	@Value("${split.size}") private String splitSize;
	
	private String query;
	private String inputType;
	private String inputValue;
//	private int instances;
	private String includeFilter;
	private String excludeFilter;
	private String entityName;
	private String databaseName;
	private String targetEntityName;
	private String targetDBName;
	private String incrementedBy;
	private String incrementedColumnType;
	private String columnName;
	private String partition;
	private String fieldDelimeter;
	private String rowDelimeter;
	private String snapshot;
	
	private List<String> columnList = new ArrayList<String>();
	
	public List<String> getColumnList() {
		return columnList;
	}

	public void setColumnList(List<String> columnList) {
		this.columnList = columnList;
	}

	@Override
	public String getNext(List<String> availableInputDescriptors,
			String lastInputDescriptor) throws NotImplementedException{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void parseDescriptor(String jsonStr) {
		if (jsonStr == null || jsonStr.length() <= 0)
			throw new IllegalArgumentException("descriptor can't be null or empty");
		try {
			JSONObject jsonObject = new JSONObject(jsonStr);
		
		inputType = jsonObject.getString("inputType");
		inputValue = jsonObject.getString("inputValue");
		incrementedBy = jsonObject.getString("incrementedBy");
//		instances = jsonObject.getInt("instances");
		if(inputType.equalsIgnoreCase("database")){
			databaseName = inputValue;
			if(!jsonObject.isNull("include"))
				includeFilter = jsonObject.getString("include");
			if(!jsonObject.isNull("exclude"))
				excludeFilter = jsonObject.getString("exclude");
		}
		if (!jsonObject.isNull("partitionedColumns"))
			partition = jsonObject.getString("partitionedColumns");
		if(partition == null || partition.length() ==0)
			partition = incrementedBy;
		if (!jsonObject.isNull("fieldDelimeter"))
			fieldDelimeter = jsonObject.getString("rowDelimeter");
		if (fieldDelimeter == null || fieldDelimeter.length() == 0)
			fieldDelimeter = JdbcConstants.CONTROL_A_DELIMETER;
		if (!jsonObject.isNull("rowDelimeter"))
			rowDelimeter = jsonObject.getString("rowDelimeter");
		if (rowDelimeter == null || rowDelimeter.length() == 0)
			rowDelimeter = JdbcConstants.NEW_LINE_DELIMETER;
		if(inputType.equalsIgnoreCase("table")){
			entityName = inputValue;
			if (!jsonObject.isNull("targetTableName"))
				targetEntityName = jsonObject.getString("targetTableName");
			if (targetEntityName == null || targetEntityName.length() == 0)
				targetEntityName = entityName;
			databaseName = jsonObject.getString("databaseName");
		}
		if (!jsonObject.isNull("snapshot"))
			snapshot = jsonObject.getString("snapshot");
		} catch (JSONException e) {
			logger.warn("Json Exception", "unable to parse descriptor jsonStr={}", jsonStr);
		}

		logger.debug("Retrieved Jdbc Reader Handler Configurations","inputType={} inputValue={} incrementedBy={} include={} exclude={} partitionedColumns={} fieldDelimeter={} rowDelimeter={}",
				inputType, inputValue, incrementedBy, includeFilter, excludeFilter, partition, fieldDelimeter, rowDelimeter);		
	}
	
	public String formatQuery(String inputType, String inputValue, String driverName) throws JdbcHandlerException {

		if(inputType.equalsIgnoreCase("database")){
			if(driverName.indexOf(JdbcConstants.ORACLE_DRIVER)> JdbcConstants.INTEGER_CONSTANT_ZERO){
				if(!inputValue.isEmpty()){
					query = "SELECT table_name FROM all_tables WHERE owner ='"+inputValue+"'";
				} else {
					query = "SELECT table_name FROM all_tables";
				}
			}
		}
		if(inputType.equalsIgnoreCase("table")){
			if(!incrementedBy.isEmpty()){
				if(!databaseName.isEmpty()){
					query = "SELECT * FROM "+databaseName+"."+inputValue+" WHERE "+incrementedBy+" > "+ JdbcConstants.QUERY_PARAMETER +" ORDER BY "+incrementedBy+" ASC";
				} else{
					query = "SELECT * FROM "+inputValue+" WHERE "+incrementedBy+" > "+ JdbcConstants.QUERY_PARAMETER +" ORDER BY "+incrementedBy+" ASC";
				}
			} else{
				throw new JdbcHandlerException("incrementedBy value cannot be null");
			}
			if(driverName.indexOf(JdbcConstants.ORACLE_DRIVER) > JdbcConstants.INTEGER_CONSTANT_ZERO && Integer.parseInt(splitSize) > JdbcConstants.INTEGER_CONSTANT_ZERO) {
				query = "SELECT t.* FROM ("+query+") t WHERE ROWNUM <"+splitSize;
			}
		}
		if(inputType.equalsIgnoreCase("sqlQuery")){
			query = inputValue;
			if(driverName.indexOf(JdbcConstants.ORACLE_DRIVER) > JdbcConstants.INTEGER_CONSTANT_ZERO && Integer.parseInt(splitSize) > JdbcConstants.INTEGER_CONSTANT_ZERO) {
				query = "SELECT t.* FROM ("+query+") t WHERE ROWNUM <"+splitSize;
			}
		}
		return query;

	}
	
	public String formatProcessTableQuery(String dbname, String tableName, String driverName) throws JdbcHandlerException{
		if(!incrementedBy.isEmpty()){
			if(!dbname.isEmpty()){
				query = "SELECT * FROM "+dbname+"."+tableName+" WHERE "+incrementedBy+" > "+ JdbcConstants.QUERY_PARAMETER +" ORDER BY "+incrementedBy+" ASC";
			} else{
				query = "SELECT * FROM "+tableName+" WHERE "+incrementedBy+" > "+ JdbcConstants.QUERY_PARAMETER +" ORDER BY "+incrementedBy+" ASC";
			}
		} else{
			throw new JdbcHandlerException("incrementedBy value cannot be null");
		}
		if(driverName.indexOf(JdbcConstants.ORACLE_DRIVER) > JdbcConstants.INTEGER_CONSTANT_ZERO && Integer.parseInt(splitSize) > JdbcConstants.INTEGER_CONSTANT_ZERO) {
			query = "SELECT t.* FROM ("+query+") t WHERE ROWNUM <"+splitSize;
		}
		return query;
	}

	public String getQuery() {
		return query;
	}

	public void setQuery(String query) {
		this.query = query;
	}
	
	public String getInputType() {
		return inputType;
	}

	public void setInputType(String inputType) {
		this.inputType = inputType;
	}

	public String getInputValue() {
		return inputValue;
	}

	public void setInputValue(String inputValue) {
		this.inputValue = inputValue;
	}

//	public int getInstances() {
//		return instances;
//	}
//
//	public void setInstances(int instances) {
//		this.instances = instances;
//	}

	public String getIncludeFilter() {
		return includeFilter;
	}

	public void setIncludeFilter(String includeFilter) {
		this.includeFilter = includeFilter;
	}

	public String getExcludeFilter() {
		return excludeFilter;
	}

	public void setExcludeFilter(String excludeFilter) {
		this.excludeFilter = excludeFilter;
	}

	public String getEntityName() {
		return entityName;
	}

	public void setEntityName(String entityName) {
		this.entityName = entityName;
	}
	
	public String getDatabaseName() {
		return databaseName;
	}

	public void setDatabaseName(String databaseName) {
		this.databaseName = databaseName;
	}
	
	public String getTargetEntityName() {
		return targetEntityName;
	}

	public void setTargetEntityName(String targetEntityName) {
		this.targetEntityName = targetEntityName;
	}

	public String getTargetDBName() {
		return targetDBName;
	}

	public void setTargetDBName(String targetDBName) {
		this.targetDBName = targetDBName;
	}
	
	public String getIncrementedBy() {
		return incrementedBy;
	}

	public void setIncrementedBy(String incrementedBy) {
		this.incrementedBy = incrementedBy;
	}
	
	public String getIncrementedColumnType() {
		return incrementedColumnType;
	}

	public void setIncrementedColumnType(String incrementedColumnType) {
		this.incrementedColumnType = incrementedColumnType;
	}
	
	public String getColumnName() {
		return columnName;
	}

	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}


	public String getPartition() {
		return partition;
	}

	public void setPartition(String partition) {
		this.partition = partition;
	}

	public String getFieldDelimeter() {
		return fieldDelimeter;
	}

	public void setFieldDelimeter(String fieldDelimeter) {
		this.fieldDelimeter = fieldDelimeter;
	}

	public String getRowDelimeter() {
		return rowDelimeter;
	}

	public void setRowDelimeter(String rowDelimeter) {
		this.rowDelimeter = rowDelimeter;
	}

	public String getSnapshot() {
		return snapshot;
	}

	public void setSnapshot(String snapshot) {
		this.snapshot = snapshot;
	}

	

}