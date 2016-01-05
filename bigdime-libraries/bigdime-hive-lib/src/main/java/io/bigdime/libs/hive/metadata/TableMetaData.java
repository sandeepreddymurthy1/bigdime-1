/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.libs.hive.metadata;

import io.bigdime.libs.hive.common.Column;
import io.bigdime.libs.hive.common.ColumnMetaDataUtil;

import java.util.List;
import java.util.Map;

import org.apache.hive.hcatalog.api.HCatTable;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.codehaus.jackson.annotate.JsonIgnore;

/**
 * @author jbrinnand
 */
public class TableMetaData {
	
	public TableMetaData()
	{
		
	}
	
	public TableMetaData(HCatTable table) {
		this.table = table;
	}
	
	
	private HCatTable table;
	
	@JsonIgnore
	public HCatTable getTable() {
		return table;
	}

	public void setTable(HCatTable table) {
		this.table = table;
	}
	
	public String getDatabaseName() {
		return table.getDbName();
	}	
	public String getName() {
		return table.getTableName();
	}
	public String getLocation() {
		return table.getLocation();
	}
	public String getInputFileFormat() {
		return table.getInputFileFormat();
	}
	public String getOutputFileFormat() {
		return table.getOutputFileFormat();
	}	
	public String getSerdeLib() {
		return table.getSerdeLib();
	}		
	public Map<String, String> getProperties() {
		return table.getTblProps();
	}	
	public List<Column> getColumns() {
		List<HCatFieldSchema> hcatColumns = table.getCols();
		List<Column> columns = ColumnMetaDataUtil.addColumns(hcatColumns); 
		return columns;
	}	
	public List<Column> getPartitionColumns() {
		List<HCatFieldSchema> hcatColumns = table.getPartCols();
		List<Column> columns = ColumnMetaDataUtil.addColumns(hcatColumns); 
		return columns;
	}	
}
