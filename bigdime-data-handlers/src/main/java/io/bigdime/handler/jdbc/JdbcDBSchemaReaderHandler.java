/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.handler.jdbc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import javax.sql.DataSource;

import io.bigdime.alert.LoggerFactory;
import io.bigdime.alert.Logger.ALERT_CAUSE;
import io.bigdime.alert.Logger.ALERT_SEVERITY;
import io.bigdime.alert.Logger.ALERT_TYPE;
import io.bigdime.core.ActionEvent.Status;
import io.bigdime.core.AdaptorConfigurationException;
import io.bigdime.core.HandlerException;
import io.bigdime.core.InvalidValueConfigurationException;
import io.bigdime.core.commons.AdaptorLogger;
import io.bigdime.core.config.AdaptorConfigConstants;
import io.bigdime.core.handler.AbstractHandler;

import org.apache.commons.lang.StringUtils;
import org.json.JSONException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Scope;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

/**
 * This class process all the table names from a database by using sql query,
 * and then filter the processed table names based on "include" filter value if has,
 * otherwise, processed table names will be the all table names
 * 
 * @author Rita Liu
 *
 */

@Component
@Scope("prototype")
public class JdbcDBSchemaReaderHandler extends AbstractHandler {
	private static final AdaptorLogger logger = new AdaptorLogger(
			LoggerFactory.getLogger(JdbcDBSchemaReaderHandler.class));
	
	@Autowired
	private JdbcInputDescriptor jdbcInputDescriptor;
	@Autowired
	private DataSource lazyConnectionDataSourceProxy;
	@Value("${database.driverClassName}")
	private String driverName;
	private JdbcTemplate jdbcTemplate;
	private String handlerPhase = null;
	private String jsonStr = null;
	private String dbSql;
	private static Map<String, Object> allTableMap = new HashMap<String, Object>();
	private static List<String> allTableNameList;
	private List<String> processTable = null;
	private String currentTableToProcess = null;
	int index =0;
	
	@Override
	public void build() throws AdaptorConfigurationException {
		handlerPhase = "building Jdbc DB Schema Reader Handler";
		super.build();
		logger.info(handlerPhase,
				"handler_id={} handler_name={} properties={}", getId(),
				getName(), getPropertyMap());

		@SuppressWarnings("unchecked")
		Entry<String, String> srcDescInputs = (Entry<String, String>) getPropertyMap()
				.get(AdaptorConfigConstants.SourceConfigConstants.SRC_DESC);
		if (srcDescInputs == null) {
			throw new InvalidValueConfigurationException(
					"src-desc can't be null");
		}
		logger.info(handlerPhase,
				"entity:fileNamePattern={} input_field_name={}",
				srcDescInputs.getKey(), srcDescInputs.getValue());
		jsonStr = srcDescInputs.getKey();
		try {

			jdbcInputDescriptor.parseDescriptor(jsonStr);
		} catch (IllegalArgumentException ex) {
			throw new InvalidValueConfigurationException(
					"incorrect value specified in src-desc, value must be in json string format");
		} 
	}

	@Override
	public Status process() throws HandlerException {
		handlerPhase = "processing JdbcDBSchemaReaderHandler";
		logger.info(handlerPhase,
				"handler_id={} handler_name={} properties={}", getId(),
				getName(), getPropertyMap());
		Status status = null;
		incrementInvocationCount();
		try {
			status = preProcess();
			if(status == Status.BACKOFF){
				return status;
			}
			return doProcess();
		} catch (JSONException e) {
			logger.alert(ALERT_TYPE.INGESTION_FAILED,ALERT_CAUSE.APPLICATION_INTERNAL_ERROR,
					ALERT_SEVERITY.BLOCKER,"\"jdbcAdaptor json formatter exception\" jsonString={} error={}",
					jsonStr, e.toString());
			throw new HandlerException("");
		} catch (JdbcHandlerException e) {
			logger.alert(ALERT_TYPE.INGESTION_FAILED,ALERT_CAUSE.APPLICATION_INTERNAL_ERROR,
					ALERT_SEVERITY.BLOCKER,"\"jdbcAdaptor jdbcHandler exception\" databaseName={} error={}",
					jdbcInputDescriptor.getDatabaseName(), e.toString());
			throw new HandlerException("");
		}
	}
	
	public void setDataSource(DataSource dataSource) {
		this.lazyConnectionDataSourceProxy = dataSource;
	}
	
	private boolean isFirstRun() {
		return getInvocationCount() == 1;
	}
	
	private List<String> getTableNameList(String sqlQuery){
		List<String> tableList = new ArrayList<String>();
		tableList = jdbcTemplate.queryForList(sqlQuery, String.class);
		return tableList;
	}
	public Status preProcess() throws JdbcHandlerException,
			JSONException, HandlerException {
		jdbcTemplate = new JdbcTemplate(lazyConnectionDataSourceProxy);
		dbSql = jdbcInputDescriptor.formatQuery(jdbcInputDescriptor.getInputType(), jdbcInputDescriptor.getInputValue(), driverName);
		logger.debug("Formatted Jdbc DB Reader Handler Query", "dbSql={}", dbSql);
		if(isFirstRun()){
			if (!StringUtils.isEmpty(dbSql)) {
				allTableNameList = getTableNameList(dbSql);
				allTableMap.put(jdbcInputDescriptor.getInputValue(), allTableNameList);
			}
			//includeFilter is not specified, process all tables in databases
			if(jdbcInputDescriptor.getIncludeFilter().isEmpty()){
				processTable = allTableNameList;
			} else{
				processTable = getFilteredTableList(allTableNameList);
			}
		}
		if(!allTableMap.containsKey(jdbcInputDescriptor.getInputValue())){
			if (!StringUtils.isEmpty(dbSql)) {
				allTableNameList = getTableNameList(dbSql);
				allTableMap.put(jdbcInputDescriptor.getInputValue(), allTableNameList);
			}
			//includeFilter is not specified, process all tables in databases
			if(jdbcInputDescriptor.getIncludeFilter().isEmpty()){
				processTable = allTableNameList;
			} else{
				processTable = getFilteredTableList(allTableNameList);
			}
		}
		
		return Status.READY;
	}
	
	private List<String> getFilteredTableList(List<String> tableList) throws HandlerException{
		String regex = jdbcInputDescriptor.getIncludeFilter();
		Pattern pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
		List<String> processTableList = new ArrayList<String>();
		for(String str : tableList){
			if(pattern.matcher(str).lookingAt()){
				processTableList.add(str);
			}
		}
		return processTableList;
	}
	
	public Status doProcess() throws JdbcHandlerException, HandlerException {
		currentTableToProcess = getNextTableToProcess(processTable);
		if(currentTableToProcess == null){
			logger.info("no table need to process", "return BACKOFF");
			return Status.BACKOFF;
		}
		jdbcInputDescriptor.setEntityName(currentTableToProcess);
		jdbcInputDescriptor.setTargetEntityName(currentTableToProcess);	
		return Status.READY;
	}
		
	private String getNextTableToProcess(List<String> processTableList){
		String currentTable = "";
		if(processTableList.size()>0){
			currentTable = processTableList.remove(0);
			return currentTable;
		} else {
			return null;
		}
	}
}
