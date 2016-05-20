/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.handler.jdbc;

import java.io.Serializable;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.sql.DataSource;

import io.bigdime.adaptor.metadata.MetadataAccessException;
import io.bigdime.adaptor.metadata.MetadataStore;
import io.bigdime.adaptor.metadata.model.Metasegment;
import io.bigdime.alert.LoggerFactory;
import io.bigdime.alert.Logger.ALERT_CAUSE;
import io.bigdime.alert.Logger.ALERT_SEVERITY;
import io.bigdime.alert.Logger.ALERT_TYPE;
import io.bigdime.core.ActionEvent;
import io.bigdime.core.ActionEvent.Status;
import io.bigdime.core.AdaptorConfigurationException;
import io.bigdime.core.HandlerException;
import io.bigdime.core.InvalidValueConfigurationException;
import io.bigdime.core.commons.AdaptorLogger;
import io.bigdime.core.config.AdaptorConfig;
import io.bigdime.core.config.AdaptorConfigConstants;
import io.bigdime.core.constants.ActionEventHeaderConstants;
import io.bigdime.core.handler.AbstractHandler;
import io.bigdime.core.runtimeinfo.RuntimeInfo;
import io.bigdime.core.runtimeinfo.RuntimeInfoStore;
import io.bigdime.core.runtimeinfo.RuntimeInfoStoreException;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.SerializationUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Scope;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;


/**
 * This class processes each table record in database based on sql query, 
 * inserting into Metastore, updating Runtime Info
 * 
 *@author Murali Namburi, Pavan Sabinikari, Rita Liu
 *
 */

@Component
@Scope("prototype")
public class JdbcTableReaderHandler extends AbstractHandler {
	
	private static final AdaptorLogger logger = new AdaptorLogger(
			LoggerFactory.getLogger(JdbcDBSchemaReaderHandler.class));
	
	@Autowired
	private JdbcInputDescriptor jdbcInputDescriptor;
	@Autowired
	private DataSource lazyConnectionDataSourceProxy;
	@Autowired
	private MetadataStore metadataStore;
	@Autowired
	RuntimeInfoStore<RuntimeInfo> runTimeInfoStore;
	@Value("${database.driverClassName}")
	private String driverName;
	@Value("${hiveDBName}")
	private String hiveDBName;
	@Value("${split.size}")
	private String splitSize;
	private static String initialRuntimeDateEntry="1900-01-01 00:00:00";
	private JdbcTemplate jdbcTemplate;
	private String handlerPhase = null;
	private String jsonStr = null;
	private String columnValue;
	private String highestIncrementalColumnValue;
	private String processTableSql;
	@Autowired
	private JdbcMetadataManagement jdbcMetadataManagment;
	
	@Override
	public void build() throws AdaptorConfigurationException {
		handlerPhase = "building Jdbc Table Reader Handler";
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
		logger.info(handlerPhase,
				"handler_id={} handler_name={} properties={}", getId(),
				getName(), getPropertyMap());
		Status adaptorThreadStatus = null;
		try {
			adaptorThreadStatus = preProcess();
			return adaptorThreadStatus;
		} catch (RuntimeInfoStoreException e) {
			logger.alert(ALERT_TYPE.INGESTION_FAILED,ALERT_CAUSE.APPLICATION_INTERNAL_ERROR,
					ALERT_SEVERITY.BLOCKER,
					"\"jdbcAdaptor RuntimeInfoStore exception\"  TableName = {} error={}",
					jdbcInputDescriptor.getEntityName(), e.toString());
			throw new HandlerException("Unable to process records from table", e);
		} catch (JdbcHandlerException e) {
			logger.alert(ALERT_TYPE.INGESTION_FAILED,ALERT_CAUSE.APPLICATION_INTERNAL_ERROR,
					ALERT_SEVERITY.BLOCKER,"\"jdbcAdaptor jdbcHandler exception\" TableName={} error={}",
					jdbcInputDescriptor.getEntityName(), e.toString());
			throw new HandlerException("Unable to process records from table", e);
		}
	}
	
	public void setDataSource(DataSource dataSource) {
		this.lazyConnectionDataSourceProxy = dataSource;
	}
	
	public Status preProcess() throws HandlerException, RuntimeInfoStoreException, JdbcHandlerException {
		jdbcTemplate = new JdbcTemplate(lazyConnectionDataSourceProxy);
		List<ActionEvent> actionEvents = getHandlerContext().getEventList();
		for(ActionEvent actionEvent : actionEvents){
			String entityName = actionEvent.getHeaders().get(ActionEventHeaderConstants.TARGET_ENTITY_NAME);
			jdbcInputDescriptor.setTargetEntityName(entityName);
			jdbcInputDescriptor.setEntityName(entityName);
		}
		jdbcInputDescriptor.setTargetDBName(hiveDBName);
		processTableSql = jdbcInputDescriptor.formatProcessTableQuery(jdbcInputDescriptor.getDatabaseName(), jdbcInputDescriptor.getEntityName(), driverName);
		if(!StringUtils.isEmpty(processTableSql)){
			logger.debug("Formatted Jdbc Table Reader Handler Query", "processTableSql={}", processTableSql);
			// Get Source Metadata..
			try {
				Metasegment metasegment = jdbcMetadataManagment.getSourceMetadata(
							jdbcInputDescriptor, jdbcTemplate);
				jdbcMetadataManagment.setColumnList(jdbcInputDescriptor,
							metasegment);

				logger.debug("Jdbc Table Reader Handler source column list",
							"ColumnList={}", jdbcInputDescriptor.getColumnList());
				
				if (jdbcInputDescriptor.getColumnList().size() == JdbcConstants.INTEGER_CONSTANT_ZERO)
					throw new HandlerException(
						"Unable to retrieve the column list for the table Name = "
									+ jdbcInputDescriptor.getEntityName());
				
				// throw Exception:
				// if the incrementedBy column doesn't exist in the table..
				if (jdbcInputDescriptor.getIncrementedBy().length() > JdbcConstants.INTEGER_CONSTANT_ZERO
						&& jdbcInputDescriptor.getIncrementedColumnType() == null) {
					throw new HandlerException(
							"IncrementedBy Value doesn't exist in the table column list");
				}
				// Put into Metadata...
				metadataStore.put(metasegment);
			} catch (MetadataAccessException e) {
				throw new HandlerException(
					"Unable to put metadata to Metastore for the table Name = "
								+ jdbcInputDescriptor.getEntityName());
			}
		}
		boolean processFlag = false;
		// Check if Runtime details Exists..
		if (getOneQueuedRuntimeInfo(runTimeInfoStore,
				jdbcInputDescriptor.getEntityName()) == null) {
			// Insert into Runtime Data...
			if (jdbcInputDescriptor.getIncrementedBy() != null
					&& jdbcInputDescriptor.getIncrementedBy().length() > JdbcConstants.INTEGER_CONSTANT_ZERO) {
				HashMap<String, String> properties = new HashMap<String, String>();
				if (driverName
						.equalsIgnoreCase(JdbcConstants.ORACLE_DRIVER_NAME)
						&& (jdbcInputDescriptor.getIncrementedColumnType()
								.equalsIgnoreCase("DATE") || jdbcInputDescriptor
								.getIncrementedColumnType().equalsIgnoreCase(
										"TIMESTAMP"))) {
					properties.put(jdbcInputDescriptor.getIncrementedBy(),
							initialRuntimeDateEntry);
				} else {
					properties.put(jdbcInputDescriptor.getIncrementedBy(),
							JdbcConstants.INTEGER_CONSTANT_ZERO + "");
				}
				boolean runtimeInsertionFlag = updateRuntimeInfo(
						runTimeInfoStore, jdbcInputDescriptor.getEntityName(),
						jdbcInputDescriptor.getIncrementedColumnType(),
						RuntimeInfoStore.Status.QUEUED, properties);

				logger.info(
						"Jdbc Table Reader Handler inserting Runtime data",
						"tableName={} PropertyKey={} PropertyValue={} status={}",
						jdbcInputDescriptor.getEntityName(),
						jdbcInputDescriptor.getIncrementedBy(), columnValue,
						runtimeInsertionFlag);
			}
			processFlag = processRecords();
		} else {
			logger.debug(
					"Jdbc Table Reader Handler processing an existing table ",
					"tableName={}", jdbcInputDescriptor.getEntityName());
			processFlag = processRecords();
		}
		if (processFlag) {
			return Status.CALLBACK;
		} else {
			return Status.READY;
		}
	}
	
	/**
	 * This methods fetches the number of records from the source.
	 * 
	 * @return
	 */
	public boolean processRecords() {
		boolean moreRecordsExists = true;
		jdbcTemplate = new JdbcTemplate(lazyConnectionDataSourceProxy);
		String repoColumnValue = getCurrentColumnValue();
		logger.debug("Jdbc Table Reader Handler in process records",
				"Latest Incremented Repository Value= {}", repoColumnValue);
		if (repoColumnValue != null)
			columnValue = repoColumnValue;
		logger.info("Jdbc Table Reader Handler in processing ",
				"actual processTableSql={} latestIncrementalValue={}", processTableSql, columnValue);
		List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
		jdbcTemplate.setQueryTimeout(120);
		long startTime = System.currentTimeMillis();
		if (processTableSql.contains(JdbcConstants.QUERY_PARAMETER)) {
			if (columnValue != null) {
				
					if (driverName.indexOf(JdbcConstants.ORACLE_DRIVER) > JdbcConstants.INTEGER_CONSTANT_ZERO
							&& (jdbcInputDescriptor.getIncrementedColumnType()
									.equalsIgnoreCase("DATE") || jdbcInputDescriptor.getIncrementedColumnType()
									.equalsIgnoreCase("TIMESTAMP"))) {
						
						rows = jdbcTemplate
								.queryForList(
										processTableSql,
										new Object[] { Timestamp.valueOf(columnValue)});
						;
												
					} else
						rows = jdbcTemplate.queryForList(processTableSql,
								new Object[] { columnValue });
				
			} else
				rows = jdbcTemplate.queryForList(processTableSql,
						new Object[] { JdbcConstants.INTEGER_CONSTANT_ZERO });
		} else {
			rows = jdbcTemplate.queryForList(processTableSql);
		}
		long endTime = System.currentTimeMillis();
		logger.info(
				"Jdbc Table Reader Handler during processing records",
				"Time taken to fetch records from table ={} from a columnValue={} with a fetchSize={} is {} milliseconds",
				jdbcInputDescriptor.getEntityName(), columnValue, splitSize,
				(endTime - startTime));
		logger.debug("Jdbc Table Reader Handler during processing records","columns in the tableName={} are {}",
				jdbcInputDescriptor.getEntityName(),jdbcInputDescriptor.getColumnList());

		// Process each row to HDFS...
		if (rows.size() > 0)
			processEachRecord(rows);
		else {
			logger.info("Jdbc Table Reader Handler during processing records",
					"No more rows found for query={}", processTableSql);
			moreRecordsExists = false;
		}
		// Assigning the current incremental value..
		if (jdbcInputDescriptor.getIncrementedBy().length() > JdbcConstants.INTEGER_CONSTANT_ZERO
				&& highestIncrementalColumnValue != null) {
			
			HashMap<String, String> properties = new HashMap<String, String>();
			properties.put(jdbcInputDescriptor.getIncrementedBy(), highestIncrementalColumnValue);
			boolean highestValueStatus=false;
			try {
				highestValueStatus = updateRuntimeInfo(
						runTimeInfoStore,
						jdbcInputDescriptor.getEntityName(),
						jdbcInputDescriptor.getIncrementedColumnType(),
						RuntimeInfoStore.Status.QUEUED, properties);
			} catch (RuntimeInfoStoreException e) {
				
				logger.alert(ALERT_TYPE.INGESTION_FAILED,
						ALERT_CAUSE.APPLICATION_INTERNAL_ERROR,
						ALERT_SEVERITY.BLOCKER,
						"\"jdbcAdaptor RuntimeInfo Update exception\" TableName={} error={}",
						jdbcInputDescriptor.getEntityName(), e.toString());
			}
			logger.info("Jdbc Table Reader Handler saved highest incremental value",
					"incremental column saved status(true/false)? {}", highestValueStatus);
			columnValue = highestIncrementalColumnValue;
		}
		if (jdbcInputDescriptor.getIncrementedBy().length() == JdbcConstants.INTEGER_CONSTANT_ZERO
				|| jdbcInputDescriptor.getIncrementedBy() == null)
			moreRecordsExists = false;
		return moreRecordsExists;
	}

	/**
	 * This method sets each record in Action Event for further Data cleansing
	 * 
	 * @param rows
	 */
	private void processEachRecord(List<Map<String, Object>> rows) {
        String maxValue = null;
        List<Map<String, Object>> maxList = new ArrayList<Map<String, Object>>();
		List<ActionEvent> actionEvents = new ArrayList<ActionEvent>();
		for (Map<String, Object> row : rows) {
			// Preparing data to hand over to DataCleansing Handler
			byte[] body = SerializationUtils.serialize((Serializable) row);
			if (body != null) {
				
				ActionEvent actionEvent = new ActionEvent();
				actionEvent.setBody(body);
				actionEvents.add(actionEvent);

				highestIncrementalColumnValue = row.get(jdbcInputDescriptor
						.getColumnList().get(
								jdbcInputDescriptor.getColumnList().indexOf(
										jdbcInputDescriptor.getColumnName())))
						+ "";
			}
		}
		for(int i = rows.size()-1; i>=0; i--){
			maxValue = rows.get(i).get(jdbcInputDescriptor
					.getColumnList().get(
							jdbcInputDescriptor.getColumnList().indexOf(
									jdbcInputDescriptor.getColumnName())))
					+ "";
			if(maxValue.equals(highestIncrementalColumnValue)){
				maxList.add(rows.get(i));
			}
		}
		List<ActionEvent> ignoredActionEventList = getIgnoredBatchRecords(processTableSql.replaceAll(">","="),maxList,highestIncrementalColumnValue);
		if(ignoredActionEventList!=null && ignoredActionEventList.size() > 0){
  		  actionEvents.addAll(ignoredActionEventList);
  	  	}
		if (actionEvents.size() > 0){
			getHandlerContext().setEventList(actionEvents);
			processIt(actionEvents);
		}
	}
	
	/**
	 * Processes each record to Channel
	 * 
	 * @param actionEvents
	 * @return
	 * @throws HandlerException
	 */
	@SuppressWarnings("unchecked")
	private void processIt(List<ActionEvent> actionEvents){
		long startTime = System.currentTimeMillis();
		while (!actionEvents.isEmpty()) {
			ActionEvent actionEvent = actionEvents.remove(0);
			
			byte[] data = actionEvent.getBody();
			StringBuffer sbFormattedRowContent = new StringBuffer();
			StringBuffer sbHiveNonPartitionColumns = new StringBuffer();
			String datePartition = null;
			if(data.length>0){
				Map<String, Object> row = (Map<String, Object>) SerializationUtils
					.deserialize(data);
	
				if (row != null) {
					for (int columnNamesListCount = 0; columnNamesListCount < jdbcInputDescriptor
						.getColumnList().size(); columnNamesListCount++) {
						// Ensure each field doesn't have rowlineDelimeter
						Pattern p = Pattern.compile(JdbcConstants.FIELD_CHARACTERS_TO_REPLACE);
						StringBuffer sbfMatcher = new StringBuffer();
					
						Matcher m = p.matcher(sbfMatcher.append(row.get(jdbcInputDescriptor.getColumnList().
										get(columnNamesListCount))));
						StringBuffer sbFormattedField = new StringBuffer();
						while (m.find()) {
							m.appendReplacement(sbFormattedField, JdbcConstants.FIELD_CHARACTERS_REPLACE_BY);
						}
						m.appendTail(sbFormattedField);
						sbFormattedRowContent.append(sbFormattedField);
						if (columnNamesListCount != jdbcInputDescriptor.getColumnList().size() - 1)
							sbFormattedRowContent.append(jdbcInputDescriptor.getFieldDelimeter());
	
						if (jdbcInputDescriptor.getIncrementedColumnType().indexOf("DATE") >= JdbcConstants.INTEGER_CONSTANT_ZERO
							    || jdbcInputDescriptor.getIncrementedColumnType().indexOf("TIMESTAMP") >= JdbcConstants.INTEGER_CONSTANT_ZERO) {
	
							if (jdbcInputDescriptor.getColumnList().get(columnNamesListCount)
								.equalsIgnoreCase(jdbcInputDescriptor.getIncrementedBy())) {
							
								datePartition = Timestamp.valueOf(row
										.get(jdbcInputDescriptor.getColumnList().get(columnNamesListCount))
										.toString()).toString().substring(0,10).replaceAll("-","");
							}
						}
					}
	
					//Each Row is delimited by "\n"
				
					sbFormattedRowContent.append(jdbcInputDescriptor.getRowDelimeter());
					actionEvent.setBody(sbFormattedRowContent.toString().getBytes());
	
				
					sbHiveNonPartitionColumns.append(ActionEventHeaderConstants.ENTITY_NAME);
					actionEvent.getHeaders().put(ActionEventHeaderConstants.ENTITY_NAME.toUpperCase(),
						             jdbcInputDescriptor.getTargetEntityName());
				
					actionEvent.getHeaders().put(ActionEventHeaderConstants.LINES_TERMINATED_BY,
									 jdbcInputDescriptor.getRowDelimeter());
				
					actionEvent.getHeaders().put(ActionEventHeaderConstants.FIELDS_TERMINATED_BY,
									 jdbcInputDescriptor.getFieldDelimeter());
	
					// Partition Dates logic..
					if (jdbcInputDescriptor.getSnapshot() != null
									  && jdbcInputDescriptor.getSnapshot().equalsIgnoreCase("YES")) {
						// get current date and format it to string
						actionEvent.getHeaders().put(ActionEventHeaderConstants.DATE, 
										 dateFormatHolder.get().format(new Date()));
					} else {
						if (datePartition != null) {
							actionEvent.getHeaders().put(ActionEventHeaderConstants.DATE, datePartition);
						} else
							actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_PARTITION_REQUIRED, "false");
					
					}
				
				
					if (actionEvent.getHeaders().get(ActionEventHeaderConstants.ENTITY_NAME.toUpperCase()) != null)
						actionEvent.getHeaders().put(ActionEventHeaderConstants.ENTITY_NAME,
										 actionEvent.getHeaders().get(ActionEventHeaderConstants.ENTITY_NAME.toUpperCase()));
				
				
					if (actionEvent.getHeaders().get(ActionEventHeaderConstants.DATE) != null){
						actionEvent.getHeaders().put(ActionEventHeaderConstants.INPUT_DESCRIPTOR, 
										 actionEvent.getHeaders().get(ActionEventHeaderConstants.DATE));
					} else
						actionEvent.getHeaders().put(ActionEventHeaderConstants.INPUT_DESCRIPTOR, 
										 actionEvent.getHeaders().get(ActionEventHeaderConstants.ENTITY_NAME) + JdbcConstants.WITH_NO_PARTITION);
				    
					actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_NON_PARTITION_NAMES, 
						    		 sbHiveNonPartitionColumns.toString());
					/*
					 * Check for outputChannel map. get the eventList of channels.
					 * check the criteria and put the message.
					 */
					if (getOutputChannel() != null) {
						getOutputChannel().put(actionEvent);
					}
	
				}
			}
		}
		long endTime = System.currentTimeMillis();
	
		logger.info("Data Cleansing during processing records","Time taken to process action Events is ={} milliseconds",
										(endTime - startTime));
	}
	
	
	/**
	 * dateFormatHolder.get() used to get an instance of SimpleDateFormat object
	 */
	private static final ThreadLocal<SimpleDateFormat> dateFormatHolder = new ThreadLocal<SimpleDateFormat>() {
		@Override
		protected SimpleDateFormat initialValue() {
			return new SimpleDateFormat("yyyyMMdd");
		}
	};
	
	private String getCurrentColumnValue() {
		String currentIncrementalColumnValue = null;
		RuntimeInfo runtimeInfo = null;
		try {
			runtimeInfo = getOneQueuedRuntimeInfo(runTimeInfoStore,jdbcInputDescriptor.getEntityName());
		} catch (RuntimeInfoStoreException e) {
			
			logger.alert(ALERT_TYPE.INGESTION_FAILED, ALERT_CAUSE.APPLICATION_INTERNAL_ERROR,
					ALERT_SEVERITY.BLOCKER,
					"\"jdbcAdaptor RuntimeInfoStore exception while getting current value\" TableName:{} error={}",
					jdbcInputDescriptor.getEntityName(),e.toString());
		}
		if (runtimeInfo != null)
			currentIncrementalColumnValue = runtimeInfo.getProperties().get(
					jdbcInputDescriptor.getIncrementedBy());
		return currentIncrementalColumnValue;
	}
	
	@SuppressWarnings("unchecked")
	public List<ActionEvent> getIgnoredBatchRecords(String ignoredRowsSql,List<Map<String,Object>> list, String conditionValue){
		List<ActionEvent> ignoredActionEvents = new ArrayList<ActionEvent>();
		List<Map<String, Object>> ignoredRows = new ArrayList<Map<String, Object>>();
		if (ignoredRowsSql.contains(JdbcConstants.QUERY_PARAMETER)) {
			if (conditionValue != null) {
					if (driverName.indexOf(JdbcConstants.ORACLE_DRIVER) > JdbcConstants.INTEGER_CONSTANT_ZERO
							&& (jdbcInputDescriptor.getIncrementedColumnType()
									.equalsIgnoreCase("DATE") || jdbcInputDescriptor.getIncrementedColumnType()
									.equalsIgnoreCase("TIMESTAMP"))) {
						
						ignoredRows = jdbcTemplate
								.queryForList(
										ignoredRowsSql,
										new Object[] { Timestamp.valueOf(conditionValue)});
						;
												
					} else
						ignoredRows = jdbcTemplate.queryForList(ignoredRowsSql,
								new Object[] { conditionValue });
			} else
				ignoredRows = jdbcTemplate.queryForList(ignoredRowsSql,
						new Object[] { JdbcConstants.INTEGER_CONSTANT_ZERO });
		} else {
			ignoredRows = jdbcTemplate.queryForList(ignoredRowsSql);
		}
		if (ignoredRows.size() > JdbcConstants.INTEGER_CONSTANT_ZERO){
			List<Map<String, Object>> diffList = (List<Map<String, Object>>) CollectionUtils.disjunction(list, ignoredRows);
			if(diffList.size() > JdbcConstants.INTEGER_CONSTANT_ZERO){
				for(Map<String, Object> ignoredRow: diffList){
					byte[] body = SerializationUtils.serialize((Serializable) ignoredRow);
					if (body != null) {
						ActionEvent actionEvent = new ActionEvent();
						actionEvent.setBody(body);
						ignoredActionEvents.add(actionEvent);
					}
				}
			}
		}else {
			logger.info("Jdbc Table Reader Handler during Ignored processing records",
					"No more rows found for query={}", ignoredRowsSql);
		}
		if(ignoredActionEvents.size() > 0) return ignoredActionEvents;
		return null;
	}
}
