package io.bigdime.validation;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.hcatalog.common.HCatException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import io.bigdime.alert.Logger;
import io.bigdime.alert.Logger.ALERT_CAUSE;
import io.bigdime.alert.Logger.ALERT_SEVERITY;
import io.bigdime.alert.Logger.ALERT_TYPE;
import io.bigdime.alert.LoggerFactory;
import io.bigdime.core.ActionEvent;
import io.bigdime.core.config.AdaptorConfig;
import io.bigdime.core.constants.ActionEventHeaderConstants;
import io.bigdime.core.validation.DataValidationException;
import io.bigdime.core.validation.Factory;
import io.bigdime.core.validation.ValidationResponse;
import io.bigdime.core.validation.Validator;
import io.bigdime.core.validation.ValidationResponse.ValidationResult;
import io.bigdime.adaptor.metadata.*;
import io.bigdime.adaptor.metadata.model.Attribute;
import io.bigdime.adaptor.metadata.model.Entitee;
import io.bigdime.adaptor.metadata.model.Metasegment;
import io.bigdime.libs.hive.common.Column;
import io.bigdime.libs.hive.metadata.TableMetaData;
import io.bigdime.libs.hive.table.HiveTableManger;

@Component
@Factory(id = "column_count", type = ColumnCountValidator.class)
public class ColumnCountValidator implements Validator{
	
	private static final Logger logger = LoggerFactory.getLogger(ColumnCountValidator.class);
	
	private HiveTableManger hiveTableManager;
	
	private Properties props = new Properties();
	
	@Autowired
	private MetadataStore metadataStore;
	
	private Metasegment metasegment;
	
	private Entitee entitee;
	
	private String name;
	
	/**
	 * This method is to compare column count of specific hive table to column count of specific table in metastore(Source)
	 *      
	 * @param actionEvent
	 * @return ValidationResponse
	 * Three conditions:
	 * 		1. hive column count = source column count --- PASSED
	 * 		2. hive column count > source column count --- FAILED (BAD situation)
	 *      3. hive column count < source column count --- COLUMN_COUNT_MISMATCH
	 * return INCOMPLETE_SETUP if hive table is not found or no such metadata found in metastore
	 * 
	 * @author Rita Liu
	 */

	@Override
	public ValidationResponse validate(ActionEvent actionEvent)
			throws DataValidationException {
		TableMetaData table = null;
		int port = 0;
		int sourceColumnCount = 0;
		ValidationResponse validationPassed = new ValidationResponse();
		validationPassed.setValidationResult(ValidationResult.FAILED);
		String hiveHost = actionEvent.getHeaders().get(ActionEventHeaderConstants.HIVE_HOST_NAME);
		String hivePort = actionEvent.getHeaders().get(ActionEventHeaderConstants.HIVE_PORT);
		String hiveDBName = actionEvent.getHeaders().get(ActionEventHeaderConstants.HIVE_DB_NAME);
		String hiveTableName = actionEvent.getHeaders().get(ActionEventHeaderConstants.HIVE_TABLE_NAME);
		
		
		checkNullStrings(ActionEventHeaderConstants.HIVE_HOST_NAME, hiveHost);
		checkNullStrings(ActionEventHeaderConstants.PORT, hivePort);
		try {
			 port = Integer.parseInt(hivePort);
			
		} catch (NumberFormatException e) {
			logger.warn(AdaptorConfig.getInstance().getAdaptorContext().getAdaptorName(), "NumberFormatException",
					"Illegal port number input({}) while parsing string to integer", hivePort);
			throw new NumberFormatException();
		}
		
		checkNullStrings(ActionEventHeaderConstants.HIVE_DB_NAME, hiveDBName);
		checkNullStrings(ActionEventHeaderConstants.HIVE_TABLE_NAME, hiveTableName);

		//connect to hive
				 
		props.put(HiveConf.ConfVars.METASTOREURIS, "thrift://" + hiveHost + ":" + port);
//		props.put(HiveConf.ConfVars.PREEXECHOOKS.varname, "");
//		props.put(HiveConf.ConfVars.POSTEXECHOOKS.varname, "");
//		props.put(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, Boolean.FALSE.toString());
//		props.putAll(actionEvent.getHeaders());
				
		hiveTableManager = HiveTableManger.getInstance(props);
		try {
			if(hiveTableManager.isTableCreated(hiveDBName, hiveTableName)){
				table = hiveTableManager.getTableMetaData(hiveDBName, hiveTableName);
				List<Column> hiveColumnList = table.getColumns();
				hiveColumnList.addAll(table.getPartitionColumns());
				int hiveColumnCount = hiveColumnList.size();
				Set<Attribute> metadataColumns = null;
				metasegment = metadataStore.getAdaptorMetasegment(AdaptorConfig.getInstance().getAdaptorContext().getAdaptorName(), ActionEventHeaderConstants.SCHEMA_TYPE_HIVE, hiveTableName);
				if(metasegment == null || metasegment.getEntitees() == null || metasegment.getEntitees().size() == 0) {
					logger.alert(AdaptorConfig.getInstance().getAdaptorContext().getAdaptorName(), ALERT_TYPE.OTHER_ERROR, ALERT_CAUSE.VALIDATION_ERROR, ALERT_SEVERITY.MAJOR, "No such metasegment for table {} found in metastore", hiveTableName);
					validationPassed.setValidationResult(ValidationResult.INCOMPLETE_SETUP);
				}else{
					entitee = metasegment.getEntity(hiveTableName);
					metadataColumns = entitee.getAttributes();
					sourceColumnCount = metadataColumns.size();
				
					if(hiveColumnCount == sourceColumnCount){
						logger.info(AdaptorConfig.getInstance().getAdaptorContext().getAdaptorName(), "Column Count match", "Hive table {} has the same number of columns as source in {} database, column count: {}", hiveTableName, hiveDBName, hiveColumnCount);
						validationPassed.setValidationResult(ValidationResult.PASSED);
					}
					//hive has more column than source --- hive column count > source column count
					if(hiveColumnCount > sourceColumnCount){
						List<Column> hiveList = new ArrayList<Column>(hiveColumnList);
						List<Attribute> sourceList = new ArrayList<Attribute>(metadataColumns);
						Iterator<Column> iterator = hiveList.iterator();
						while(iterator.hasNext()){
							Column column = iterator.next();
							for(Attribute attribute : sourceList){
								if(attribute.getAttributeName().compareToIgnoreCase(column.getName())==0){
									iterator.remove();
								}
							}
						}
					
						//additional columns list
						StringBuilder sb = new StringBuilder();
						if(!hiveList.isEmpty()){
							for (Column col : hiveList) {
								sb.append(col + " ");
							}
							hiveList.clear();
						}
						int diff = Math.abs(hiveColumnCount - sourceColumnCount);
						logger.debug(AdaptorConfig.getInstance().getAdaptorContext().getAdaptorName(), "column count validate failed", "source column count({}) is not match as hive column count({})", sourceColumnCount, hiveColumnCount);
						logger.warn(AdaptorConfig.getInstance().getAdaptorContext().getAdaptorName(), "Additional hive column found", "Hive table {} has {} more column than source in {} database, column(s): {}", hiveTableName, diff, hiveDBName, sb.toString(), "This is really BAD!!");
						validationPassed.setValidationResult(ValidationResult.FAILED);
						sb.setLength(0);
					}
					//hive has less column than source --- hive column count < source column count
					if(hiveColumnCount < sourceColumnCount){
						List<Attribute> sourceColumnList = new ArrayList<Attribute>(metadataColumns);
						Iterator<Attribute> iterator = sourceColumnList.iterator();
						while (iterator.hasNext()) {
							Attribute attribute = iterator.next();
							for (Column column : hiveColumnList) {
								if (attribute.getAttributeName().compareToIgnoreCase(column.getName())==0) {
									iterator.remove();
								}
							}
						}
					
						//additional columns list
						StringBuilder strBuilder = new StringBuilder();
						if(!sourceColumnList.isEmpty()){
							for (Attribute attr : sourceColumnList) {
								strBuilder.append(attr.getAttributeName().toLowerCase() + ":"+ attr.getAttributeType().toLowerCase() + " ");
							}
							sourceColumnList.clear();
						}
						int diff = Math.abs(hiveColumnCount - sourceColumnCount);
						logger.debug(AdaptorConfig.getInstance().getAdaptorContext().getAdaptorName(), "column count mismatch", "source column count({}) is not match as hive column count({})", sourceColumnCount, hiveColumnCount);
						logger.warn(AdaptorConfig.getInstance().getAdaptorContext().getAdaptorName(), "Additional source column found", "Source table {} has {} more column than hive in {} database, column(s): {}", hiveTableName, diff, hiveDBName, strBuilder.toString());
						validationPassed.setValidationResult(ValidationResult.COLUMN_COUNT_MISMATCH);
						strBuilder.setLength(0);
					}
				}
			}else{
				logger.warn(AdaptorConfig.getInstance().getAdaptorContext().getAdaptorName(), "Hive table not exist", "Hive table {} is not found in hive databases {}", hiveTableName, hiveDBName);
				validationPassed.setValidationResult(ValidationResult.INCOMPLETE_SETUP);
			}
		} catch (HCatException e) {
			logger.warn(AdaptorConfig.getInstance().getAdaptorContext().getAdaptorName(), "HCatException", "Exception occurred while getting column count from hive", e);
			throw new DataValidationException("Exception during getting column count from hive");
		} catch (MetadataAccessException ex){
			logger.warn(AdaptorConfig.getInstance().getAdaptorContext().getAdaptorName(), "MetadataAccessException", "Exception occurred while getting column count from metastore", ex);
			throw new DataValidationException("Exception during getting column count from metastore");
		}
		
		return validationPassed;
	}
	
	/**
	 * This method to check provided argument from ActionEvent if null/empty or
	 * not, if null/empty, give warning and throw IllegalArgumentException.
	 * 
	 * @param key
	 * @param value
	 * @throws IllegalArgumentException
	 * 
	 * @author Rita Liu
	 * 
	 */
	private void checkNullStrings(String key, String value) {
		if (StringUtils.isBlank(value)) {
			logger.warn(AdaptorConfig.getInstance().getAdaptorContext().getAdaptorName(),
					"Checking Null/Empty for provided arugument: " + key, "{} is null/empty", key);
			throw new IllegalArgumentException();
		}
	}

	@Override
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
}
