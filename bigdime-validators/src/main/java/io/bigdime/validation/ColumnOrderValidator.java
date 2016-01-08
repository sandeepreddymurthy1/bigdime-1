package io.bigdime.validation;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.hcatalog.common.HCatException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import io.bigdime.adaptor.metadata.MetadataAccessException;
import io.bigdime.adaptor.metadata.MetadataStore;
import io.bigdime.adaptor.metadata.model.Attribute;
import io.bigdime.adaptor.metadata.model.Entitee;
import io.bigdime.adaptor.metadata.model.Metasegment;
import io.bigdime.alert.Logger;
import io.bigdime.alert.LoggerFactory;
import io.bigdime.alert.Logger.ALERT_CAUSE;
import io.bigdime.alert.Logger.ALERT_SEVERITY;
import io.bigdime.alert.Logger.ALERT_TYPE;
import io.bigdime.core.ActionEvent;
import io.bigdime.core.config.AdaptorConfig;
import io.bigdime.core.constants.ActionEventHeaderConstants;
import io.bigdime.core.validation.DataValidationException;
import io.bigdime.core.validation.Factory;
import io.bigdime.core.validation.ValidationResponse;
import io.bigdime.core.validation.Validator;
import io.bigdime.core.validation.ValidationResponse.ValidationResult;
import io.bigdime.libs.hive.common.Column;
import io.bigdime.libs.hive.metadata.TableMetaData;
import io.bigdime.libs.hive.table.HiveTableManger;
@Component
@Factory(id = "column_order", type = ColumnOrderValidator.class)
public class ColumnOrderValidator implements Validator{
	
	private HiveTableManger hiveTableManager;
	
	private Properties props = new Properties();
	
	private Metasegment metasegment;
	
	@Autowired
	private MetadataStore metadataStore;
	
	private Entitee entitee;
	
	private String name;
	
	private static final Logger logger = LoggerFactory.getLogger(ColumnOrderValidator.class);
	
	/**
	 * This method is to validate whether hive column order is same as source column order or not
	 * 
	 * @param actionEvent
	 * @return PASSED if hive column order matches source column order
	 * 		   otherwise return FAILED when met first mismatch order, return INCOMPLETE_SETUP if hive table is not found
	 * 
	 * @author Rita Liu
	 * 
	 */
	
	@Override
	public ValidationResponse validate(ActionEvent actionEvent)
			throws DataValidationException {
		TableMetaData table = null;
		int port =0;
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
		
		boolean columnOrder = false;
		String wrongOrder = null;
		try {
			if(hiveTableManager.isTableCreated(hiveDBName, hiveTableName)){
				table = hiveTableManager.getTableMetaData(hiveDBName, hiveTableName);
				List<Column> partitionColumnList = table.getPartitionColumns();
				List<Column> hiveColumnList = table.getColumns();
				hiveColumnList.addAll(partitionColumnList);
				Set<Attribute> metadataColumns = null;
				metasegment = metadataStore.getAdaptorMetasegment(AdaptorConfig.getInstance().getAdaptorContext().getAdaptorName(), ActionEventHeaderConstants.SCHEMA_TYPE_HIVE, hiveTableName);
				if(metasegment == null || metasegment.getEntitees() == null || metasegment.getEntitees().size() == 0) {
					logger.alert(AdaptorConfig.getInstance().getAdaptorContext().getAdaptorName(), ALERT_TYPE.OTHER_ERROR, ALERT_CAUSE.VALIDATION_ERROR, ALERT_SEVERITY.MAJOR, "No such metasegment for table {} found in metastore", hiveTableName);
					validationPassed.setValidationResult(ValidationResult.INCOMPLETE_SETUP);
				}else{
					entitee = metasegment.getEntity(hiveTableName);
					metadataColumns = entitee.getAttributes();
					List<Attribute> sourceColumnList = new ArrayList<Attribute>(metadataColumns);
					StringBuilder st = new StringBuilder();
					for(int i = 0; i< metadataColumns.size(); i++){
						st.append(sourceColumnList.get(i).getAttributeName().toLowerCase() + ":" + sourceColumnList.get(i).getAttributeType().toLowerCase() + " ");
					}
					for(int i = 0; i < hiveColumnList.size(); i++){
						if(hiveColumnList.get(i).getName().compareToIgnoreCase(sourceColumnList.get(i).getAttributeName())==0){
							columnOrder = true;
						}else{
							columnOrder=false;
							wrongOrder = hiveColumnList.get(i).getName() + ":" + hiveColumnList.get(i).getType();
							break;
						}
					}
					if(columnOrder){	
						logger.info(AdaptorConfig.getInstance().getAdaptorContext().getAdaptorName(), "Column Order match", "Hive table {} in {} database has the same column order as source, columns: {}", hiveTableName, hiveDBName, hiveColumnList.toString());
						validationPassed.setValidationResult(ValidationResult.PASSED);
					}else{
						logger.debug(AdaptorConfig.getInstance().getAdaptorContext().getAdaptorName(), "Wrong order", "Hive Column list: {} and Source Column list: {}", hiveColumnList.toString(), st.toString());
						logger.warn(AdaptorConfig.getInstance().getAdaptorContext().getAdaptorName(), "Column Order mismatch", "Hive table {} in {} database has different column order as source, first happened at {}", hiveTableName, hiveDBName, wrongOrder);
						validationPassed.setValidationResult(ValidationResult.FAILED);
					}
				}
			}else{
				logger.warn(AdaptorConfig.getInstance().getAdaptorContext().getAdaptorName(), "Hive table not exist", "Hive table {} is not found in hive database {}", hiveTableName, hiveDBName);
				validationPassed.setValidationResult(ValidationResult.INCOMPLETE_SETUP);
			}
		} catch (HCatException e) {
			logger.warn(AdaptorConfig.getInstance().getAdaptorContext().getAdaptorName(), "HCatException", "Exception occurred while getting column order from hive", e);
			throw new DataValidationException("Exception during getting column order from hive");
		} catch (MetadataAccessException ex){
			logger.warn(AdaptorConfig.getInstance().getAdaptorContext().getAdaptorName(), "MetadataAccessException", "Exception occurred while getting column order from metastore", ex);
			throw new DataValidationException("Exception during getting column order from metastore");
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
					"Checking Null/Empty for provided arugument: " + key, " {} is null/empty", key);
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
