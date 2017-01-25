package io.bigdime.handler.file;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang.NotImplementedException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;


import io.bigdime.alert.LoggerFactory;
import io.bigdime.alert.Logger.ALERT_CAUSE;
import io.bigdime.alert.Logger.ALERT_SEVERITY;
import io.bigdime.alert.Logger.ALERT_TYPE;
import io.bigdime.core.ActionEvent.Status;
import io.bigdime.core.ActionEvent;
import io.bigdime.core.AdaptorConfigurationException;
import io.bigdime.core.HandlerException;
import io.bigdime.core.InvalidValueConfigurationException;
import io.bigdime.core.commons.AdaptorLogger;
import io.bigdime.core.commons.PropertyHelper;
import io.bigdime.core.config.AdaptorConfigConstants;
import io.bigdime.core.handler.AbstractHandler;
import io.bigdime.core.handler.SimpleJournal;
import io.bigdime.core.runtimeinfo.RuntimeInfo;
import io.bigdime.core.runtimeinfo.RuntimeInfoStore;
import io.bigdime.core.runtimeinfo.RuntimeInfoStoreException;
import io.bigdime.handler.webservice.WebserviceInputDescriptor;

/**
 * 
 * 
 * This Handler would fetch the data from a given webservice endpoint
 * 
 * @formatter:off
 * application-context ,Specifies the application context of the end point URI
 * param_list, Specifies a Map of key value pairs which would be the arguments for the end point URL
 * dynamic_param_list, Specifies a Map of key value pairs which if provided would be the dynamic arguments for the end point URI
 * authorization, if provided, would be used in the header of webservice request
 * contenttype, if provided, would be used in the header of webservice request
 * accept, if provided, would be used in the header of webservice request
 * 
 * 
 *  @author Sandeep Reddy,Murthy
 */
@Component
@Scope("prototype")
public class FileWebserviceHandler extends AbstractHandler{

	private static final AdaptorLogger logger = new AdaptorLogger(
			LoggerFactory.getLogger(FileWebserviceHandler.class));
	@Autowired
	private RuntimeInfoStore<RuntimeInfo> runtimeInfoStore;
	private String applicationContext;
	private LinkedHashMap<String, String> paramMap;
	private LinkedHashMap<String,String> dynamicParamMap;	
	private Set<String> headers;
	
	
	private String handlerPhase = "";
	private String entityName;
	
	@SuppressWarnings("unchecked")
	@Override
	public void build() throws AdaptorConfigurationException {
		handlerPhase = "building FileInputStreamHandler";
		logger.info(handlerPhase, "handler_name={} handler_id={} \"properties={}\"", getName(), getId(),
				getPropertyMap());
		super.build();
		@SuppressWarnings("unchecked")
		Entry<String, String> srcDescInputs = (Entry<String, String>) getPropertyMap()
				.get(AdaptorConfigConstants.SourceConfigConstants.SRC_DESC);
		if (srcDescInputs == null) {
			throw new InvalidValueConfigurationException("src-desc can't be null");
		}
		logger.debug(handlerPhase, "entity:fileNamePattern={} input_field_name={}", srcDescInputs.getKey(),
				srcDescInputs.getValue());
		applicationContext = PropertyHelper.getStringProperty(getPropertyMap(), "applicationContext");
		if(applicationContext==null)
			throw new InvalidValueConfigurationException("applicationContext can't be null");
		paramMap= (LinkedHashMap<String, String>) getPropertyMap().get("paramMap");
		if(paramMap==null)
			throw new InvalidValueConfigurationException("paramMap can't be null");
		dynamicParamMap=(LinkedHashMap<String, String>) getPropertyMap().get("dynamicParamMap");
		if(dynamicParamMap==null)
			throw new InvalidValueConfigurationException("dynamicParamMap can't be null");
	    headers=(Set<String>) getPropertyMap().get("headers");
		String url=getURL(applicationContext,paramMap,dynamicParamMap,headers);
//		String url=PropertyHelper.getStringProperty(getPropertyMap(), "URL Location");
//		logger.debug(handlerPhase, "url={}", url);
		
		WebserviceInputDescriptor webserviceInputDescriptor=new WebserviceInputDescriptor();
		String[] srcDesc = webserviceInputDescriptor.parseSourceDescriptor(srcDescInputs.getKey());
		String entityName = srcDesc[0];
		String fileNamePattern = srcDesc[1];
		logger.debug(handlerPhase, "entityName={} fileNamePattern={}", entityName, fileNamePattern);
		
	}
	
	@Override
	public Status process() throws HandlerException {
		handlerPhase = "processing FileInputStreamHandler";	
		incrementInvocationCount();
		try {
			Status status = preProcess();
			if (status == Status.BACKOFF) {
				logger.debug(handlerPhase, "returning BACKOFF");
				return status;
			}
			return doProcess();
		} catch (IOException e) {
			logger.alert(ALERT_TYPE.INGESTION_FAILED, ALERT_CAUSE.APPLICATION_INTERNAL_ERROR, ALERT_SEVERITY.BLOCKER,
					"error during reading file", e);
			throw new HandlerException("Unable to process message from file", e);
		} catch (RuntimeInfoStoreException e) {
			throw new HandlerException("Unable to process message from file", e);
		}
	}
	long dirtyRecordCount = 0;
	List<RuntimeInfo> dirtyRecords;
	private boolean processingDirty = false;
	private Status preProcess() throws IOException, RuntimeInfoStoreException, HandlerException {
		if (isFirstRun()) {
			dirtyRecords = getAllStartedRuntimeInfos(runtimeInfoStore, entityName);
			if (dirtyRecords != null && !dirtyRecords.isEmpty()) {
				dirtyRecordCount = dirtyRecords.size();
				logger.warn(handlerPhase,
						"_message=\"dirty records found\" handler_id={} dirty_record_count=\"{}\" entityName={}",
						getId(), dirtyRecordCount, entityName);
			} else {
				logger.info(handlerPhase, "_message=\"no dirty records found\" handler_id={}", getId());
			}
		}
//		if (readAllFromFile()) {
//			setNextFileToProcess();
//			if (file == null) {
//				logger.info(handlerPhase, "_message=\"no file to process\" handler_id={} ", getId());
//				return Status.BACKOFF;
//			}
//			fileLength = file.length();
//			logger.info(handlerPhase, "_message=\"got a new file to process\" handler_id={} file_length={}", getId(),
//					fileLength);
//			if (fileLength == 0) {
//				logger.info(handlerPhase, "_message=\"file is empty\" handler_id={} ", getId());
//				return Status.BACKOFF;
//			}
//			getSimpleJournal().setTotalSize(fileLength);
//			fileChannel = file.getChannel();
//			return Status.READY;
//		}
		return Status.READY;
	}
	private Status doProcess() throws IOException, HandlerException, RuntimeInfoStoreException {
		return null;
	}
	
	private boolean isFirstRun() {
		return getInvocationCount() == 1;
	}
	
	
	private String getURL(String applicationContext,LinkedHashMap<String,String> param_list,LinkedHashMap<String,String> dynamic_param_list,Set<String> headers){
     String url=applicationContext;
     int paramCount=0;
     int dynamicParamCount=0;
     for(Map.Entry<String, String> entry :param_list.entrySet()){
    	 if(paramCount==0){
    		 url=url+"?"+entry.getKey()+"="+entry.getValue();
    		 paramCount++;
    	 }else{
    		 url=url+"&"+entry.getKey()+"="+entry.getValue();
    	 }
     }
     for(Map.Entry<String, String> entry :dynamic_param_list.entrySet()){
    	 if(dynamicParamCount==0){
    		 url=url+"&"+entry.getKey()+"="+entry.getValue();
    		 dynamicParamCount++;
    	 }else{
    		 url=url+"&"+entry.getKey()+"="+entry.getValue();
    	 }
     }
     return url;
	}
	
	@Override
	public void shutdown() {
		super.shutdown();
		shutdown0();
	}

	private void shutdown0() {
		throw new NotImplementedException();
	}

	private long getTotalReadFromJournal() throws HandlerException {
		return getSimpleJournal().getTotalRead();
	}

	private long getTotalSizeFromJournal() throws HandlerException {
		return getSimpleJournal().getTotalSize();
	}

	private SimpleJournal getSimpleJournal() throws HandlerException {
		return getNonNullJournal(SimpleJournal.class);
	}

	private boolean readAllFromBackLog() throws HandlerException {
		if (getTotalReadFromJournal() == getTotalSizeFromJournal()) {
			return true;
		}
		return false;
	}

	protected String getHandlerPhase() {
		return handlerPhase;
	}

	public String getEntityName() {
		return entityName;
	}

}
