package io.bigdime.impl.biz.service;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import io.bigdime.alert.Logger;
import io.bigdime.alert.LoggerFactory;
import io.bigdime.hbase.client.DataInsertionSpecification;
import io.bigdime.hbase.client.DataRetrievalSpecification;
import io.bigdime.hbase.client.HbaseManager;
import io.bigdime.hbase.client.exception.HBaseClientException;
import io.bigdime.impl.biz.dao.Datahandler;
import io.bigdime.impl.biz.dao.JsonData;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Result;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.annotate.JsonSerialize;
import org.codehaus.jackson.node.ObjectNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.annotation.JsonInclude;

@Component
public class HbaseJsonDataService {
	private static final Logger logger = LoggerFactory
			.getLogger(HbaseJsonDataService.class);
	
	@Autowired
	private HbaseManager hbaseManager;
	@Value("${hbase.adaptor.table}")
	private String adaptorTable;

	/**
	 * get configTemplate of a given adaptor
	 */
	
	public JsonData getJSON(String configTemplate){
		JsonData jsonData=null;
		Get get = new Get(Bytes.toBytes(configTemplate));
		try {
			DataRetrievalSpecification.Builder dataRetrievalSpecificationBuilder = new DataRetrievalSpecification.Builder();
			DataRetrievalSpecification dataRetrievalSpecification = dataRetrievalSpecificationBuilder
					.withTableName(adaptorTable).withGet(get).build();
			hbaseManager.retreiveData(dataRetrievalSpecification);
			Result result = hbaseManager.getResult();
			ObjectMapper objMapper = new ObjectMapper();
			objMapper.setSerializationInclusion(JsonSerialize.Inclusion.NON_NULL);
			if (result != null) {				
					try {
						ObjectNode adaptor = (ObjectNode) objMapper.readTree(new String(result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("jn"))));
						jsonData=new JsonData();
						jsonData=objMapper.readValue(adaptor.toString(),JsonData.class);
					} catch (UnsupportedEncodingException e) {
						logger.warn(
								"BIGDIME-MONITORING-SERVICE",
								"The result cannot be parsed",
								e.getMessage());
					}
				}

		} catch (HBaseClientException | IOException e) {
			logger.warn(
					"BIGDIME-MONITORING-SERVICE",
					"Unable to fetch data from HBASE",
					e.getMessage());
		} catch(Exception e){
			logger.warn(
					"BIGDIME-MONITORING-SERVICE",
					"Unable to connect to HBASE",
					e.getMessage());
		}
		return jsonData;
	}
	
	/**
	 * Save user written configTemplate of a given adaptor
	 */
	public boolean updateJson(JsonData jsonData,String key) throws JsonGenerationException,
			JsonMappingException, IOException {
		Get get = new Get(Bytes.toBytes(key));
		try {
			DataRetrievalSpecification.Builder dataRetrievalSpecificationBuilder = new DataRetrievalSpecification.Builder();
			DataRetrievalSpecification dataRetrievalSpecification = dataRetrievalSpecificationBuilder
					.withTableName(adaptorTable).withGet(get).build();
			hbaseManager.retreiveData(dataRetrievalSpecification);
			Put put = new Put(Bytes.toBytes(key));
			Result result = hbaseManager.getResult();
			ObjectMapper objMapper = new ObjectMapper();
			objMapper.setSerializationInclusion(JsonSerialize.Inclusion.NON_NULL);
			String rowKeyValuefromHbase = null;
			if (result != null) {				
				rowKeyValuefromHbase=new String(result.getRow());
				}
			if (!key.equals(rowKeyValuefromHbase)) {
				DataInsertionSpecification.Builder dataInsertionSpecificationBuiler=new DataInsertionSpecification.Builder();
				put.add(Bytes.toBytes("cf"),Bytes.toBytes("jn"),Bytes.toBytes(objMapper
						.writeValueAsString((JsonData)jsonData)));
				dataInsertionSpecificationBuiler.withTableName(adaptorTable).withtPut(put).build();
			}else{
				logger.info("BIGDIME-MONITORING-SERVICE","INSERT/UPDATE","The JSON is aleady present in Hbase and no updates are made");  
	        	 return false;
			}
		} catch (HBaseClientException | IOException e) {
			logger.warn(
					"BIGDIME-MONITORING-SERVICE",
					"Unable to fetch data from HBASE",
					e.getMessage());
		} catch(Exception e){
			logger.warn(
					"BIGDIME-MONITORING-SERVICE",
					"Unable to connect to HBASE",
					e.getMessage());
		}
		return false;
	}
	
	/**
	 * get handlerTemplate of a given adaptor
	 */
	
	public Datahandler getHandler(String handlerTemplate){
		Datahandler datahandler=null;
		Get get = new Get(Bytes.toBytes(handlerTemplate));
		
		try {
			DataRetrievalSpecification.Builder dataRetrievalSpecificationBuilder = new DataRetrievalSpecification.Builder();
			DataRetrievalSpecification dataRetrievalSpecification = dataRetrievalSpecificationBuilder
					.withTableName(adaptorTable).withGet(get).build();
			hbaseManager.retreiveData(dataRetrievalSpecification);
			Result result = hbaseManager.getResult();
			ObjectMapper objMapper = new ObjectMapper();
			objMapper.setSerializationInclusion(JsonSerialize.Inclusion.NON_NULL);
			if (result != null) {				
					try {
						ObjectNode handlerJson = (ObjectNode) objMapper.readTree(new String(result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("jn"))));
						datahandler=new Datahandler();
						datahandler=objMapper.readValue(handlerJson.toString(),Datahandler.class);
					} catch (UnsupportedEncodingException e) {
						logger.warn(
								"BIGDIME-MONITORING-SERVICE",
								"The result cannot be parsed",
								e.getMessage());
					}
				}

		} catch (HBaseClientException | IOException e) {
			logger.warn(
					"BIGDIME-MONITORING-SERVICE",
					"Unable to fetch data from HBASE",
					e.getMessage());
		} catch(Exception e){
			logger.warn(
					"BIGDIME-MONITORING-SERVICE",
					"Unable to connect to HBASE",
					e.getMessage());
		}
		return datahandler;
	}
	
}
	