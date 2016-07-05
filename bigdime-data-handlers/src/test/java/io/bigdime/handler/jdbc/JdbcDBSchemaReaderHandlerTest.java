/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.handler.jdbc;

import static org.mockito.MockitoAnnotations.initMocks;
import static org.mockito.Mockito.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.json.JSONObject;
import org.mockito.Mock;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.testng.PowerMockTestCase;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import io.bigdime.core.ActionEvent.Status;
import io.bigdime.core.HandlerException;
import io.bigdime.core.InvalidValueConfigurationException;
import io.bigdime.core.config.AdaptorConfigConstants;

@PrepareForTest({JdbcTemplate.class, JdbcDBSchemaReaderHandler.class})
public class JdbcDBSchemaReaderHandlerTest extends PowerMockTestCase {

	JdbcDBSchemaReaderHandler jdbcDBHandler;
	
	@Mock
	JdbcTemplate jdbcTemplate;
	@Mock
	JdbcInputDescriptor jdbcInputDescriptor;
	@Mock
	DataSource dataSource;
	
	@BeforeClass
	public void init() {
		initMocks(this);
		jdbcDBHandler = new JdbcDBSchemaReaderHandler();
	}
	
	@Test
	public void testBuild() throws Exception {
		jdbcDBHandler.setDataSource(dataSource);
		ReflectionTestUtils.setField(jdbcDBHandler, "jdbcTemplate", jdbcTemplate);
		PowerMockito.whenNew(JdbcTemplate.class).withArguments((DataSource)any()).thenReturn(jdbcTemplate);
		Map<String, Object> propertyMap = new HashMap<>();
		String inputString = "{\"input1\" : {\"inputType\":\"database\",\"inputValue\":\"testDB\", \"incrementedBy\":\"incrementalCol\", \"include\":\"\"}}";
		JSONObject jsonObject = new JSONObject(inputString);
		Map<String, String> srcDEscEntryMap = new HashMap<>();
		srcDEscEntryMap.put(jsonObject.getString("input1"), "input1");
		propertyMap.put(AdaptorConfigConstants.SourceConfigConstants.SRC_DESC,
				srcDEscEntryMap.entrySet().iterator().next());
		jdbcDBHandler.setPropertyMap(propertyMap);
		ReflectionTestUtils.setField(jdbcDBHandler, "jdbcInputDescriptor", jdbcInputDescriptor);
		when(jdbcInputDescriptor.getInputType()).thenReturn("database");
		when(jdbcInputDescriptor.getInputValue()).thenReturn("testDB");
		jdbcDBHandler.build();
		Assert.assertEquals(jdbcInputDescriptor.getInputType(), "database");
		Assert.assertEquals(jdbcInputDescriptor.getInputValue(), "testDB");
	}
	
	@Test(expectedExceptions = InvalidValueConfigurationException.class)
	public void testBuildWithoutSrcDesc() throws Exception {
		jdbcDBHandler.setDataSource(dataSource);
		ReflectionTestUtils.setField(jdbcDBHandler, "jdbcTemplate", jdbcTemplate);
		PowerMockito.whenNew(JdbcTemplate.class).withArguments((DataSource)any()).thenReturn(jdbcTemplate);
		Map<String, Object> propertyMap = new HashMap<>();
		String inputString = "{\"input1\" : {\"inputType\":\"database\",\"inputValue\":\"testDB\", \"incrementedBy\":\"incrementalCol\", \"include\":\"\"}}";
		JSONObject jsonObject = new JSONObject(inputString);
		Map<String, String> srcDEscEntryMap = new HashMap<>();
		srcDEscEntryMap.put(jsonObject.getString("input1"), "input1");
		jdbcDBHandler.setPropertyMap(propertyMap);
		jdbcDBHandler.build();
	}
	
	@Test(expectedExceptions = HandlerException.class)
	public void testProcessFirstRunWithNullDbSqlQuery() throws HandlerException, JdbcHandlerException {
		jdbcDBHandler.setDataSource(dataSource);
		ReflectionTestUtils.setField(jdbcDBHandler, "jdbcInputDescriptor", jdbcInputDescriptor);
		when(jdbcInputDescriptor.getDatabaseName()).thenReturn("testDB");
		when(jdbcInputDescriptor.formatQuery(anyString(), anyString(), anyString())).thenReturn(null);
		jdbcDBHandler.process();
	}
	
	@Test(expectedExceptions = HandlerException.class)
	public void testProcessWithNullDBsqlQuery() throws HandlerException, JdbcHandlerException {
		jdbcDBHandler.setDataSource(dataSource);
		ReflectionTestUtils.setField(jdbcDBHandler, "jdbcInputDescriptor", jdbcInputDescriptor);
		when(jdbcInputDescriptor.getDatabaseName()).thenReturn("testDB");
		when(jdbcInputDescriptor.formatQuery(anyString(), anyString(), anyString())).thenReturn(null);
		jdbcDBHandler.incrementInvocationCount();
		jdbcDBHandler.process();
	}
	
	@Test
	public void testProcessWithoutIncludeFilter() throws Exception {
		jdbcDBHandler.setDataSource(dataSource);
		ReflectionTestUtils.setField(jdbcDBHandler, "jdbcInputDescriptor", jdbcInputDescriptor);
		ReflectionTestUtils.setField(jdbcDBHandler, "jdbcTemplate", jdbcTemplate);
		when(jdbcInputDescriptor.formatQuery(anyString(), anyString(), anyString())).thenReturn("dbSql");
		when(jdbcInputDescriptor.getInputValue()).thenReturn("key1");
		when(jdbcInputDescriptor.getIncludeFilter()).thenReturn("");
		PowerMockito.whenNew(JdbcTemplate.class).withArguments((DataSource)any()).thenReturn(jdbcTemplate);
		List<String> tableList = new ArrayList<String>();
		tableList.add("table1");
		when(jdbcTemplate.queryForList(anyString(),eq(String.class))).thenReturn(tableList);	
		Assert.assertEquals(jdbcDBHandler.process(), Status.READY);
		jdbcDBHandler.incrementInvocationCount();
		when(jdbcInputDescriptor.getInputValue()).thenReturn("key2");
		when(jdbcInputDescriptor.getIncludeFilter()).thenReturn("");
		List<String> tableList2 = new ArrayList<String>();
		tableList2.add("table2");
		when(jdbcTemplate.queryForList(anyString(),eq(String.class))).thenReturn(tableList2);	
		Assert.assertEquals(jdbcDBHandler.process(), Status.READY);
	}
	
	@Test
	public void testProcessWithIncludeFilter() throws Exception {
		jdbcDBHandler.setDataSource(dataSource);
		ReflectionTestUtils.setField(jdbcDBHandler, "jdbcInputDescriptor", jdbcInputDescriptor);
		ReflectionTestUtils.setField(jdbcDBHandler, "jdbcTemplate", jdbcTemplate);
		when(jdbcInputDescriptor.formatQuery(anyString(), anyString(), anyString())).thenReturn("dbSql");
		when(jdbcInputDescriptor.getInputValue()).thenReturn("key3");
		when(jdbcInputDescriptor.getIncludeFilter()).thenReturn("filter");
		PowerMockito.whenNew(JdbcTemplate.class).withArguments((DataSource)any()).thenReturn(jdbcTemplate);
		List<String> tableList = new ArrayList<String>();
		tableList.add("filterTable");
		when(jdbcTemplate.queryForList(anyString(),eq(String.class))).thenReturn(tableList);	
		Assert.assertEquals(jdbcDBHandler.process(), Status.READY);
		jdbcDBHandler.incrementInvocationCount();
		when(jdbcInputDescriptor.getInputValue()).thenReturn("key4");
		when(jdbcInputDescriptor.getIncludeFilter()).thenReturn("filter");
		List<String> tableList2 = new ArrayList<String>();
		tableList2.add("table2");
		when(jdbcTemplate.queryForList(anyString(),eq(String.class))).thenReturn(tableList2);	
		Assert.assertEquals(jdbcDBHandler.process(), Status.BACKOFF); //no table matches the regex filter, return BACKOFF
	}
	
	@Test
	public void NoTableNeedProcess() throws Exception {
		jdbcDBHandler.setDataSource(dataSource);
		ReflectionTestUtils.setField(jdbcDBHandler, "jdbcInputDescriptor", jdbcInputDescriptor);
		ReflectionTestUtils.setField(jdbcDBHandler, "jdbcTemplate", jdbcTemplate);
		when(jdbcInputDescriptor.formatQuery(anyString(), anyString(), anyString())).thenReturn("dbSql");
		when(jdbcInputDescriptor.getInputValue()).thenReturn("key5");
		when(jdbcInputDescriptor.getIncludeFilter()).thenReturn("filter");
		PowerMockito.whenNew(JdbcTemplate.class).withArguments((DataSource)any()).thenReturn(jdbcTemplate);
		List<String> tableList = new ArrayList<String>();
		tableList.add("");
		when(jdbcTemplate.queryForList(anyString(),eq(String.class))).thenReturn(tableList);	
		Assert.assertEquals(jdbcDBHandler.process(), Status.BACKOFF); //no table matches the regex filter, return BACKOFF
	}
}
