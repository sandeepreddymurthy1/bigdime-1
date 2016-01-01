/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.handler.line;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;

import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import io.bigdime.adaptor.metadata.MetadataAccessException;
import io.bigdime.adaptor.metadata.MetadataStore;
import io.bigdime.adaptor.metadata.model.Attribute;
import io.bigdime.adaptor.metadata.model.Entitee;
import io.bigdime.adaptor.metadata.model.Metasegment;
import io.bigdime.adaptor.metadata.utils.MetaDataJsonUtils;
import io.bigdime.core.ActionEvent;
import io.bigdime.core.ActionEvent.Status;
import io.bigdime.core.AdaptorConfigurationException;
import io.bigdime.core.HandlerException;
import io.bigdime.core.config.AdaptorConfigConstants.SourceConfigConstants;
import io.bigdime.core.constants.ActionEventHeaderConstants;
import io.bigdime.core.handler.HandlerContext;

public class TextHiveMapperTest {

	@InjectMocks
	TextHiveMapperHandler textHiveMapper;
	@Mock
	MetadataStore metadataStore;
	@Mock
	MetaDataJsonUtils metaDataJsonUtils;

	@BeforeMethod
	public void init() {
		MockitoAnnotations.initMocks(this);
	}

	@Test
	public void testBuild() throws AdaptorConfigurationException, MetadataAccessException {
		Map<String, String> srcDEscEntryMap = new HashMap<>();
		srcDEscEntryMap.put("some-invalid-path", "input1");
		Map<String, Object> propertyMap = new HashMap<>();
		propertyMap.put(SourceConfigConstants.SRC_DESC, srcDEscEntryMap.entrySet().iterator().next());
		textHiveMapper.setPropertyMap(propertyMap);
		Mockito.when(metadataStore.getAdaptorMetasegment(Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
				.thenReturn(Mockito.mock(Metasegment.class));
		textHiveMapper.build();
	}

	@Test
	public void testProcess() throws Throwable {

		FutureTask<Status> futureTask = new FutureTask<>(new Callable<Status>() {
			@Override
			public Status call() throws Exception {
				TextHiveMapperHandler textHiveMapperHandler = new TextHiveMapperHandler();
				MetadataStore metadataStore = Mockito.mock(MetadataStore.class);
				MetaDataJsonUtils metaDataJsonUtils = Mockito.mock(MetaDataJsonUtils.class);

				ReflectionTestUtils.setField(textHiveMapperHandler, "metadataStore", metadataStore);
				ReflectionTestUtils.setField(textHiveMapperHandler, "metaDataJsonUtils", metaDataJsonUtils);
				addDummyEventToContext(HandlerContext.get(), "unit-entity");
				mockBuildTextHiveMapperHandler(textHiveMapperHandler);

				Status status = textHiveMapperHandler.process();
				String processedBody = new String(HandlerContext.get().getEventList().get(0).getBody());
				Assert.assertEquals(processedBody, "us1_col11us1_col12us1_col13us1_col14\n");
				Assert.assertEquals(status, Status.READY);
				return status;
			}
		});
		ExecutorService executorService = Executors.newFixedThreadPool(1);
		executorService.execute(futureTask);
		try {
			futureTask.get();
		} catch (ExecutionException ex) {
			throw ex.getCause();
		}
	}

	@Test
	public void testProcessWithMoreThanOneActionEvent() throws Throwable {
		FutureTask<Status> futureTask = new FutureTask<>(new Callable<Status>() {
			@Override
			public Status call() throws Exception {

				TextHiveMapperHandler textHiveMapperHandler = new TextHiveMapperHandler();
				MetadataStore metadataStore = Mockito.mock(MetadataStore.class);
				MetaDataJsonUtils metaDataJsonUtils = Mockito.mock(MetaDataJsonUtils.class);

				ReflectionTestUtils.setField(textHiveMapperHandler, "metadataStore", metadataStore);
				ReflectionTestUtils.setField(textHiveMapperHandler, "metaDataJsonUtils", metaDataJsonUtils);

				addDummyEventToContext(HandlerContext.get(), "unit-entity");
				addDummyEventToContext(HandlerContext.get(), "unit-entity");

				mockBuildTextHiveMapperHandler(textHiveMapperHandler);

				Status status = textHiveMapperHandler.process();
				String processedBody = new String(HandlerContext.get().getEventList().get(0).getBody());
				Assert.assertEquals(processedBody, "us1_col11us1_col12us1_col13us1_col14\n");
				Assert.assertEquals(status, Status.CALLBACK);
				return status;
			}
		});
		ExecutorService executorService = Executors.newFixedThreadPool(1);
		executorService.execute(futureTask);
		try {
			futureTask.get();
		} catch (ExecutionException ex) {
			throw ex.getCause();
		}
	}

	private void addDummyEventToContext(HandlerContext context, String entityName) {
		ActionEvent actionEvent1 = new ActionEvent();
		actionEvent1.setBody("us1_col11|us1_col12|us1_col13|us1_col14".getBytes());
		Map<String, String> headers = new HashMap<>();
		headers.put(ActionEventHeaderConstants.ENTITY_NAME, entityName);
		actionEvent1.setHeaders(headers);

		if (context.getEventList() == null)
			context.createSingleItemEventList(actionEvent1);
		else
			context.getEventList().add(actionEvent1);
	}

	private void mockBuildTextHiveMapperHandler(TextHiveMapperHandler textHiveMapperHandler) {
		List<Map<String, Metasegment>> metasegments = new ArrayList<>();
		Map<String, Metasegment> entityNameToSchemaMetasegmentMap = new HashMap<>();
		Map<String, Metasegment> entityNameToDbMetasegmentMap = new HashMap<>();
		Metasegment schemaMetasegment = new Metasegment();
		Metasegment dbMetasegment = new Metasegment();
		metasegments.add(entityNameToSchemaMetasegmentMap);
		metasegments.add(entityNameToDbMetasegmentMap);
		entityNameToSchemaMetasegmentMap.put("unit-entity", schemaMetasegment);
		entityNameToDbMetasegmentMap.put("unit-entity", dbMetasegment);
		ReflectionTestUtils.setField(textHiveMapperHandler, "metasegments", metasegments);
		ReflectionTestUtils.setField(textHiveMapperHandler, "fieldDelimiter", "\\|");

		Set<Entitee> dbEntities = createNewEntities("unit-entity", new String[] { "col1", "col2", "col3", "col4" });
		dbMetasegment.setEntitees(dbEntities);

		Set<Entitee> schemaEntities = createNewEntities("unit-entity", new String[] { "col1", "col2", "col3", "col4" });
		schemaMetasegment.setEntitees(schemaEntities);

	}

	@Test
	public void testProcessWithModifiedFields() throws HandlerException {

		ActionEvent actionEvent1 = new ActionEvent();
		actionEvent1.setBody("us1_col11|us1_col12|us1_col13|us1_col14".getBytes());
		Map<String, String> headers = new HashMap<>();
		headers.put(ActionEventHeaderConstants.ENTITY_NAME, "unit-entity");
		actionEvent1.setHeaders(headers);

		HandlerContext.get().createSingleItemEventList(actionEvent1);

		List<Map<String, Metasegment>> metasegments = new ArrayList<>();
		Map<String, Metasegment> entityNameToSchemaMetasegmentMap = new HashMap<>();
		Map<String, Metasegment> entityNameToDbMetasegmentMap = new HashMap<>();
		Metasegment schemaMetasegment = new Metasegment();
		Metasegment dbMetasegment = new Metasegment();
		metasegments.add(entityNameToSchemaMetasegmentMap);
		metasegments.add(entityNameToDbMetasegmentMap);
		entityNameToSchemaMetasegmentMap.put("unit-entity", schemaMetasegment);
		entityNameToDbMetasegmentMap.put("unit-entity", dbMetasegment);
		ReflectionTestUtils.setField(textHiveMapper, "metasegments", metasegments);
		ReflectionTestUtils.setField(textHiveMapper, "fieldDelimiter", "\\|");

		Set<Entitee> dbEntities = createNewEntities("unit-entity", new String[] { "col1", "col2", "col3", "col4" });
		dbMetasegment.setEntitees(dbEntities);

		Set<Entitee> schemaEntities = createNewEntities("unit-entity", new String[] { "col5", "col2", "col3", "col4" });
		schemaMetasegment.setEntitees(schemaEntities);

		textHiveMapper.process();
		String processedBody = new String(HandlerContext.get().getEventList().get(0).getBody());
		Assert.assertEquals(processedBody, "nullus1_col12us1_col13us1_col14\n");
	}

	private Set<Entitee> createNewEntities(String entityName, String[] attributeNames) {
		Set<Entitee> schemaEntities = new HashSet<>();
		Entitee schemaEntity = new Entitee();
		schemaEntity.setEntityName(entityName);
		Set<Attribute> schemaAttributes = createNewAttributes(attributeNames);
		schemaEntity.setAttributes(schemaAttributes);
		schemaEntities.add(schemaEntity);
		return schemaEntities;

	}

	private Set<Attribute> createNewAttributes(String[] attributeNames) {
		Set<Attribute> attributes = new LinkedHashSet<>();
		for (String attributeName : attributeNames) {
			Attribute attribute = new Attribute();
			attribute.setAttributeName(attributeName);
			attributes.add(attribute);

		}
		//
		// attribute = new Attribute();
		// attribute.setAttributeName("col2");
		// attributes.add(attribute);
		//
		// attribute = new Attribute();
		// attribute.setAttributeName("col3");
		// attributes.add(attribute);
		//
		// attribute = new Attribute();
		// attribute.setAttributeName("col4");
		// attributes.add(attribute);
		return attributes;
	}

}
