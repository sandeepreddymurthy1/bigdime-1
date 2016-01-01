/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.metadata.integration;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
import org.testng.Assert;

import io.bigdime.adaptor.metadata.dto.AttributeDTO;
import io.bigdime.adaptor.metadata.dto.DataTypeDTO;
import io.bigdime.adaptor.metadata.dto.EntiteeDTO;
import io.bigdime.adaptor.metadata.dto.MetasegmentDTO;
import io.bigdime.adaptor.metadata.impl.MetadataRepositoryService;
import io.bigdime.adaptor.metadata.repositories.DataTypeRepository;
import io.bigdime.adaptor.metadata.repositories.MetadataRepository;

/**
 * Class MetadadataRepositoryServiceIntegrationTest
 * 
 * @author Neeraj Jain, psabinikari
 * 
 */
@ContextConfiguration(locations = "classpath:META-INF/application-context.xml")
public class MetadataRepositoryServiceTest extends
		AbstractTestNGSpringContextTests {

	MetasegmentDTO metasegment, metSegment;
	EntiteeDTO entities;

	@Autowired
	MetadataRepositoryService metadataRepositoryService;

	@Autowired
	MetadataRepository metadataRepository;

	@Autowired
	DataTypeRepository dataTypeRepository;

	private static final String ENVIORNMENT = "env";

	/**
	 * Load the test data
	 * 
	 * @throws Exception
	 */

	@BeforeClass
	public void init() throws Exception {

		Set<AttributeDTO> attributesSet = new LinkedHashSet<AttributeDTO>();
		AttributeDTO attribute = new AttributeDTO("USER_ID", "STRING", "INTPART1",
				"FRACTIONALPART1", "Not Null", "NO", "COLUMN", "", "");

		attributesSet.add(attribute);
		AttributeDTO attribute1 = new AttributeDTO();
		attribute1.setAttributeName("dt");
		attribute1.setAttributeType("STRING");
		attribute1.setIntPart("INTPART2");
		attribute1.setFractionalPart("FRACTIONALPART2");
		attribute1.setNullable("NO");
		attribute1.setComment("Not Null");
		attribute1.setFieldType("PARTITIONCOLUMN");
		attribute1.setMappedAttributeName("");
		attribute1.setDefaultValue("");
		attributesSet.add(attribute1);
		
        
		
        Set<EntiteeDTO> entitySet = new HashSet<EntiteeDTO>();
		entities = new EntiteeDTO("USWEB", "/data/unit/raw/USWEB", 1.1,
				"unit Source Details", attributesSet);
		entitySet.add(entities);

		metasegment = new MetasegmentDTO("Users", "FILE", "", "", "SOURCE",
				"Y", "unit Adapter details", entitySet, 
				"TEST_USER",  "TEST_USER");

		Set<AttributeDTO> attributesSet1 = new HashSet<AttributeDTO>();
		AttributeDTO attribute2 = new AttributeDTO();
		attribute2.setAttributeName("USER_ID_1");
		attribute2.setAttributeType("STRING");
		attribute2.setIntPart("INTPART5");
		attribute2.setFractionalPart("FRACTIONALPART5");
		attribute2.setNullable("NO");
		attribute2.setComment("Not Null");
		attribute2.setFieldType("COLUMN");
		attributesSet1.add(attribute2);
		AttributeDTO attribute3 = new AttributeDTO();
		attribute3.setAttributeName("dt1");

		attribute3.setAttributeType("STRING");
		attribute3.setIntPart("INTPART6");
		attribute3.setFractionalPart("FRACTIONALPART6");
		attribute3.setNullable("NO");
		attribute3.setComment("Not Null");
		attribute3.setFieldType("PARTITIONCOLUMN");
		attributesSet1.add(attribute3);

		Set<EntiteeDTO> entitySet1 = new HashSet<EntiteeDTO>();
		entities = new EntiteeDTO();
		entities.setEntityName("USWEB1");
		entities.setEntityLocation("/data/unit/raw/USWEB1");
		entities.setDescription("unit Source Details");
		entities.setVersion(1.1);

		entities.setAttributes(attributesSet1);
		
		entitySet1.add(entities);
		metSegment = new MetasegmentDTO();
		metSegment.setAdaptorName(metasegment.getAdaptorName());
		metSegment.setSchemaType(metasegment.getSchemaType());
		metSegment.setDatabaseName(metasegment.getDatabaseName());
		metSegment.setDatabaseLocation(metasegment.getDatabaseLocation());
		metSegment.setDescription(metasegment.getDescription());
		metSegment.setRepositoryType(metasegment.getRepositoryType());
		metSegment.setCreatedAt(metasegment.getCreatedAt());
		metSegment.setCreatedBy(metasegment.getCreatedBy());
		metSegment.setUpdatedAt(metasegment.getUpdatedAt());
		metSegment.setUpdatedBy(metasegment.getUpdatedBy());
		metSegment.setEntitees(entitySet1);
	}

	/**
	 * Setup the environment variables
	 */
	@BeforeTest
	public void setup() {
		logger.info("Setting the environment");
		System.setProperty(ENVIORNMENT, "localhost");
	}

	@Test(priority = 1)
	public void testNullTest() {

		
		List<MetasegmentDTO> mets = metadataRepositoryService.getAllSegments();
		Assert.assertEquals(mets.size(), 0,
				"None of the segmets should exist while running this test");

		Set<String> distinctDataSources = metadataRepositoryService
				.getDistinctDataSources();
		Assert.assertEquals(distinctDataSources.size(), 0,
				"No data should exists while running this test");

		Set<EntiteeDTO> entities = metadataRepositoryService.getAllEntites(
				"testApplicationName", "testSchemaName");
		Assert.assertNull(entities);

		metadataRepositoryService.remove(metSegment, false);
	}

	@Test(priority=2)
	public void testAttributeandEntityMethods() {

		// DataType Test
		DataTypeDTO dataType = new DataTypeDTO("int", "Int data Type");
		Assert.assertEquals(dataType.getDataType(), "int",
				"data type should be int");
		Assert.assertEquals(dataType.getDescription(), "Int data Type",
				"Description should be same as that is set");
		dataType = new DataTypeDTO();
		dataType.setDataType("String");
		dataType.setDescription("String Description");
		Assert.assertEquals(dataType.getDataType(), "String",
				"data type should be string");
		Assert.assertEquals(dataType.getDescription(), "String Description",
				"Description should be same as that is set");

		// Entitee Test
		Set<EntiteeDTO> entiteeSet = metasegment.getEntitees();
		for (EntiteeDTO entitee : entiteeSet) {

			Assert.assertEquals(entitee.getEntityName(), "USWEB",
					"entity name should be same");

			Assert.assertEquals(entitee.getEntityLocation(),
					"/data/unit/raw/USWEB",
					"Entity location should be same");

			Assert.assertEquals(entitee.getVersion(), 1.1,
					"Version should be equal");
			Assert.assertEquals(entitee.getDescription(),
					"unit Source Details", "Description should be equal");
			Set<AttributeDTO> attributeSet = entitee.getAttributes();
			// Attribute Test
			for (AttributeDTO attribute : attributeSet) {
				if (attribute.getAttributeName().equalsIgnoreCase("USER_ID")) {
					attribute.setDataType(dataType);
					Assert.assertEquals(attribute.getDataType().getDataType(),
							"String",
							"dataType should be same as that assigned");
					Assert.assertEquals(attribute.getAttributeName(),
							"USER_ID", "Attribute name should be same");
					Assert.assertEquals(attribute.getAttributeType(), "STRING",
							"Attribute type should be same");
					Assert.assertEquals(attribute.getIntPart(), "INTPART1",
							"Attribute integer part should be same");
					Assert.assertEquals(attribute.getFractionalPart(),
							"FRACTIONALPART1",
							"Attribute fractional part should be same");
					Assert.assertEquals(attribute.getNullable(), "NO",
							"Attribute nullable column should be same");
					Assert.assertEquals(attribute.getComment(), "Not Null",
							"Attribtue comment value should be same");
					Assert.assertEquals(attribute.getFieldType(), "COLUMN",
							"Attribtue field type should be same");
					Assert.assertEquals(attribute.getMappedAttributeName(), "",
							"Mapped attribute name should be same as that was assigned");
					Assert.assertEquals(attribute.getDefaultValue(), "",
							"Default value should be same as that was assigned.");
					Assert.assertNull(attribute.getId());
				}
			}
		}

	}

	/**
	 * Class Method Test: create a new segment
	 */
	@Test(priority = 3)
	public void testCreateMetasegment() {

		metadataRepositoryService.createOrUpdateMetasegment(metasegment, false);
		MetasegmentDTO metasegments = metadataRepository
				.findByAdaptorNameAndSchemaType(metasegment.getAdaptorName(),
						metasegment.getSchemaType());
		Assert.assertNotNull(metasegments);

	}
	
	
	@Test(priority=4)
	public void  TestCheckAttributesAssociation() {
		
		Set<EntiteeDTO> entityDTOSet = metasegment.getEntitees();
		for(EntiteeDTO entitee: entityDTOSet){
			Set<AttributeDTO> attributeDTOSet = entitee.getAttributes();
			AttributeDTO attribute2 = new AttributeDTO();
			attribute2.setAttributeName("TEST-1");
			attribute2.setAttributeType("STRING");
			attribute2.setIntPart("TestPart-1");
			attribute2.setFractionalPart("TestFP-1");
			attribute2.setNullable("NO");
			attribute2.setComment("Not Null");
			attribute2.setFieldType("COLUMN");
			
			attributeDTOSet.add(attribute2);
		}
		metadataRepositoryService.checkAttributesAssociation(metasegment);
	}
	
	@Test(priority=5)
	public void TestcheckRandomnAttributesAssociation() {
		Set<EntiteeDTO> entityDTOSet = metasegment.getEntitees();
		for(EntiteeDTO entitee: entityDTOSet){
			Set<AttributeDTO> attributeDTOSet = entitee.getAttributes();
			attributeDTOSet.clear();
			AttributeDTO attribute2 = new AttributeDTO();
			attribute2.setAttributeName("TEST-1");
			attribute2.setAttributeType("STRING");
			attribute2.setIntPart("TestPart-1");
			attribute2.setFractionalPart("TestFP-1");
			attribute2.setNullable("NO");
			attribute2.setComment("Not Null");
			attribute2.setFieldType("COLUMN");
			
			attributeDTOSet.add(attribute2);
			
			AttributeDTO attribute3 = new AttributeDTO();
			attribute3.setAttributeName("dt");

			attribute3.setAttributeType("STRING");
			attribute3.setIntPart("INTPART6");
			attribute3.setFractionalPart("FRACTIONALPART6");
			attribute3.setNullable("NO");
			attribute3.setComment("Not Null");
			attribute3.setFieldType("PARTITIONCOLUMN");
			attributeDTOSet.add(attribute3);
		}
		metadataRepositoryService.checkAttributesAssociation(metasegment);
	}
	
	@Test(priority=6)
	public void testDoUpdateMetasegment() {
		
		MetasegmentDTO metasegments = metadataRepository
				.findByAdaptorNameAndSchemaType(metasegment.getAdaptorName(),
						metasegment.getSchemaType());
		for(EntiteeDTO repoEntityDTO: metasegments.getEntitees()) {
			repoEntityDTO.setEntityLocation("/data/unit/raw/USWEB2");
			Set<AttributeDTO> attributeDTOSet = repoEntityDTO.getAttributes();
			AttributeDTO attribute3 = new AttributeDTO();
			attribute3.setAttributeName("date");

			attribute3.setAttributeType("STRING");
			attribute3.setIntPart("INTPART6");
			attribute3.setFractionalPart("FRACTIONALPART6");
			attribute3.setNullable("NO");
			attribute3.setComment("Not Null");
			attribute3.setFieldType("PARTITIONCOLUMN");
			attributeDTOSet.add(attribute3);
			
			repoEntityDTO.getAttributes().add(attribute3);
			
		}
		metadataRepositoryService.createOrUpdateMetasegment(metasegments, true);
		
		Assert.assertNotNull(metasegments);
	}

	/**
	 * Test: remove entity for an adapter
	 */
	@Test(priority = 7)
	public void testRemoveSchema() {

		MetasegmentDTO metaSegs = metadataRepository
				.findByAdaptorNameAndSchemaType(metasegment.getAdaptorName(),
						metasegment.getSchemaType());
		Assert.assertNotNull(metaSegs);

		metadataRepositoryService.remove(metaSegs, false);
		for (EntiteeDTO entity : metaSegs.getEntitees()) {
			MetasegmentDTO metas = metadataRepositoryService.getSchema(
					metasegment.getAdaptorName(), metasegment.getSchemaType(),
					entity.getEntityName());
			Assert.assertTrue(metas.getEntitees().isEmpty());
		}

		Set<EntiteeDTO> entities = metadataRepositoryService.getAllEntites(
				metaSegs.getAdaptorName(), metaSegs.getSchemaType());
		Assert.assertNull(entities);
	}

	/**
	 * Test: Create new entity for an adapter
	 */
	@Test(priority = 8)
	public void testCreateNewEntity() {

		System.out.println("Metasegment is "+metSegment);
		metadataRepositoryService.createOrUpdateMetasegment(metSegment, false);

		MetasegmentDTO metasegments = metadataRepository
				.findByAdaptorNameAndSchemaType(metSegment.getAdaptorName(),
						metSegment.getSchemaType());
		Assert.assertNotNull(metasegments);
	}

	/**
	 * Test: Check existence of metasegment
	 */
	@Test(priority = 9)
	public void testSchemaExists() {
		Assert.assertFalse(metadataRepositoryService.schemaExists(metasegment));
		Assert.assertTrue(metadataRepositoryService.schemaExists(metSegment));

	}

	/**
	 * Test: Test distinct data sources
	 */
	@Test(priority = 10)
	public void testGetDistinctdataSources() {
		Assert.assertEquals(metadataRepositoryService.getDistinctDataSources()
				.size(), 1, "Datasources Set size are not same");
	}

	/**
	 * Test: Get Schema
	 */
	@Test(priority = 11)
	public void testGetSchema() {

		MetasegmentDTO metaSchema = metadataRepositoryService.getSchema(
				metasegment.getAdaptorName(), metasegment.getSchemaType(),
				"ENTITYNAME7");

		Assert.assertTrue(metaSchema.getEntitees().isEmpty(),
				"metasegments object details should be empty");
		metaSchema = metadataRepositoryService.getSchema(
				metSegment.getAdaptorName(), metSegment.getSchemaType(),
				entities.getEntityName());

		Assert.assertNotNull(metaSchema,
				"metasegments object details should not be null");
		
		EntiteeDTO entity = metaSchema.getEntity(entities.getEntityName());
		Assert.assertNotNull(entity);
	}

	/**
	 * Test: Get all metasegments available in repository
	 */
	@Test(priority = 12)
	public void testGetAllSegments() {
		List<MetasegmentDTO> mets = metadataRepositoryService.getAllSegments();
		Assert.assertEquals(mets.size(), 1,
				"Number of metasegments are not equal");

	}

	@Test(priority=13)
	public void testGetAllSegmentsForAnAdaptor() {
		List<MetasegmentDTO> metsList = metadataRepositoryService.getAllSegments(metasegment.getAdaptorName());
		Assert.assertEquals(metsList.size(), 1,
				"Number of metasegments are not equal");	
	}
	/**
	 * Test: Get all entities associated to metasegment
	 */
	@Test(priority = 14)
	public void testGetAllEntites() {
		Assert.assertEquals(
				metadataRepositoryService.getAllEntites(
						metasegment.getAdaptorName(),
						metasegment.getSchemaType()).size(), 1,
				"Entities count should be equal");

	}

	/**
	 * Test: Remove metasegment
	 */
	@Test(priority = 15)
	public void testRemoveMetasegment() {

		metadataRepositoryService.remove(metasegment, true);
		MetasegmentDTO meta = metadataRepository.findByAdaptorNameAndSchemaType(
				metasegment.getAdaptorName(), metasegment.getSchemaType());
		Assert.assertNull(meta, "Metasegment object should be null");

	}

}