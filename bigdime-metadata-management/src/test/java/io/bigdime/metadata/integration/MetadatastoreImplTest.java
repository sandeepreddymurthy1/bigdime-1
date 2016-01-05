/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.metadata.integration;

import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import io.bigdime.adaptor.metadata.MetadataAccessException;
import io.bigdime.adaptor.metadata.MetadataStore;
import io.bigdime.adaptor.metadata.dto.EntiteeDTO;
import io.bigdime.adaptor.metadata.dto.MetasegmentDTO;
import io.bigdime.adaptor.metadata.impl.MetadataStoreImpl;
import io.bigdime.adaptor.metadata.model.Attribute;
import io.bigdime.adaptor.metadata.model.Entitee;
import io.bigdime.adaptor.metadata.model.Metasegment;
import io.bigdime.adaptor.metadata.repositories.DataTypeRepository;
import io.bigdime.adaptor.metadata.repositories.EntitiesRepository;
import io.bigdime.adaptor.metadata.repositories.MetadataRepository;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;

/**
 * Class MetadatastoreImplIntegrationTest
 * 
 * This Class executes all the integration tests for Metadatastore
 * implementation.
 * 
 * 
 * @author Neeraj Jain, psabinikari
 * 
 * @version 1.0
 * 
 */
@ContextConfiguration(locations = "classpath:META-INF/application-context.xml")
public class MetadatastoreImplTest extends AbstractTestNGSpringContextTests {

	@Autowired
	MetadataStore metadataStore;

	@Autowired
	MetadataRepository repository;

	@Autowired
	EntitiesRepository entityRepository;

	@Autowired
	DataTypeRepository dataTypeRepository;

	@Autowired
	MetadataStoreImpl metadataStoreImpl;

	Metasegment metasegment;

	Entitee entities;

	private static final String ENVIORNMENT = "env";

	/**
	 * Class Method Load the data for test
	 * 
	 * @throws Exception
	 */

	@BeforeClass
	public void init() throws Exception {

		Set<Attribute> AttributesSet = new LinkedHashSet<Attribute>();
		Attribute attribute = new Attribute();
		attribute.setAttributeName("USER_ID");
		attribute.setAttributeType("STRING");
		attribute.setIntPart("INTPART1");
		attribute.setFractionalPart("FRACTIONALPART1");
		attribute.setNullable("NO");
		attribute.setComment("Not Null");
		attribute.setFieldType("COLUMN");
		AttributesSet.add(attribute);
		
		Attribute attribute1 = new Attribute();
		attribute1.setAttributeName("dt");

		attribute1.setAttributeType("STRING");
		attribute1.setIntPart("INTPART2");
		attribute1.setFractionalPart("FRACTIONALPART2");
		attribute1.setNullable("NO");
		attribute1.setComment("Not Null");
		attribute1.setFieldType("PARTITIONCOLUMN");
		AttributesSet.add(attribute1);

		Set<Entitee> entitySet = new HashSet<Entitee>();
		entities = new Entitee();
		entities.setEntityName("UNIT_ENTITY");
		entities.setEntityLocation("/data/UnitAdaptor/raw/UNIT_ENTITY");
		entities.setDescription("HDFS LOCATION");
		entities.setVersion(1.1);

		entities.setAttributes(AttributesSet);

		entitySet.add(entities);

		metasegment = new Metasegment();
		metasegment.setAdaptorName("UnitAdaptor");
		metasegment.setSchemaType("HIVE");
		metasegment.setEntitees(entitySet);
		metasegment.setDatabaseName("STBDBP");
		metasegment.setDatabaseLocation("");
		metasegment.setRepositoryType("TARGET");
		metasegment.setIsDataSource("Y");

		
		metasegment.setCreatedBy("TEST_USER");
		metasegment.setUpdatedBy("TEST_USER");
	}

	/**
	 * Class Method This method sets up all the environment variables before
	 * test execution
	 */

	@BeforeTest
	public void setup() {
		// logger.info("Setting the environment");
		System.setProperty(ENVIORNMENT, "localhost");
	}

	@Test(priority = 21,expectedExceptions = IllegalArgumentException.class)
	public void testNullCheckForRemove() throws MetadataAccessException {
		//Metasegment nullMetasegment = null;
		metadataStore.remove(null);

	}

	@Test(priority = 22)
	public void testNullCheckForCache() throws MetadataAccessException {
		metadataStoreImpl.loadCache();
	}

	
	@Test(priority = 23, expectedExceptions = IllegalArgumentException.class)
	public void testNullPutSchema() throws MetadataAccessException {
		metadataStore.put(null);
	}
	/**
	 * Class Method Test: Insert/Updates schema details.
	 * 
	 * @throws MetadataAccessException
	 */
	@Test(priority = 24)
	public void testPutSchema() throws MetadataAccessException {
		
		metadataStore.put(metasegment);

		MetasegmentDTO metdata = repository.findByAdaptorNameAndSchemaType(
				metasegment.getAdaptorName(), metasegment.getSchemaType());

		Assert.assertNotNull(metdata.getId());

	}
	
	@Test(priority = 25)
	public void testPutSchemaAdpatorDetailsOnly() throws MetadataAccessException {
		
		Metasegment mets = new Metasegment();
		mets.setAdaptorName("testAdaptorName");
		mets.setSchemaType("testSchemaType");
		metadataStore.put(mets);

		MetasegmentDTO metdata = repository.findByAdaptorNameAndSchemaType(
				mets.getAdaptorName(), mets.getSchemaType());

		Assert.assertNotNull(metdata.getId());

	}
	
	@Test(priority = 26)
	public void testUpdateAndRemoveAdaptorDetails() throws MetadataAccessException {
		
		Metasegment mets = new Metasegment();
		mets.setAdaptorName("testAdaptorName");
		mets.setSchemaType("testSchemaType");
		metadataStore.put(mets);

		MetasegmentDTO metdata = repository.findByAdaptorNameAndSchemaType(
				mets.getAdaptorName(), mets.getSchemaType());

		Assert.assertNotNull(metdata.getId());
		
		metadataStore.remove(mets);
		MetasegmentDTO metsdata = repository.findByAdaptorNameAndSchemaType(
				mets.getAdaptorName(), mets.getSchemaType());
		Assert.assertNull(metsdata);

	}

	/**
	 * Test: Update an existing entity to test hard delete functionality works
	 * 
	 * 
	 * @throws MetadataAccessException
	 */
	@Test(priority = 27)
	public void testUpdateSchema() throws MetadataAccessException {

		Set<Attribute> AttributesSet = new LinkedHashSet<Attribute>();
		Attribute attribute = new Attribute();
		attribute.setAttributeName("USER_ID_1");
		attribute.setAttributeType("STRING");
		attribute.setIntPart("INTPART3");
		attribute.setFractionalPart("FRACTIONALPART3");
		attribute.setNullable("NO");
		attribute.setComment("Not Null");
		attribute.setFieldType("COLUMN");
		AttributesSet.add(attribute);
		

		Set<Entitee> entitySet = new HashSet<Entitee>();
		entities = new Entitee();
		entities.setEntityName("UNIT_ENTITY");
		entities.setEntityLocation("/data/UnitAdaptor/raw/UNIT_ENTITY1");
		entities.setDescription("HDFS LOCATION");
		entities.setVersion(2.1);

		entities.setAttributes(AttributesSet);

		entitySet.add(entities);
		Metasegment metSegment = new Metasegment();
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
		metSegment.setEntitees(entitySet);
        metadataStore.put(metSegment);

		MetasegmentDTO metdata = repository.findByAdaptorNameAndSchemaType(
				metSegment.getAdaptorName(), metSegment.getSchemaType());
		for (EntiteeDTO entity : metdata.getEntitees()) {

			Assert.assertEquals(entity.getVersion(), 1.1,
					"Version should not be updated");

		}

		for (Entitee entity : metSegment.getEntitees())
			entity.setVersion(1.1);
		metadataStore.put(metSegment);

		metdata = repository.findByAdaptorNameAndSchemaType(
				metSegment.getAdaptorName(), metSegment.getSchemaType());

		Assert.assertNotNull(metdata.getId());

	}

	/**
	 * Test: Create a new entity to an existing metasegment.
	 * 
	 * @throws MetadataAccessException
	 */
	@Test(priority = 28)
	public void testCreateNewEntity() throws MetadataAccessException {

		Set<Attribute> AttributesSet = new LinkedHashSet<Attribute>();
		Attribute attribute = new Attribute();
		attribute.setAttributeName("ATTRIBUTENAME5");
		attribute.setAttributeType("int");
		attribute.setIntPart("INTPART5");
		attribute.setFractionalPart("FRACTIONALPART5");
		attribute.setNullable("NO");
		attribute.setComment("COMMENT");
		attribute.setFieldType("COLUMN");
		AttributesSet.add(attribute);
		
		Attribute attribute2 = new Attribute();
		attribute2.setAttributeName("ATTRIBUTENAME5EXtra");
		attribute2.setAttributeType("int");
		attribute2.setIntPart("INTPART5");
		attribute2.setFractionalPart("FRACTIONALPART5");
		attribute2.setNullable("NO");
		attribute2.setComment("COMMENT");
		attribute2.setFieldType("COLUMN");
		AttributesSet.add(attribute2);
		
		Attribute attribute1 = new Attribute();
		attribute1.setAttributeName("ATTRIBUTENAME6");

		attribute1.setAttributeType("long");
		attribute1.setIntPart("INTPART6");
		attribute1.setFractionalPart("FRACTIONALPART6");
		attribute1.setNullable("NO");
		attribute1.setComment("COMMENT2");
		attribute1.setFieldType("PARTITIONCOLUMN");
		AttributesSet.add(attribute1);
		
		

		Set<Entitee> entitySet = new HashSet<Entitee>();
		entities = new Entitee();
		entities.setEntityName("ENTITYNAME9");
		entities.setEntityLocation("ENTITYLOCATION9");
		entities.setDescription("DESCRIPTION");
		entities.setVersion(1.1);

		entities.setAttributes(AttributesSet);

		entitySet.add(entities);
		Metasegment metSegment = new Metasegment();
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
		metSegment.setEntitees(entitySet);

		metadataStore.put((Metasegment) metSegment);

		MetasegmentDTO metdata = repository.findByAdaptorNameAndSchemaType(
				metSegment.getAdaptorName(), metSegment.getSchemaType());

		Assert.assertNotNull(metdata.getId());

	}

	/**
	 * Class Method Test: Get the schema details associated to
	 * Metasegment(adapter).
	 * 
	 * @throws MetadataAccessException
	 */
	@Test(priority = 29)
	public void testGetMetasegment() throws MetadataAccessException {

		Metasegment metSegs = metadataStore.getAdaptorMetasegment(
				"TestApplicationName", "TestSchemaType", "TestEntityName");
		Assert.assertNull(metSegs);
		Metasegment metadata = metadataStore.getAdaptorMetasegment(
				metasegment.getAdaptorName(), metasegment.getSchemaType(),
				"ENTITYNAME9");

		Assert.assertNotNull(metadata.getAdaptorName());
		
		Entitee entits = metadata.getEntity("ENTITYNAME9");
		
		Assert.assertNotNull(entits);

	}

	@Test(priority = 30)
	public void testGetAdaptorEntitee() throws MetadataAccessException {

		Entitee entitee = metadataStore.getAdaptorEntity("TestApplicationName",
				"TestSchemaType", "TestEntityName");
		Assert.assertNull(entitee);

		Entitee entity = metadataStore.getAdaptorEntity(
				metasegment.getAdaptorName(), metasegment.getSchemaType(),
				entities.getEntityName());

		Assert.assertNotNull(entity.getEntityLocation());
	}

	/**
	 * Test: Get all the Entities(Tables) associated to a given Metasegment
	 * 
	 * @throws MetadataAccessException
	 */
	@Test(priority = 31)
	public void testGetAdapterEntities() throws MetadataAccessException {
		Set<Entitee> entities = metadataStore.getAdaptorEntities(
				metasegment.getAdaptorName(), metasegment.getSchemaType());

		Assert.assertEquals(entities.size(), 2,
				"Number of entities should be same for a given Adapter "
						+ metasegment.getAdaptorName());
	}

	/**
	 * Test: Get all the tables list
	 * 
	 * @throws MetadataAccessException
	 */
	@Test(priority = 32)
	public void testgetAdaptorEntityList() throws MetadataAccessException {
		Set<String> entitiesSet = metadataStore.getAdaptorEntityList(
				metasegment.getAdaptorName(), metasegment.getSchemaType());

		Assert.assertTrue(entitiesSet.contains(entities.getEntityName()
				.toLowerCase()));
		Assert.assertEquals(entitiesSet.size(), 2,
				"Number of entity list should be same for a given Adapter"
						+ metasegment.getAdaptorName());
	}

	/**
	 * Test: Get all the distinct data sources that are available in repository
	 * 
	 * @throws MetadataAccessException
	 */
	@Test(priority = 33)
	public void testGetDistictDataSource() throws MetadataAccessException {
		Set<String> dataSources = metadataStore.getDataSources();
		Assert.assertTrue(dataSources.contains(metasegment.getAdaptorName()));
	}

	@Test(priority = 34)
	public void testRemoveMetasegment() throws MetadataAccessException {

		metadataStore.remove(metasegment);
		MetasegmentDTO metdata = repository.findByAdaptorNameAndSchemaType(
				metasegment.getAdaptorName(), metasegment.getSchemaType());

		Assert.assertNull(metdata, "Expected object should be null");

	}

	/**
	 * Test: Get all the distinct data sources when none are available in
	 * repository
	 * 
	 * @throws MetadataAccessException
	 */
	@Test(priority = 35)
	public void testCheckNullablitiyForGetDistictDataSource()
			throws MetadataAccessException {
		Set<String> dataSources = metadataStore.getDataSources();
		Assert.assertEquals(dataSources.size(), 0,
				"There should not be any data Source after removing all segments");
	}

	/**
	   * 
	   */
	@Test(priority = 36)
	public void testRemoveAllDataTypes() {
		dataTypeRepository.deleteAll();
		Assert.assertEquals(dataTypeRepository.findAll().size(), 0,
				"None of the data types should exist");

	}

	/**
	 * Test: Get all the Metasegments available
	 * 
	 * @throws MetadataAccessException
	 */
	@Test(priority = 37)
	public void testGetAdaptorMetasegments() throws MetadataAccessException {

		List<Metasegment> metasegs = metadataStore.getAdaptorMetasegments(
				"testApplicationName", "testSchemaName");
		Assert.assertEquals(metasegs.size(), 0,
				"No metasegments should not exists with applicationName: testApplicationName");
		
	}
	
	@Test(priority = 38)
	public void testCreateDatasourceIfNotExist() throws MetadataAccessException{
		List<Metasegment> metasegmentsList = metadataStore.createDatasourceIfNotExist("testApplicationName1", "testSchemaName1");
		Assert.assertEquals(metasegmentsList.size(), 1,
				"No metasegments should not exists with applicationName: testApplicationName1");
		metadataStore.remove(metasegmentsList.get(0));
		MetasegmentDTO metdata = repository.findByAdaptorNameAndSchemaType(
				"testApplicationName1", "testSchemaName1");

		Assert.assertNull(metdata, "Expected object should be null");
		
	}

}
