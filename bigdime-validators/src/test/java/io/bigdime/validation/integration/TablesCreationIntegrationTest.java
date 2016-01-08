package io.bigdime.validation.integration;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import io.bigdime.adaptor.metadata.MetadataAccessException;
import io.bigdime.adaptor.metadata.MetadataStore;
import io.bigdime.adaptor.metadata.dto.MetasegmentDTO;
import io.bigdime.adaptor.metadata.model.Entitee;
import io.bigdime.adaptor.metadata.model.Metasegment;
import io.bigdime.adaptor.metadata.model.Attribute;
import io.bigdime.adaptor.metadata.repositories.DataTypeRepository;
import io.bigdime.adaptor.metadata.repositories.MetadataRepository;
import io.bigdime.libs.hive.common.Column;
import io.bigdime.libs.hive.table.HiveTableManger;
import io.bigdime.libs.hive.table.TableSpecification;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.hcatalog.common.HCatException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
//import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

/**
 * For Column Schema Validator intergration test cases, please run this test before running
 * three column validator integration test cases
 * this class is creating 3 hive tables in one database, and 3 entities in metastore
 * in local mysql 
 * 
 * @author Rita Liu
 *
 */

@ContextConfiguration(locations = "classpath:META-INF/application-context.xml")
public class TablesCreationIntegrationTest extends AbstractTestNGSpringContextTests {
	
	@Autowired
	MetadataStore metadataStore;
	
	Metasegment metasegment;

	Entitee entities;
	
	@Autowired
	MetadataRepository repository;
	
	@Autowired
	DataTypeRepository dataTypeRepository;
	
	Properties props = new Properties();
	
	HiveTableManger hiveTableManager = null;
	
	private static final Logger logger = LoggerFactory.getLogger(TablesCreationIntegrationTest.class);

	@BeforeClass
	public void init() throws Exception {
		//connect to local hive 
		props.put(HiveConf.ConfVars.METASTOREURIS, "thrift://sandbox.hortonworks.com:" + 9083);
		hiveTableManager = HiveTableManger.getInstance(props);
	}
	
	@BeforeTest
	public void setup() {
		logger.info("Setting the environment");
	}
	
	/**
	 * this method is to test first entity with 2 attributes(user_id, dt) created in metastore
	 * 
	 * @throws MetadataAccessException
	 */
	@Test(priority = 1)
	public void testCreateFirstEntity() throws MetadataAccessException {
		Set<Attribute> AttributesSet = new LinkedHashSet<Attribute>();
		Attribute attribute = new Attribute();
		attribute.setAttributeName("USER_ID");
		attribute.setAttributeType("int");
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
		entities.setEntityName("ENTITYNAME_1");
		entities.setEntityLocation("/data/UnitAdaptor/raw/ENTITYNAME_1");
		entities.setDescription("HDFS LOCATION");
		entities.setVersion(1.1);

		entities.setAttributes(AttributesSet);

		entitySet.add(entities);

		metasegment = new Metasegment();
		metasegment.setAdaptorName("UnitAdaptor");
		metasegment.setSchemaType("HIVE");
		metasegment.setEntitees(entitySet);
		metasegment.setDatabaseName("test");
		metasegment.setDatabaseLocation("");
		metasegment.setRepositoryType("TARGET");
		metasegment.setIsDataSource("Y");
		metasegment.setCreatedBy("TEST_USER");
		metasegment.setUpdatedBy("TEST_USER");
		
		metadataStore.put(metasegment);
		MetasegmentDTO metdata = repository.findByAdaptorNameAndSchemaType(
				metasegment.getAdaptorName(), metasegment.getSchemaType());
		Assert.assertNotNull(metdata.getId());		
	}
	/**
	 * create second entity with 3 attributes (id, first_name, dt)
	 * 
	 * @throws MetadataAccessException
	 */
	@Test(priority = 2)
	public void testCreateSecondEntity() throws MetadataAccessException {

		Set<Attribute> AttributesSet = new LinkedHashSet<Attribute>();
		Attribute attribute = new Attribute();
		attribute.setAttributeName("id");
		attribute.setAttributeType("int");
		attribute.setIntPart("INTPART5");
		attribute.setFractionalPart("FRACTIONALPART5");
		attribute.setNullable("NO");
		attribute.setComment("NOT NULL");
		attribute.setFieldType("COLUMN");
		AttributesSet.add(attribute);
		
		Attribute attribute2 = new Attribute();
		attribute2.setAttributeName("first_name");
		attribute2.setAttributeType("string");
		attribute2.setIntPart("INTPART5");
		attribute2.setFractionalPart("FRACTIONALPART5");
		attribute2.setNullable("NO");
		attribute2.setComment("NOT NULL");
		attribute2.setFieldType("COLUMN");
		AttributesSet.add(attribute2);
		
		Attribute attribute1 = new Attribute();
		attribute1.setAttributeName("dt");

		attribute1.setAttributeType("string");
		attribute1.setIntPart("INTPART6");
		attribute1.setFractionalPart("FRACTIONALPART6");
		attribute1.setNullable("NO");
		attribute1.setComment("NOT NULL");
		attribute1.setFieldType("PARTITIONCOLUMN");
		AttributesSet.add(attribute1);
		

		Set<Entitee> entitySet = new HashSet<Entitee>();
		entities = new Entitee();
		entities.setEntityName("ENTITYNAME_2");
		entities.setEntityLocation("/data/UnitAdaptor/raw/ENTITYNAME_2");
		entities.setDescription("HDFS LOCATION");
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
	 * create third entity with 4 attributes(id_num, address, city, state) 
	 * 
	 * @throws MetadataAccessException
	 */
	@Test(priority = 3)
	public void testCreateThirdEntity() throws MetadataAccessException{
		
		Set<Attribute> AttributesSet = new LinkedHashSet<Attribute>();
		Attribute attribute = new Attribute();
		attribute.setAttributeName("id_num");
		attribute.setAttributeType("int");
		attribute.setIntPart("INTPART5");
		attribute.setFractionalPart("FRACTIONALPART5");
		attribute.setNullable("NO");
		attribute.setComment("NOT NULL");
		attribute.setFieldType("COLUMN");
		AttributesSet.add(attribute);
		
		Attribute attribute1 = new Attribute();
		attribute1.setAttributeName("address");

		attribute1.setAttributeType("string");
		attribute1.setIntPart("INTPART6");
		attribute1.setFractionalPart("FRACTIONALPART6");
		attribute1.setNullable("NO");
		attribute1.setComment("NOT NULL");
		attribute1.setFieldType("COLUMN");
		AttributesSet.add(attribute1);
		
		Attribute attribute2 = new Attribute();
		attribute2.setAttributeName("city");
		attribute2.setAttributeType("string");
		attribute2.setIntPart("INTPART5");
		attribute2.setFractionalPart("FRACTIONALPART5");
		attribute2.setNullable("NO");
		attribute2.setComment("NOT NULL");
		attribute2.setFieldType("COLUMN");
		AttributesSet.add(attribute2);
				
		Attribute attribute3 = new Attribute();
		attribute3.setAttributeName("state");

		attribute3.setAttributeType("string");
		attribute3.setIntPart("INTPART6");
		attribute3.setFractionalPart("FRACTIONALPART6");
		attribute3.setNullable("NO");
		attribute3.setComment("NOT NULL");
		attribute3.setFieldType("COLUMN");
		AttributesSet.add(attribute3);	

		Set<Entitee> entitySet = new HashSet<Entitee>();
		entities = new Entitee();
		entities.setEntityName("ENTITYNAME_3");
		entities.setEntityLocation("/data/UnitAdaptor/raw/ENTITYNAME_3");
		entities.setDescription("HDFS LOCATION");
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
	 * this method is to create first hive table with 2 columns(user_id, dt)
	 * 
	 * @throws HCatException
	 */
	@Test(priority = 4)
	public void createFirstHiveTable() throws HCatException{
		TableSpecification.Builder tableSpecBuilder = new TableSpecification.Builder("test", "ENTITYNAME_1");
		List<Column> columns = new ArrayList<Column>();
		Column column = new Column("user_id", "int", "");
		columns.add(column);
		
		List<Column> partitionColumns = new ArrayList<Column>();
		Column partitionColumn = new Column("dt", "string", "");
		partitionColumns.add(partitionColumn);
		TableSpecification  tableSpec = tableSpecBuilder.columns(columns)
				.fieldsTerminatedBy('\001')
				.linesTerminatedBy('\001').partitionColumns(partitionColumns).fileFormat("Text").build();
		hiveTableManager.createTable(tableSpec);
	}
	
	/**
	 * this method is to create second hive table with 4 columns(id, first_name, last_name, dt)
	 * 
	 * @throws HCatException
	 */
	@Test(priority = 5)
	public void createSecondHiveTable() throws HCatException{
		TableSpecification.Builder tableSpecBuilder = new TableSpecification.Builder("test", "ENTITYNAME_2");
		List<Column> columns = new ArrayList<Column>();
		Column column = new Column("id", "int", "");
		columns.add(column);
		column = new Column("first_name", "varchar", "");
		columns.add(column);
		column = new Column("last_name", "varchar", "");
		columns.add(column);

		List<Column> partitionColumns = new ArrayList<Column>();
		Column partitionColumn = new Column("dt", "string", "");
		partitionColumns.add(partitionColumn);
		TableSpecification  tableSpec = tableSpecBuilder.columns(columns)
				.fieldsTerminatedBy('\001')
				.linesTerminatedBy('\001').partitionColumns(partitionColumns).fileFormat("Text").build();
		hiveTableManager.createTable(tableSpec);
	}
	
	/**
	 * this method is to create third hive table with 3 columns(id_num, address, state)
	 * 
	 * @throws HCatException
	 */
	@Test(priority = 6)
	public void createThirdHiveTable() throws HCatException{
		TableSpecification.Builder tableSpecBuilder = new TableSpecification.Builder("test", "ENTITYNAME_3");
		List<Column> columns = new ArrayList<Column>();
		Column column = new Column("id_num", "int", "");
		columns.add(column);
		column = new Column("address", "varchar", "");
		columns.add(column);
		column = new Column("state", "varchar", "");
		columns.add(column);

		TableSpecification  tableSpec = tableSpecBuilder.columns(columns)
				.fieldsTerminatedBy('\001')
				.linesTerminatedBy('\001').fileFormat("Text").build();
		hiveTableManager.createTable(tableSpec);
	}
	
//	@AfterTest
//	public void cleanup() throws HCatException, MetadataAccessException{
//		hiveTableManager.dropTable("test","ENTITYNAME_1");
//		hiveTableManager.dropTable("test","ENTITYNAME_2");
//		hiveTableManager.dropTable("test","ENTITYNAME_3");
////		metadataStore.remove(metasegment);
////		dataTypeRepository.deleteAll();
//	}
}
