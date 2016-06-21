/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.metadata;

import static org.mockito.MockitoAnnotations.initMocks;

import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import io.bigdime.adaptor.metadata.ObjectEntityMapper;
import io.bigdime.adaptor.metadata.dto.AttributeDTO;
import io.bigdime.adaptor.metadata.dto.DataTypeDTO;
import io.bigdime.adaptor.metadata.dto.EntiteeDTO;
import io.bigdime.adaptor.metadata.dto.MetasegmentDTO;
import io.bigdime.adaptor.metadata.impl.ObjectEntityMapperImpl;
import io.bigdime.adaptor.metadata.model.Attribute;
import io.bigdime.adaptor.metadata.model.DataType;
import io.bigdime.adaptor.metadata.model.Entitee;
import io.bigdime.adaptor.metadata.model.Metasegment;

import org.mockito.Mock;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class ObjectEntityMapperImplTest {

	ObjectEntityMapper objectEntityMapper;

	@Mock
	MetasegmentDTO mockMetasegmentDTO;

	@Mock
	Metasegment mockMetasegment;

	@Mock
	Set<EntiteeDTO> mockEntiteeDTOSet;

	@Mock
	Set<Entitee> mockEntiteeSet;

	@Mock
	Set<Attribute> mockAttributeSet;

	@Mock
	Set<AttributeDTO> mockAttributeDTOSet;
	@Mock
	Entitee mockEntitee;

	@Mock
	Attribute mockAttribute;

	@Mock
	AttributeDTO mockAttributeDTO;
	@Mock
	EntiteeDTO mockEntiteeDTO;

	@Mock
	DataType mockDataType;

	@Mock
	DataTypeDTO mockDataTypeDTO;

	@Mock
	List<MetasegmentDTO> allSegments;

	@BeforeTest
	public void init() {
		initMocks(this);
		objectEntityMapper = new ObjectEntityMapperImpl();
	}

	@Test
	public void testMapMetasegmentObject() {
		Mockito.when(mockMetasegmentDTO.getId()).thenReturn(1);
		Mockito.when(mockMetasegmentDTO.getAdaptorName()).thenReturn(
				"testAdaptorName");
		Mockito.when(mockMetasegmentDTO.getSchemaType()).thenReturn(
				"testSchemaType");
		Mockito.when(mockMetasegmentDTO.getDatabaseLocation()).thenReturn(
				"testDatabaseLocation");
		Mockito.when(mockMetasegmentDTO.getDatabaseName()).thenReturn(
				"testDatabaseName");
		Mockito.when(mockMetasegmentDTO.getDescription()).thenReturn(
				"testDescription");
		Mockito.when(mockMetasegmentDTO.getIsDataSource()).thenReturn("Y");
		Mockito.when(mockMetasegmentDTO.getCreatedAt()).thenReturn(new Date());
		Mockito.when(mockMetasegmentDTO.getCreatedBy()).thenReturn("testUser");
		Mockito.when(mockMetasegmentDTO.getUpdatedAt()).thenReturn(new Date());
		Mockito.when(mockMetasegmentDTO.getUpdatedBy()).thenReturn("testUser");
		Mockito.when(mockMetasegmentDTO.getEntitees()).thenReturn(
				mockEntiteeDTOSet);
		Assert.assertNotNull(objectEntityMapper
				.mapMetasegmentObject(mockMetasegmentDTO));
	}

	@Test
	public void testMapMetasegmentObjectNull() {
		Assert.assertNull(objectEntityMapper.mapMetasegmentObject(null));
	}

	@Test
	public void testMapMetasegmentEntity() {		
		Metasegment metasegment=new Metasegment();
		metasegment.setAdaptorName("TEST");
		Set<Entitee> entiteeSet=new HashSet<Entitee>();
		Entitee entitee=new Entitee();
		entitee.setDescription("TEST");
		Attribute attribute=new Attribute();
		attribute.setAttributeName("TEST");
		Set<Attribute> attributeSet=new HashSet<Attribute>();
		attributeSet.add(attribute);
		entitee.setAttributes(attributeSet);
		entiteeSet.add(entitee);
		metasegment.setEntitees(entiteeSet);
		Assert.assertEquals(objectEntityMapper.mapMetasegmentEntity(metasegment).getAdaptorName(),"TEST");
		for(EntiteeDTO entiteeDTO:objectEntityMapper.mapMetasegmentEntity(metasegment).getEntitees()){
			Assert.assertEquals(entiteeDTO.getDescription(), "TEST");
			for(AttributeDTO attributesDTO:entiteeDTO.getAttributes()){
				Assert.assertEquals(attributesDTO.getAttributeName(), "TEST");
			}
		}

	}

	@Test
	public void testMapMetasegmentEntityNull() {
		Assert.assertNull(objectEntityMapper.mapMetasegmentEntity(null));
	}

	@Test
	public void testMapEntiteeSetObject() {

		Mockito.when(mockEntiteeDTOSet.size()).thenReturn(1);
		@SuppressWarnings("unchecked")
		Iterator<EntiteeDTO> mockIterator = Mockito.mock(Iterator.class);
		Mockito.when(mockEntiteeDTOSet.iterator()).thenReturn(mockIterator);
		Mockito.when(mockIterator.hasNext()).thenReturn(true, false);
		Mockito.when(mockIterator.next()).thenReturn(mockEntiteeDTO);
		Assert.assertNotNull(objectEntityMapper
				.mapEntiteeSetObject(mockEntiteeDTOSet));

	}

	@Test
	public void testMapEntiteeSetObjectNull() {
		Mockito.when(mockEntiteeDTOSet.size()).thenReturn(0);
		Assert.assertNull(objectEntityMapper
				.mapEntiteeSetObject(mockEntiteeDTOSet));
	}

	@Test
	public void testMapEntiteeSetEntity() {
         
		Set<Entitee> entiteeSet=new HashSet<Entitee>();
		Entitee entitee=new Entitee();
		entitee.setDescription("TEST");
		Set<Attribute> attributeSet =new HashSet<Attribute>();
		attributeSet.add(new Attribute());
		entitee.setAttributes(attributeSet);
		entiteeSet.add(entitee);
		for(EntiteeDTO entiteeDTO:objectEntityMapper.mapEntiteeSetEntity(entiteeSet)){
			Assert.assertEquals(entiteeDTO.getDescription(), "TEST");
		}

	}

	@Test
	public void testMapEntiteeSetEntityNull() {
		Mockito.when(mockEntiteeSet.size()).thenReturn(0);
		Assert.assertNull(objectEntityMapper
				.mapEntiteeSetEntity(mockEntiteeSet));
	}

	@Test
	public void testMapEntiteeDTONull() {
		Assert.assertNull(objectEntityMapper.mapEntiteeDTO(null));
	}

	@Test
	public void testMapEntiteeObjectNull() {
		Assert.assertNull(objectEntityMapper.mapEntiteeObject(null));
	}

	@Test
	public void testMapAttributeEntitySet() {

		Mockito.when(mockAttributeSet.size()).thenReturn(1);
		Iterator<Attribute> mockIterator = Mockito.mock(Iterator.class);
		Mockito.when(mockAttributeSet.iterator()).thenReturn(mockIterator);
		Mockito.when(mockIterator.hasNext()).thenReturn(true, false);
		Mockito.when(mockIterator.next()).thenReturn(mockAttribute);
		Assert.assertNotNull(objectEntityMapper
				.mapAttributeEntitySet(mockAttributeSet));
	}

	@Test
	public void testMapAttributeObjectSet() {

		Mockito.when(mockAttributeDTOSet.size()).thenReturn(1);
		@SuppressWarnings("unchecked")
		Iterator<AttributeDTO> mockIterator = Mockito.mock(Iterator.class);
		Mockito.when(mockAttributeDTOSet.iterator()).thenReturn(mockIterator);
		Mockito.when(mockIterator.hasNext()).thenReturn(true, false);
		Mockito.when(mockIterator.next()).thenReturn(mockAttributeDTO);
		Assert.assertNotNull(objectEntityMapper
				.mapAttributeObjectSet(mockAttributeDTOSet));
	}

	@Test
	public void testMapAttributeEntityNull() {
		Assert.assertNull(objectEntityMapper.mapAttributeEntity(null));
	}

	@Test
	public void testMapAttributeObjectNull() {
		Assert.assertNull(objectEntityMapper.mapAttributeObject(null));
	}

	@Test
	public void testMapDataTypeEntity() {
		Mockito.when(mockDataType.getDataType()).thenReturn("testDataType");
		Mockito.when(mockDataType.getDescription()).thenReturn(
				"testDescription");
		Mockito.when(mockDataType.getDataTypeId()).thenReturn(123);
		Assert.assertNotNull(objectEntityMapper.mapDataTypeEntity(mockDataType));

	}

	@Test
	public void testMapDataTypeObject() {

		Mockito.when(mockDataTypeDTO.getDataType()).thenReturn("testDataType");
		Mockito.when(mockDataTypeDTO.getDescription()).thenReturn(
				"testDescription");
		Mockito.when(mockDataTypeDTO.getDataTypeId()).thenReturn(123);
		Assert.assertNotNull(objectEntityMapper
				.mapDataTypeObject(mockDataTypeDTO));

	}

	@Test
	public void testMapMetasegmentListObject() {
		// List<MetasegmentDTO> allSegments = (List<MetasegmentDTO>)
		// Mockito.mock(MetasegmentDTO.class);
		Mockito.when(allSegments.size()).thenReturn(1);
		Iterator<MetasegmentDTO> mockIterator = Mockito.mock(Iterator.class);
		Mockito.when(allSegments.iterator()).thenReturn(mockIterator);
		Mockito.when(mockIterator.hasNext()).thenReturn(true, false);
		Mockito.when(mockIterator.next()).thenReturn(mockMetasegmentDTO);
		Assert.assertNotNull(objectEntityMapper
				.mapMetasegmentListObject(allSegments));

	}

	@Test
	public void testMapMetasegmentListObjectNull() {
		Mockito.when(allSegments.size()).thenReturn(0);
		Assert.assertNull(objectEntityMapper
				.mapMetasegmentListObject(allSegments));
	}
}
