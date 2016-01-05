/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.metadata;

import static org.mockito.MockitoAnnotations.initMocks;

import java.util.Date;
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

		Mockito.when(mockMetasegment.getId()).thenReturn(1);
		Mockito.when(mockMetasegment.getAdaptorName()).thenReturn(
				"testAdaptorName");
		Mockito.when(mockMetasegment.getSchemaType()).thenReturn(
				"testSchemaType");
		Mockito.when(mockMetasegment.getDatabaseLocation()).thenReturn(
				"testDatabaseLocation");
		Mockito.when(mockMetasegment.getDatabaseName()).thenReturn(
				"testDatabaseName");
		Mockito.when(mockMetasegment.getDescription()).thenReturn(
				"testDescription");
		Mockito.when(mockMetasegment.getIsDataSource()).thenReturn("Y");
		Mockito.when(mockMetasegment.getCreatedAt()).thenReturn(new Date());
		Mockito.when(mockMetasegment.getCreatedBy()).thenReturn("testUser");
		Mockito.when(mockMetasegment.getUpdatedAt()).thenReturn(new Date());
		Mockito.when(mockMetasegment.getUpdatedBy()).thenReturn("testUser");
		Mockito.when(mockMetasegment.getEntitees()).thenReturn(mockEntiteeSet);
		Assert.assertNotNull(objectEntityMapper
				.mapMetasegmentEntity(mockMetasegment));

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

		Mockito.when(mockEntiteeSet.size()).thenReturn(1);
		@SuppressWarnings("unchecked")
		Iterator<Entitee> mockIterator = Mockito.mock(Iterator.class);
		Mockito.when(mockEntiteeSet.iterator()).thenReturn(mockIterator);
		Mockito.when(mockIterator.hasNext()).thenReturn(true, false);
		Mockito.when(mockIterator.next()).thenReturn(mockEntitee);
		Assert.assertNotNull(objectEntityMapper
				.mapEntiteeSetEntity(mockEntiteeSet));

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
