/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.metadata;

import java.lang.reflect.Field;



import java.util.HashSet;
import java.util.Set;

import io.bigdime.adaptor.metadata.dto.AttributeDTO;
import io.bigdime.adaptor.metadata.dto.DataTypeDTO;
import io.bigdime.adaptor.metadata.dto.EntiteeDTO;

import org.mockito.Mock;
import org.mockito.Mockito;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

/**
 * Class EntiteeTest
 * 
 * @author Neeraj Jain, psabinikari
 * 
 */
public class EntiteeDTOTest {
	
	private static String TEST="test";
	private static int INTTEST=0;
	
	@Mock
	EntiteeDTO mockEntiteeDTO ;
	
	@BeforeTest
	public void init(){
		initMocks(this);
	}

	@Test
	public void testGettersAndSettersTest() {

		EntiteeDTO entity = new EntiteeDTO();
		EntiteeDTO mockEntity = Mockito.mock(EntiteeDTO.class);
		Set<AttributeDTO> attributeSet =new HashSet<AttributeDTO>();
		AttributeDTO attributeDTO=new AttributeDTO();
		attributeDTO.setAttributeName(TEST);
		attributeDTO.setAttributeType(TEST);
		attributeDTO.setComment(TEST);
		attributeDTO.setDataType(new DataTypeDTO());
		attributeDTO.setDefaultValue(TEST);
		attributeDTO.setFieldType(TEST);
		attributeDTO.setFractionalPart(TEST);
		attributeDTO.setId(Integer.valueOf(INTTEST));
		attributeDTO.setIntPart(TEST);
		attributeDTO.setMappedAttributeName(TEST);
		attributeDTO.setNullable(TEST);
		attributeSet.add(attributeDTO);
		entity.setAttributes(attributeSet);
		ReflectionTestUtils.setField(entity, "attributes", attributeSet);
		when(mockEntity.getAttributes()).thenReturn(attributeSet);
		for(AttributeDTO attrDTO:entity.getAttributes()){
			Assert.assertEquals(attrDTO.getAttributeName(), TEST);
			Assert.assertEquals(attrDTO.getAttributeType(), TEST);
			Assert.assertEquals(attrDTO.getComment(), TEST);
			Assert.assertTrue (attrDTO.getDataType() instanceof DataTypeDTO);
			Assert.assertEquals(attrDTO.getDefaultValue(), TEST);
			Assert.assertEquals(attrDTO.getFieldType(), TEST);
			Assert.assertEquals(attrDTO.getFractionalPart(), TEST);
			Assert.assertEquals(attrDTO.getId(), Integer.valueOf(INTTEST));
			Assert.assertEquals(attrDTO.getIntPart(), TEST);
			Assert.assertEquals(attrDTO.getMappedAttributeName(), TEST);
			Assert.assertEquals(attrDTO.getNullable(), TEST);
		}

	}
	
	@Test
	public void testConstructor(){
		Set<AttributeDTO> attributeSet =new HashSet<AttributeDTO>();
		AttributeDTO attributeDTO=new AttributeDTO();
		attributeDTO.setAttributeName(TEST);
		attributeDTO.setAttributeType(TEST);
		attributeDTO.setComment(TEST);
		attributeDTO.setDataType(new DataTypeDTO());
		attributeDTO.setDefaultValue(TEST);
		attributeDTO.setFieldType(TEST);
		attributeDTO.setFractionalPart(TEST);
		attributeDTO.setId(Integer.valueOf(INTTEST));
		attributeDTO.setIntPart(TEST);
		attributeDTO.setMappedAttributeName(TEST);
		attributeDTO.setNullable(TEST);
		attributeSet.add(attributeDTO);
		EntiteeDTO entity = new EntiteeDTO(TEST, TEST, 1.0, TEST, attributeSet);
		Assert.assertEquals(entity.getDescription(), TEST);
		Assert.assertEquals(entity.getEntityLocation(), TEST);
		Assert.assertEquals(entity.getVersion(), 1.0);
		for(AttributeDTO attrDTO:entity.getAttributes()){
			Assert.assertEquals(attrDTO.getAttributeName(), TEST);
			Assert.assertEquals(attrDTO.getAttributeType(), TEST);
			Assert.assertEquals(attrDTO.getComment(), TEST);
			Assert.assertTrue (attrDTO.getDataType() instanceof DataTypeDTO);
			Assert.assertEquals(attrDTO.getDefaultValue(), TEST);
			Assert.assertEquals(attrDTO.getFieldType(), TEST);
			Assert.assertEquals(attrDTO.getFractionalPart(), TEST);
			Assert.assertEquals(attrDTO.getId(), Integer.valueOf(INTTEST));
			Assert.assertEquals(attrDTO.getIntPart(), TEST);
			Assert.assertEquals(attrDTO.getMappedAttributeName(), TEST);
			Assert.assertEquals(attrDTO.getNullable(), TEST);
		}
		
	}
	@Test
	public void testClone(){
		EntiteeDTO entiteeDTO =new EntiteeDTO();
		Assert.assertTrue(entiteeDTO.clone() instanceof EntiteeDTO);
	}
}
