/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.metadata;

import java.lang.reflect.Field;

import io.bigdime.adaptor.metadata.dto.AttributeDTO;
import io.bigdime.adaptor.metadata.dto.DataTypeDTO;

import org.apache.commons.lang3.reflect.FieldUtils;

import io.bigdime.common.testutils.GetterSetterTestHelper;

import org.mockito.Mock;
import org.mockito.Mockito;

import static org.mockito.MockitoAnnotations.initMocks;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Class AttributeTest
 * 
 * @author Neeraj Jain, psabinikari
 * 
 */
public class AttributeDTOTest {

	AttributeDTO attributeDTO;

	@Mock
	AttributeDTO mockAttributeDTO;

	@Mock
	DataTypeDTO mockDataTypeDTO;

	@BeforeClass
	public void init() {
		initMocks(this);
		attributeDTO = new AttributeDTO();
	}

	@Test
	public void testConstructor() {
		attributeDTO = new AttributeDTO("testAttributeName", "testAttribtueType",
				"testIntPart", "testFractionalPart", "testComment", "NotNull",
				"testColumn", "testTargetMappedName", "testDefaultValue");

		Assert.assertEquals(attributeDTO.getAttributeName(), "testAttributeName");

	}

	@Test
	public void testGettersAndSetters() {
		Field[] fields = FieldUtils.getAllFields(AttributeDTO.class);
		for (Field f : fields) {
			if (f.getType() == String.class) {
				GetterSetterTestHelper.doTest(attributeDTO, f.getName(),
						"UNIT-TEST-" + f.getName());
			}

			if (f.getType() == Integer.class) {
				GetterSetterTestHelper.doTest(attributeDTO, f.getName(), 2);
			}

		}

	}

	@Test
	public void testDataType() {
		attributeDTO.setDataType(mockDataTypeDTO);
		Assert.assertEquals(mockDataTypeDTO, attributeDTO.getDataType());
		Mockito.when(mockDataTypeDTO.getDataType()).thenReturn("test");
		attributeDTO.onPostLoad();
	}

}
