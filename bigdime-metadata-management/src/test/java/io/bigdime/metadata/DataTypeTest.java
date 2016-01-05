/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.metadata;

import static org.mockito.MockitoAnnotations.initMocks;

import java.lang.reflect.Field;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.mockito.Mock;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import io.bigdime.adaptor.metadata.model.DataType;
import io.bigdime.common.testutils.GetterSetterTestHelper;

/**
 * Class DataTypeTest
 * 
 * @author Neeraj Jain, psabinikari
 * 
 */
public class DataTypeTest {
	
	DataType dataType;
	
	@Mock
	DataType mockDataType;
	
	@BeforeClass
	public void init() {
		initMocks(this);
		dataType = new DataType();
	}

	@Test
	public void testConstructor() {
		dataType = new DataType("testDataType", "testDescription");

		Assert.assertEquals(dataType.getDataType(), "testDataType");
	}
	
	@Test
	public void testGettersAndSetters() {
		Field[] fields = FieldUtils.getAllFields(DataType.class);
		for (Field f : fields) {
			if (f.getType() == String.class) {
				GetterSetterTestHelper.doTest(dataType, f.getName(),
						"UNIT-TEST-" + f.getName());
			}

			if (f.getType() == int.class) {
				GetterSetterTestHelper.doTest(dataType, f.getName(), 2);
			}

		}

	}
	
}


