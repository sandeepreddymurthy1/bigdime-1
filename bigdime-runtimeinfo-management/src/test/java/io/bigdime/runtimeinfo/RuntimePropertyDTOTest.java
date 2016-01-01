/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.runtimeinfo;

import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import java.lang.reflect.Field;

import io.bigdime.common.testutils.GetterSetterTestHelper;
import io.bigdime.core.runtimeinfo.RuntimeInfoStore.Status;
import io.bigdime.runtimeinfo.DTO.RuntimeInfoDTO;
import io.bigdime.runtimeinfo.DTO.RuntimePropertyDTO;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.mockito.Mock;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class RuntimePropertyDTOTest {
	
	RuntimePropertyDTO runtimeProperty;
	@Mock
	RuntimePropertyDTO mockRuntimeProperty;
	
	@BeforeClass
	public void init() {
		initMocks(this);
		runtimeProperty = new RuntimePropertyDTO();
	}
	
	
	@Test
	public void testConstructor() {
		runtimeProperty = new RuntimePropertyDTO("testKey","testValue");
	    Assert.assertEquals(runtimeProperty.getKey(),"testKey");
	    
	    System.out.println("RuntimeProperties values are "+runtimeProperty);
	}
	
	@Test
	public void testGettersAndSetters() {
		Field[] fields = FieldUtils.getAllFields(RuntimePropertyDTO.class);
		for (Field f : fields) {
			if (f.getType() == String.class){
				GetterSetterTestHelper.doTest(runtimeProperty, f.getName(), "UNIT-TEST-"+f.getName());
			}
			
			if(f.getType() == int.class) {
				GetterSetterTestHelper.doTest(runtimeProperty, f.getName(), 2);
			}
			/*Date date = new Date();
			if(f.getType() == Date.class){
				GetterSetterTestHelper.doTest(runtimeInfoDTO, f.getName(), date+"");
			}*/
		}
		
	}
	
	
	/*@Test
	public void testId() {
		
		runtimeProperty.setRuntimePropertyId(1);
		Assert.assertNotEquals(runtimeProperty.getRuntimePropertyId(), 4);
		ReflectionTestUtils.setField(runtimeProperty, "runtimePropertyId", 7);
		when(mockRuntimeProperty.getRuntimePropertyId()).thenReturn(7);
		Assert.assertEquals(runtimeProperty.getRuntimePropertyId(), mockRuntimeProperty.getRuntimePropertyId());

		
	}
	
	@Test
	public void testKey() {
		runtimeProperty.setKey("test");
		Assert.assertNotEquals(runtimeProperty.getKey(), "testKey");
		ReflectionTestUtils.setField(runtimeProperty, "key", "testKey");
		when(mockRuntimeProperty.getKey()).thenReturn("testKey");
		Assert.assertEquals(runtimeProperty.getKey(), mockRuntimeProperty.getKey());

		
	}
	
	@Test
	public void testValue() {
		runtimeProperty.setValue("test");
		Assert.assertNotEquals(runtimeProperty.getValue(), "testValue");
		ReflectionTestUtils.setField(runtimeProperty, "value", "testValue");
		when(mockRuntimeProperty.getValue()).thenReturn("testValue");
		Assert.assertEquals(runtimeProperty.getValue(), mockRuntimeProperty.getValue());

		
	}
	*/

}
