/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.runtimeinfo;

import static org.mockito.MockitoAnnotations.initMocks;

import java.lang.reflect.Field;
import java.util.Set;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.mockito.Mock;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import io.bigdime.common.testutils.GetterSetterTestHelper;
import io.bigdime.core.runtimeinfo.RuntimeInfoStore.Status;
import io.bigdime.runtimeinfo.DTO.RuntimeInfoDTO;
import io.bigdime.runtimeinfo.DTO.RuntimePropertyDTO;

public class RuntimeInfoDTOTest {

	RuntimeInfoDTO runtimeInfoDTO;

	@Mock
	RuntimeInfoDTO mockRuntimInfo;
	@Mock
	Set<RuntimePropertyDTO> mockRuntimePropertiesSetDTO;

	@BeforeClass
	public void init() {
		initMocks(this);
		runtimeInfoDTO = new RuntimeInfoDTO();
	}

	@Test
	public void testConstructor() {
		// runtimeInfoDTO = new
		// RuntimeInfoDTO("testAdapterName","testEntityName","testInputDescriptor",Status.STARTED,7,mockRuntimePropertiesSetDTO);
		runtimeInfoDTO = new RuntimeInfoDTO("testAdapterName",
				"testEntityName", "testInputDescriptor", Status.STARTED, 9,
				mockRuntimePropertiesSetDTO);
		Assert.assertEquals(runtimeInfoDTO.getAdaptorName(), "testAdapterName");

		// System.out.println("RuntimeInfo test values are "+runtimeInfoDTO);
	}

	@Test
	public void testGettersAndSetters() {
		Field[] fields = FieldUtils.getAllFields(RuntimeInfoDTO.class);
		for (Field f : fields) {
			if (f.getType() == String.class) {
				GetterSetterTestHelper.doTest(runtimeInfoDTO, f.getName(),
						"UNIT-TEST-" + f.getName());
			}

			if (f.getType() == int.class) {
				GetterSetterTestHelper.doTest(runtimeInfoDTO, f.getName(), 2);
			}
			/*
			 * Date date = new Date(); if(f.getType() == Date.class){
			 * GetterSetterTestHelper.doTest(runtimeInfoDTO, f.getName(),
			 * date+""); }
			 */
		}

	}

	/**
	 * Test getters and setters for status field.
	 */
	@Test
	public void testStatusGetterAndSetter() {
		Status status = Status.FAILED;
		runtimeInfoDTO.setStatus(status);
		Assert.assertSame(Status.FAILED, runtimeInfoDTO.getStatus());
	}

	/**
	 * Test getters and setters for CreatedAt field.
	 */
	@Test
	public void testCreatedAtGetterAndSetter() {

		runtimeInfoDTO.setCreatedAt();

		Assert.assertNotNull(runtimeInfoDTO.getCreatedAt());
	}

	/**
	 * Test getters and setters for UpdatedAt field.
	 */
	@Test
	public void testUpdatedAtGetterAndSetter() {

		runtimeInfoDTO.setUpdatedAt();

		Assert.assertNotNull(runtimeInfoDTO.getUpdatedAt());
	}

	/**
	 * Test getters and setters for runtimeProperties field.
	 */
	@Test
	public void testRuntimeProperties() {
		runtimeInfoDTO.setRuntimeProperties(mockRuntimePropertiesSetDTO);
		Assert.assertEquals(mockRuntimePropertiesSetDTO,
				runtimeInfoDTO.getRuntimeProperties());
	}
}
