/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.runtimeinfo.integration;


import java.util.HashSet;
import java.util.List;
import java.util.Set;

import io.bigdime.runtimeinfo.impl.RuntimeInfoRepositoryService;
import io.bigdime.core.runtimeinfo.RuntimeInfoStore.Status;
import io.bigdime.runtimeinfo.DTO.RuntimeInfoDTO;
import io.bigdime.runtimeinfo.DTO.RuntimePropertyDTO;
import io.bigdime.runtimeinfo.repositories.RuntimeInfoRepository;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
//import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

@ContextConfiguration(locations = "classpath:META-INF/application-context.xml")
public class RuntimeInfoRepositoryServiceTest extends AbstractTestNGSpringContextTests {
	
	
	RuntimeInfoDTO runtimeInfo;
	
	@Autowired
	RuntimeInfoRepositoryService runtimeInfoRepositoryService;
	
	@Autowired
	RuntimeInfoRepository runtimeInfoRepository;
	
	List<RuntimeInfoDTO> runtimeInfoList;
	
	/**
	 * Load the test data
	 * 
	 * @throws Exception
	 */

	@BeforeTest
	public void init() throws Exception {
		
		
		Set<RuntimePropertyDTO> runtimePropertySet = new HashSet<RuntimePropertyDTO>();
		RuntimePropertyDTO runtimeProperty = new RuntimePropertyDTO();
		runtimeProperty.setKey("Key1");
		runtimeProperty.setValue("value1");
		
		runtimePropertySet.add(runtimeProperty);
		
		RuntimePropertyDTO runtimeProperty1 = new RuntimePropertyDTO();
		runtimeProperty1.setKey("Key2");
		runtimeProperty1.setValue("value2");
		
		runtimePropertySet.add(runtimeProperty1);
		
		runtimeInfo = new RuntimeInfoDTO();
		runtimeInfo.setAdaptorName("adaptorName");
		runtimeInfo.setEntityName("entityName");
		runtimeInfo.setInputDescriptor("descriptors");
		runtimeInfo.setNumOfAttempts(9);
		runtimeInfo.setStatus(Status.VALIDATED);
		runtimeInfo.setRuntimeProperties(runtimePropertySet);
		
		
	}
	
	@Test(priority=8)
	public void testCreate(){
		Assert.assertTrue(runtimeInfoRepositoryService.create(runtimeInfo));
		
		
	}

	
	@Test(priority=9)
	public void testGetAdapterAndEntityName() {
		 runtimeInfoList = runtimeInfoRepositoryService.get("adaptorName", "entityName");
		Assert.assertEquals(runtimeInfoList.size(), 1);
	}
	
	@Test(priority =10)
	public void testGetAdapterAndEntityAndDescriptor() {
		RuntimeInfoDTO runtimeInf = runtimeInfoRepositoryService.get("adaptorName", "entityName", "descriptors");
		Assert.assertNotNull(runtimeInf);
	}
	
	@Test(priority=11)
	public void testUpdateEntry() {
		runtimeInfo.setNumOfAttempts(10);
		runtimeInfo.setStatus(Status.ROLLED_BACK);
		Assert.assertTrue(runtimeInfoRepositoryService.create(runtimeInfo));
		
	}
	
	@Test(priority=12)
	public void testGetLatestRecord() {
		System.out.println(runtimeInfoRepositoryService.getLatestRecord("adaptorName", "entityName"));
		Assert.assertNotNull(runtimeInfoRepositoryService.getLatestRecord("adaptorName", "entityName"));
	}
	@Test(priority=13)
	public void testDeleteEnrtries() {
		
		RuntimeInfoDTO runtimeInformation = runtimeInfoRepositoryService.get("adaptorName", "entityName","descriptors");
		runtimeInfoRepository.delete(runtimeInformation);
		runtimeInfo = runtimeInfoRepositoryService.get("adaptorName", "entityName","descriptors");
		Assert.assertNull(runtimeInfo);
		
	}
}

