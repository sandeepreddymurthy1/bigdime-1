/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.runtimeinfo.integration;


import io.bigdime.runtimeinfo.impl.RuntimeInfoRepositoryService;
import io.bigdime.core.runtimeinfo.RuntimeInfoStore;
import io.bigdime.core.runtimeinfo.RuntimeInfo;
import io.bigdime.core.runtimeinfo.RuntimeInfoStoreException;
import io.bigdime.runtime.ObjectEntityMapper;
import io.bigdime.runtimeinfo.DTO.RuntimeInfoDTO;
import io.bigdime.runtimeinfo.DTO.RuntimePropertyDTO;
import io.bigdime.runtimeinfo.repositories.RuntimeInfoRepository;
import io.bigdime.core.runtimeinfo.RuntimeInfoStore.Status;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

@ContextConfiguration(locations = "classpath:META-INF/application-context.xml")
public class AdaptorRuntimeInfoStoreImplTest extends AbstractTestNGSpringContextTests{
	
	RuntimeInfo runtimeInfo;
	
	@Autowired
	RuntimeInfoStore<RuntimeInfo> adaptorRuntimeInfoStoreImpl;
	
	@Autowired
	RuntimeInfoRepository runtimeInfoRepository;
	
	@Autowired
	ObjectEntityMapper objectEntityMapper;
	
	Map<String,String> properties;
	@Autowired
	RuntimeInfoRepositoryService runtimeInfoRepositoryService;
	
	@BeforeTest
	public void init() throws Exception {
		
		
		properties = new HashMap<String,String>();
		properties.put("Key1", "value1");
		properties.put("key2", "value2");
		
		
		
		runtimeInfo = new RuntimeInfo();
		runtimeInfo.setAdaptorName("adaptorName");
		runtimeInfo.setEntityName("entityName");
		runtimeInfo.setInputDescriptor("descriptors");
		runtimeInfo.setNumOfAttempts(Integer.toString(9));
		runtimeInfo.setStatus(Status.VALIDATED);
		runtimeInfo.setProperties(properties);
		
		
	}
	
	
	@Test(priority=1)
	public void testPut() throws RuntimeInfoStoreException {
		
		System.out.println("runtime is "+runtimeInfo);
		System.out.println("runtime adaptor name is "+runtimeInfo.getAdaptorName());
		if (runtimeInfo.getAdaptorName().length() > 0) {
			System.out.println("adaptor element are "+runtimeInfo.getAdaptorName());
		Assert.assertTrue(adaptorRuntimeInfoStoreImpl.put(runtimeInfo));
		
		}else {
			System.out.println("don't have content");
		}
		
		
	}
	
	@Test(priority=2)
		public void testUpdate() throws RuntimeInfoStoreException {
		runtimeInfo.setStatus(Status.STARTED);
		Assert.assertTrue(adaptorRuntimeInfoStoreImpl.put(runtimeInfo));
	}
	
	
	@Test(priority=3)
	public void testGet() throws RuntimeInfoStoreException{
		RuntimeInfo runtimeData = adaptorRuntimeInfoStoreImpl.get(runtimeInfo.getAdaptorName(), runtimeInfo.getEntityName(), runtimeInfo.getInputDescriptor());
		System.out.println("Adaptor Name is "+runtimeData.getAdaptorName());
		Assert.assertNotNull(runtimeData);
	}
	
	@Test(priority=4)
	public void testGetAll() throws RuntimeInfoStoreException{
		List<RuntimeInfo> runtimeInfoList = (List<RuntimeInfo>) adaptorRuntimeInfoStoreImpl.getAll(runtimeInfo.getAdaptorName(), runtimeInfo.getEntityName());
	    Assert.assertEquals(runtimeInfoList.size(), 1);
	}
	
	@Test(priority=5)
	public void testGetAllWithStatus() throws RuntimeInfoStoreException {
		
		runtimeInfo = new RuntimeInfo();
		runtimeInfo.setAdaptorName("adaptorName");
		runtimeInfo.setEntityName("entityName");
		runtimeInfo.setInputDescriptor("descriptor1");
		runtimeInfo.setNumOfAttempts(Integer.toString(9));
		runtimeInfo.setStatus(Status.ROLLED_BACK);
		properties = new HashMap<String,String>();
		runtimeInfo.setProperties(properties);
		Assert.assertTrue(adaptorRuntimeInfoStoreImpl.put(runtimeInfo));
		List<RuntimeInfo> runtimeInfoList = (List<RuntimeInfo>) adaptorRuntimeInfoStoreImpl.getAll(runtimeInfo.getAdaptorName(), runtimeInfo.getEntityName(), runtimeInfo.getStatus());
		Assert.assertEquals(runtimeInfoList.size(), 1);   
	}
	
	@Test(priority=6)
	public void testGetLatest() throws RuntimeInfoStoreException {
		Assert.assertNotNull(adaptorRuntimeInfoStoreImpl.getLatest("adaptorName", "entityName"));
	}
	
	@Test(priority=7)
	public void testDelete() throws RuntimeInfoStoreException {
		//RuntimeInfoRepositoryService runtimeInfoRepositoryService = new RuntimeInfoRepositoryService();
	    List<RuntimeInfoDTO> runtimeInfoDTOList = runtimeInfoRepositoryService.get("adaptorName", "entityName");
	    runtimeInfoRepository.delete(runtimeInfoDTOList);
	    
	}

}

