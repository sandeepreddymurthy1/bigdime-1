/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.runtimeinfo;

import java.util.Iterator;
import java.util.List;

import org.mockito.Mock;
import org.mockito.Mockito;

import static org.mockito.MockitoAnnotations.initMocks;
import static org.mockito.Mockito.*;

import org.springframework.test.util.ReflectionTestUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import io.bigdime.runtime.ObjectEntityMapper;
import io.bigdime.runtimeinfo.impl.AdaptorRuntimeInfoStoreImpl;
import io.bigdime.runtimeinfo.impl.RuntimeInfoRepositoryService;
import io.bigdime.runtimeinfo.DTO.RuntimeInfoDTO;
import io.bigdime.core.runtimeinfo.RuntimeInfo;
import io.bigdime.core.runtimeinfo.RuntimeInfoStore.Status;
import io.bigdime.core.runtimeinfo.RuntimeInfoStoreException;

public class AdaptorRuntimeInfoStoreImplTest {

	@Mock
	List<RuntimeInfoDTO> mockRuntimeInfoDTOList;
	@Mock
	Iterator<RuntimeInfoDTO> mockIteratorDTO;
	@Mock
	RuntimeInfo mockRuntimeInfo;
	
	@Mock
	RuntimeInfoDTO mockRuntimeInfoDTO;

	AdaptorRuntimeInfoStoreImpl adaptorRuntimeInfoStoreImpl;
	
	@Mock
	ObjectEntityMapper mockObjectEntityMapper;

	@Mock
	RuntimeInfoRepositoryService mockRuntimeInforRepositoryService;

	@BeforeClass
	public void init() {

		initMocks(this);
		adaptorRuntimeInfoStoreImpl = new AdaptorRuntimeInfoStoreImpl();
		ReflectionTestUtils.setField(adaptorRuntimeInfoStoreImpl,
				"runtimeInfoRepositoryService",
				mockRuntimeInforRepositoryService);
		ReflectionTestUtils.setField(adaptorRuntimeInfoStoreImpl, "objectEntityMapper", mockObjectEntityMapper);

	}

	@Test
	public void testPut() throws RuntimeInfoStoreException {

		
		Mockito.when(mockObjectEntityMapper.mapEntityObject(mockRuntimeInfo)).thenReturn(mockRuntimeInfoDTO);
		Mockito.when(mockRuntimeInforRepositoryService.create(mockRuntimeInfoDTO))
		.thenReturn(true);
		Assert.assertTrue(adaptorRuntimeInfoStoreImpl.put(mockRuntimeInfo));
	}

	@Test(expectedExceptions = IllegalArgumentException.class)
	public void testNullForPut() throws RuntimeInfoStoreException {
		adaptorRuntimeInfoStoreImpl.put(null);
	}

	@Test
	public void testGetAllWithStatus() throws RuntimeInfoStoreException {

		Mockito.when(
				mockRuntimeInforRepositoryService.get(Mockito.anyString(),
						Mockito.anyString())).thenReturn(mockRuntimeInfoDTOList);
		Mockito.when(mockRuntimeInfoDTOList.iterator()).thenReturn(mockIteratorDTO);
		Mockito.when(mockIteratorDTO.hasNext()).thenReturn(true, false);
		Mockito.when(mockIteratorDTO.next()).thenReturn(mockRuntimeInfoDTO);

		Mockito.when(mockRuntimeInfoDTO.getStatus()).thenReturn(Status.FAILED);

		Assert.assertEquals(
				adaptorRuntimeInfoStoreImpl.getAll("aaptorName", "entityName",
						Status.FAILED).size(), 1);
	}

	@Test(expectedExceptions = IllegalArgumentException.class)
	public void testNullForGetAllWithStatus() throws RuntimeInfoStoreException {
		adaptorRuntimeInfoStoreImpl.getAll("aaptorName", null, Status.FAILED);
	}

	@Test
	public void testToGetAll() throws RuntimeInfoStoreException {
		Mockito.when(
				mockRuntimeInforRepositoryService.get(
						Mockito.any(String.class), Mockito.any(String.class)))
				.thenReturn(mockRuntimeInfoDTOList);
		when(mockRuntimeInfoDTOList.size()).thenReturn(2);
		@SuppressWarnings("unchecked")
		List<RuntimeInfo> mockRuntimeInfoList = Mockito.mock(List.class);
          Mockito.when(mockObjectEntityMapper.mapObjectList(mockRuntimeInfoDTOList)).thenReturn(mockRuntimeInfoList);
      
		 Assert.assertNotNull(adaptorRuntimeInfoStoreImpl.getAll(
				"adaptorName", "entityName"));
	}

	@Test(expectedExceptions = IllegalArgumentException.class)
	public void testNullForGetAll() throws RuntimeInfoStoreException {

		adaptorRuntimeInfoStoreImpl.getAll(null, "entityName");

	}
	
	@Test
	public void testGetReturnNull() throws RuntimeInfoStoreException{
		
		
		
		Mockito.when(
				mockRuntimeInforRepositoryService.get(
						Mockito.any(String.class), Mockito.any(String.class)))
				.thenReturn(mockRuntimeInfoDTOList);
		
		 Assert.assertNull(adaptorRuntimeInfoStoreImpl.getAll(
				"adaptorName", "entityName"));
		
	}

	@Test
	public void testGet() throws RuntimeInfoStoreException {

		Mockito.when(
				mockRuntimeInforRepositoryService.get(
						Mockito.any(String.class), Mockito.any(String.class),
						Mockito.anyString())).thenReturn(
				mockRuntimeInfoDTO);
		Mockito.when(mockObjectEntityMapper.mapObject(mockRuntimeInfoDTO)).thenReturn(mockRuntimeInfo);
		Assert.assertNotNull(adaptorRuntimeInfoStoreImpl.get("adaptorName",
				"entityName", "descriptor"));

	}

	@Test(expectedExceptions = IllegalArgumentException.class)
	public void testNullGet() throws RuntimeInfoStoreException {
		adaptorRuntimeInfoStoreImpl.get(null, "entityName", "descriptor");
	}
	
	@Test
	public void getLatest() throws RuntimeInfoStoreException {
		Assert.assertNull(adaptorRuntimeInfoStoreImpl.getLatest("adaptorName", "entityName"));
	}
	
	@Test(expectedExceptions = IllegalArgumentException.class)
		public void testNullForGetLates() throws RuntimeInfoStoreException {
		adaptorRuntimeInfoStoreImpl.getLatest(null, "entityName");
			
		}
	

}
