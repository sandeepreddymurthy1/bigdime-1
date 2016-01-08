/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.metadata;

import static org.mockito.Mockito.when;

import java.util.Set;

import io.bigdime.adaptor.metadata.dto.AttributeDTO;
import io.bigdime.adaptor.metadata.dto.EntiteeDTO;

import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Class EntiteeTest
 * 
 * @author Neeraj Jain, psabinikari
 * 
 */
public class EntiteeDTOTest {

	@Test
	public void testGettersAndSettersTest() {

		EntiteeDTO entity = new EntiteeDTO();
		EntiteeDTO mockEntity = Mockito.mock(EntiteeDTO.class);

		// id
		entity.setId(1);
		Assert.assertNotEquals(entity.getId(), 2);
		ReflectionTestUtils.setField(entity, "id", 7);
		when(mockEntity.getId()).thenReturn(7);
		Assert.assertEquals(entity.getId(), mockEntity.getId());

		// entityName
		entity.setEntityName("test");
		Assert.assertNotEquals(entity.getEntityName(), "testEntityName");
		ReflectionTestUtils.setField(entity, "entityName", "testEntityName");
		when(mockEntity.getEntityName()).thenReturn("testEntityName");
		Assert.assertEquals(entity.getEntityName(), mockEntity.getEntityName());

		// entityLocation
		entity.setEntityLocation("test");
		Assert.assertNotEquals(entity.getEntityLocation(), "testEntityLocation");
		ReflectionTestUtils.setField(entity, "entityLocation",
				"testEntityLocation");
		when(mockEntity.getEntityLocation()).thenReturn("testEntityLocation");
		Assert.assertEquals(entity.getEntityLocation(),
				mockEntity.getEntityLocation());

		// Verion
		entity.setVersion(2.0);
		Assert.assertNotEquals(entity.getVersion(), 3.0);
		ReflectionTestUtils.setField(entity, "version", 1.0);
		when(mockEntity.getVersion()).thenReturn(1.0);
		Assert.assertEquals(entity.getVersion(), mockEntity.getVersion());

		// Description
		entity.setDescription("test");
		Assert.assertNotEquals(entity.getDescription(), "testDescription");
		ReflectionTestUtils.setField(entity, "description", "testDescription");
		when(mockEntity.getDescription()).thenReturn("testDescription");
		Assert.assertEquals(entity.getDescription(),
				mockEntity.getDescription());

		// entitee
		// @SuppressWarnings("unused")
		// Attribute mockAttribute = Mockito.mock(Attribute.class);
		@SuppressWarnings("unchecked")
		Set<AttributeDTO> mockAttributeSet = Mockito.mock(Set.class);
		ReflectionTestUtils.setField(entity, "attributes", mockAttributeSet);
		when(mockEntity.getAttributes()).thenReturn(mockAttributeSet);
		Assert.assertNotNull(entity.getAttributes());

	}

}
