/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.metadata;

import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import io.bigdime.adaptor.metadata.dto.MetasegmentDTO;
import io.bigdime.adaptor.metadata.dto.EntiteeDTO;
import io.bigdime.common.testutils.GetterSetterTestHelper;

/**
 * Class MetasegmentTest
 * 
 * @author Neeraj Jain, psabinikari
 * 
 */
public class MetasegmentDTOTest {
	
	private static String TEST="test";

	MetasegmentDTO metasegment;

	@Mock
	MetasegmentDTO mockMetasegment;

	@Mock
	EntiteeDTO mockEntity;

	@BeforeClass
	public void init() {
		initMocks(this);
		metasegment = new MetasegmentDTO();
	}

	@Test
	public void testConstructor() {
		metasegment = new MetasegmentDTO("TestAdapterName", "testSchemaType",
				"testDatabaseName", "testDatabaseLocation",
				"testRepositoryType", "Y", "testDescription",
				new HashSet<EntiteeDTO>(), "testUser", "testUser");
		
		//metasegment = new MetasegmentDTO("TestAdapterName", "TestSchemaType", "TestDatabaseName", "TestDatabaseLocation", "TestRepositoryType", "Y", "TestDescription", new HashSet<EntiteeDTO>(), "TestUser", "TestUser");

		Assert.assertEquals(metasegment.getAdaptorName(), "TestAdapterName");
	}

	@Test
	public void testGettersAndSetters() {
		Field[] fields = FieldUtils.getAllFields(MetasegmentDTO.class);
		for (Field f : fields) {
			if (f.getType() == String.class) {
				GetterSetterTestHelper.doTest(metasegment, f.getName(),
						"UNIT-TEST-" + f.getName());
			}

			if (f.getType() == Integer.class) {
				GetterSetterTestHelper.doTest(metasegment, f.getName(), 2);
			}

		}

	}



	@SuppressWarnings("unchecked")
	@Test
	public void testGetEntity() {
		MetasegmentDTO metasegmentDTO=new MetasegmentDTO();
		Set<EntiteeDTO> entitees=new HashSet<EntiteeDTO>();
		EntiteeDTO entiteeDTO=new EntiteeDTO();
		entiteeDTO.setDescription(TEST);
		entiteeDTO.setEntityLocation(TEST);
		entiteeDTO.setEntityName(TEST);
		entitees.add(entiteeDTO);
		metasegmentDTO.setEntitees(entitees);
		for(EntiteeDTO entity:metasegmentDTO.getEntitees()){
			Assert.assertEquals(entity.getDescription(), TEST);
			Assert.assertEquals(entity.getEntityLocation(), TEST);
			Assert.assertEquals(entity.getEntityName(), TEST);
		}
	}
	@Test
	public void testNullGetEntity() {
		Assert.assertNull(metasegment.getEntity("Test1"));
	}

}
