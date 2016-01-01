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

import io.bigdime.adaptor.metadata.model.Entitee;
import io.bigdime.adaptor.metadata.model.Metasegment;
import io.bigdime.common.testutils.GetterSetterTestHelper;

/**
 * Class MetasegmentTest
 * 
 * @author Neeraj Jain, psabinikari
 * 
 */
public class MetasegmentTest {

	Metasegment metasegment;

	@Mock
	Metasegment mockMetasegment;

	@Mock
	Entitee mockEntity;

	@BeforeClass
	public void init() {
		initMocks(this);
		metasegment = new Metasegment();
	}

	@Test
	public void testConstructor() {
		metasegment = new Metasegment("TestAdapterName", "testSchemaType",
				"testDatabaseName", "testDatabaseLocation",
				"testRepositoryType", "Y", "testDescription",
				new HashSet<Entitee>(), "testUser", "testUser");
		
		//metasegment = new MetasegmentDTO("TestAdapterName", "TestSchemaType", "TestDatabaseName", "TestDatabaseLocation", "TestRepositoryType", "Y", "TestDescription", new HashSet<EntiteeDTO>(), "TestUser", "TestUser");

		Assert.assertEquals(metasegment.getAdaptorName(), "TestAdapterName");
	}

	@Test
	public void testGettersAndSetters() {
		Field[] fields = FieldUtils.getAllFields(Metasegment.class);
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
		
		Set<Entitee> mockEntiteeSet = (Set<Entitee>) Mockito.mock(Set.class);
		metasegment.setEntitees(mockEntiteeSet);
		Iterator<Entitee> entiteeIterator = Mockito.mock(Iterator.class);
		when(mockEntiteeSet.iterator()).thenReturn(entiteeIterator);
		when(entiteeIterator.hasNext()).thenReturn(true, false);
		when(entiteeIterator.next()).thenReturn(mockEntity);
		when(mockEntity.getEntityName()).thenReturn("Test");
		Assert.assertEquals(mockEntity, metasegment.getEntity("Test"));
	}

	@Test
	public void testNullGetEntity() {
		Assert.assertNull(metasegment.getEntity("Test1"));
	}

}


