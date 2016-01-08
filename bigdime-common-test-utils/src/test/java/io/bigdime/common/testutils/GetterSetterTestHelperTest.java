/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.common.testutils;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.testng.annotations.Test;

public class GetterSetterTestHelperTest {

	private static class DummyClass {
		private String stringField;
		private int intField;
		private long longField;
		private List<String> stringList;

		public String getStringField() {
			return stringField;
		}

		public void setStringField(String stringField) {
			this.stringField = stringField;
		}

		public int getIntField() {
			return intField;
		}

		public void setIntField(int intField) {
			this.intField = intField;
		}

		public long getLongField() {
			return longField;
		}

		public void setLongField(long longField) {
			this.longField = longField;
		}

		public List<String> getStringList() {
			return stringList;
		}

		public void setStringList(List<String> stringList) {
			this.stringList = stringList;
		}
	}

	/**
	 * Assert that doTest method of GetterSetterTester works for String field by
	 * setting and getting a value on String field in DummyClass.
	 */
	@Test
	public void testDoTestWithStringField() {
		DummyClass object = new DummyClass();
		GetterSetterTestHelper.doTest(object, "stringField", "unit-test-stringField");
	}

	/**
	 * Assert that doTest method of GetterSetterTester works for int field by
	 * setting and getting a value on String field in DummyClass.
	 */
	@Test
	public void testDoTestWithIntField() {
		DummyClass object = new DummyClass();
		GetterSetterTestHelper.doTest(object, "intField", 1);
	}

	@Test
	public void testDoTestWithLongField() {
		DummyClass object = new DummyClass();
		GetterSetterTestHelper.doTest(object, "longField", new Random().nextLong());
	}

	@Test
	public void testDoTestWithObjectField() {
		DummyClass object = new DummyClass();
		GetterSetterTestHelper.doTest(object, "stringList", new ArrayList<String>());
	}

}
