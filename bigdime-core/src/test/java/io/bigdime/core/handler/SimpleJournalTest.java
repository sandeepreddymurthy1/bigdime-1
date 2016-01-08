/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core.handler;

import java.util.ArrayList;
import java.util.Random;

import org.testng.Assert;
import org.testng.annotations.Test;

import io.bigdime.common.testutils.GetterSetterTestHelper;
import io.bigdime.core.ActionEvent;

public class SimpleJournalTest {

	@Test
	public void testGettersAndSetters() {

		HandlerJournal simpleJournal = new SimpleJournal();
		Random rand = new Random();
		GetterSetterTestHelper.doTest(simpleJournal, "totalRead", rand.nextLong());
		GetterSetterTestHelper.doTest(simpleJournal, "totalSize", rand.nextLong());
		GetterSetterTestHelper.doTest(simpleJournal, "eventList", new ArrayList<ActionEvent>());
	}

	@Test
	public void testReset() {
		SimpleJournal simpleJournal = new SimpleJournal();
		Random rand = new Random();
		simpleJournal.setTotalRead(rand.nextInt());
		simpleJournal.setEventList(new ArrayList<ActionEvent>());
		simpleJournal.setTotalSize(rand.nextInt());
		simpleJournal.reset();
		Assert.assertEquals(simpleJournal.getTotalRead(), 0);
		Assert.assertNull(simpleJournal.getEventList());
		Assert.assertEquals(simpleJournal.getTotalSize(), 0);
	}

	@Test
	public void testToString() {
		SimpleJournal simpleJournal = new SimpleJournal();
		Random rand = new Random();
		simpleJournal.setTotalRead(rand.nextInt());
		simpleJournal.setEventList(new ArrayList<ActionEvent>());
		simpleJournal.setTotalSize(rand.nextInt());
		Assert.assertTrue(simpleJournal.toString().startsWith("SimpleJournal [totalRead"));

	}

}
