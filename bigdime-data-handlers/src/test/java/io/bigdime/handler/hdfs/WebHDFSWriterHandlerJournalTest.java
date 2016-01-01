/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.handler.hdfs;

import java.util.List;
import java.util.Random;

import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import io.bigdime.common.testutils.GetterSetterTestHelper;
import io.bigdime.handler.webhdfs.WebHDFSWriterHandlerJournal;

public class WebHDFSWriterHandlerJournalTest {

	@Test
	public void testGetterSetter() {
		Random rand = new Random();
		WebHDFSWriterHandlerJournal journal = new WebHDFSWriterHandlerJournal();
		GetterSetterTestHelper.doTest(journal, "recordCount", rand.nextInt());
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testReset() {
		Random rand = new Random();
		WebHDFSWriterHandlerJournal journal = new WebHDFSWriterHandlerJournal();
		journal.setRecordCount(rand.nextInt());
		journal.setEventList(Mockito.mock(List.class));
		journal.setCurrentHdfsPath("unit-testReset");
		journal.reset();
		Assert.assertEquals(journal.getRecordCount(), 0);
		Assert.assertNull(journal.getEventList());
		Assert.assertEquals(journal.getCurrentHdfsPath(), "");
	}
}
