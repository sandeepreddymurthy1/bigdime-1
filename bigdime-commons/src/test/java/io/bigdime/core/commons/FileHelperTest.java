/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core.commons;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

public class FileHelperTest {

	@Test
	public void testGetAvailableFiles() {
		try {
			String tempDir = System.getProperty("java.io.tmpdir");
			FileHelper fileHelper = FileHelper.getInstance();
			File[] tempFiles = new File[10];
			File newDir = new File(tempDir + "testGetAvailableFiles");
			boolean createdNewDir = newDir.mkdir();
			if (!createdNewDir)
				newDir = new File(tempDir);

			for (int i = 0; i < 5; i++)
				tempFiles[i] = File.createTempFile("unit", ".txt");
			for (int i = 5; i < 10; i++)
				tempFiles[i] = File.createTempFile("unit", ".txt", newDir);
			List<String> fileNames = fileHelper.getAvailableFiles(tempDir, ".*");
			Assert.assertTrue(fileNames.size() >= 10);
			for (int i = 0; i < 10; i++)
				tempFiles[i].delete();
			newDir.delete();
		} catch (IOException e) {
			e.printStackTrace();
			Assert.fail("failed", e);
		}
	}

	@Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Parameter 'directory' is not a directory.*")
	public void testGetAvailableFiles1() throws IOException {
		FileHelper fileHelper = FileHelper.getInstance();
		fileHelper.getAvailableFiles("/tmpunit/", ".*");
	}
}
