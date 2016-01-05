/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.handler.file;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

import io.bigdime.common.testutils.GetterSetterTestHelper;

public class FileInputDescriptorTest {

	@Test
	public void testGettersAndSetters() {
		FileInputDescriptor fileInputDescriptor = new FileInputDescriptor();
		GetterSetterTestHelper.doTest(fileInputDescriptor, "path", "unit-basePath-testGettersAndSetters");
		GetterSetterTestHelper.doTest(fileInputDescriptor, "fileName", "unit-fileName-testGettersAndSetters");
	}

	@Test
	public void testGetNext() {
		FileInputDescriptor fileInputDescriptor = new FileInputDescriptor();
		List<String> availableInputs = new ArrayList<>();
		availableInputs.add("/dir1/dir2/file1");
		availableInputs.add("/dir1/dir2/file2");
		availableInputs.add("/dir1/dir2/file3");
		String lastInput = "/dir1/dir2/file2";
		String nextDescriptor = fileInputDescriptor.getNext(availableInputs, lastInput);
		Assert.assertEquals(nextDescriptor, "/dir1/dir2/file3");
	}

	/**
	 * Assert that if the available list is null, an IllegalArgumentException is
	 * thrown. This actually indicates that thre was a problem during building
	 * thr handler.
	 */
	@Test(expectedExceptions = IllegalArgumentException.class)
	public void testGetNextWithNullAvailableInputs() {
		FileInputDescriptor fileInputDescriptor = new FileInputDescriptor();
		String lastInput = "/dir1/dir2/file2";
		fileInputDescriptor.getNext(getNullStringList(), lastInput);
	}

	private List<String> getNullStringList() {
		return null;
	}

	/**
	 * Assert that if the last input received is null, an
	 * IllegalArgumentException is thrown. This indicates that RuntimeInfo
	 */
	@Test(expectedExceptions = IllegalArgumentException.class)
	public void testGetNextWithNullLastInput() {
		FileInputDescriptor fileInputDescriptor = new FileInputDescriptor();
		List<String> availableInputs = new ArrayList<>();
		availableInputs.add("/dir1/dir2/file1");
		availableInputs.add("/dir1/dir2/file2");
		availableInputs.add("/dir1/dir2/file3");
		fileInputDescriptor.getNext(availableInputs, getNull());
	}

	private String getNull() {
		return null;
	}

	/**
	 * Assert that if the last input received is same as last entry on
	 * availableInputs, a null is received.
	 */
	@Test
	public void testGetNextNoNextInputDescriptor() {
		FileInputDescriptor fileInputDescriptor = new FileInputDescriptor();
		List<String> availableInputs = new ArrayList<>();
		availableInputs.add("/dir1/dir2/file1");
		availableInputs.add("/dir1/dir2/file2");
		availableInputs.add("/dir1/dir2/file3");
		String lastInput = "/dir1/dir2/file3";
		Assert.assertNull(fileInputDescriptor.getNext(availableInputs, lastInput));
	}

	@Test
	public void testParseDescriptor() {
		FileInputDescriptor fileInputDescriptor = new FileInputDescriptor();
		fileInputDescriptor.parseDescriptor(convertToSystemPath("dir1/dir2/dir3/fname1"));
		Assert.assertEquals(fileInputDescriptor.getPath(), convertToSystemPath("dir1/dir2/dir3/"));
		Assert.assertEquals(fileInputDescriptor.getFileName(), "fname1");
	}

	@Test
	public void testParseDescriptorWithEntityNameAndBasePath() {
		FileInputDescriptor fileInputDescriptor = new FileInputDescriptor();
		fileInputDescriptor.parseDescriptor("/tmp/");
		Assert.assertEquals(fileInputDescriptor.getPath(), "/tmp/");
	}

	@Test
	public void testParseSourceDescriptorWithEntityNameAndBasePath() {
		FileInputDescriptor fileInputDescriptor = new FileInputDescriptor();
		String[] descs = fileInputDescriptor.parseSourceDescriptor("us_biz_users:file-prefix.*");
		Assert.assertEquals(descs[0], "us_biz_users");
		Assert.assertEquals(descs[1], "file-prefix.*");
	}

	@Test(expectedExceptions = IllegalArgumentException.class)
	public void testParseSourceDescriptorWithNoEntityName() {
		FileInputDescriptor fileInputDescriptor = new FileInputDescriptor();
		fileInputDescriptor.parseSourceDescriptor("part1");
	}

	@Test(expectedExceptions = IllegalArgumentException.class)
	public void testParseSourceDescriptorWithMoreThan2Colons() {
		FileInputDescriptor fileInputDescriptor = new FileInputDescriptor();
		fileInputDescriptor.parseSourceDescriptor("part1:part2:part3");
	}

	@Test
	public void testParseDescriptorWithEntityName() {
		String tempDir = System.getProperty("java.io.tmpdir");
		FileInputDescriptor fileInputDescriptor = new FileInputDescriptor();
		fileInputDescriptor.parseDescriptor(tempDir);
		if (!tempDir.endsWith(File.separator))
			tempDir += File.separator;
		Assert.assertEquals(fileInputDescriptor.getPath(), tempDir);
	}

	@Test
	public void testParseDescriptorWithFileInRootDirectory() {
		FileInputDescriptor fileInputDescriptor = new FileInputDescriptor();
		fileInputDescriptor.parseDescriptor(convertToSystemPath("/fname1"));
		Assert.assertEquals(fileInputDescriptor.getPath(), "/");
		Assert.assertEquals(fileInputDescriptor.getFileName(), "fname1");
	}

	/**
	 * Assert that parsing null throws a NullPointerException.
	 */
	@Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "descriptor can't be null or empty")
	public void testParseDescriptorWithNullDescriptor() {
		FileInputDescriptor fileInputDescriptor = new FileInputDescriptor();
		fileInputDescriptor.parseDescriptor(null);
	}

	/**
	 * Assert that parsing null throws a NullPointerException.
	 */
	@Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "descriptor can't be null or empty")
	public void testParseDescriptorWithEmptyDescriptor() {
		FileInputDescriptor fileInputDescriptor = new FileInputDescriptor();
		fileInputDescriptor.parseDescriptor("");
	}

	// @Test
	// public void testParseDescriptorWithInvalidDescriptor() {
	// FileInputDescriptor fileInputDescriptor = new FileInputDescriptor();
	// fileInputDescriptor.parseDescriptor("testParseDescriptorWithInvalidDescriptor");
	// }

	@Test
	public void testToString() {
		FileInputDescriptor fileInputDescriptor = new FileInputDescriptor();
		fileInputDescriptor.setPath("unit-setBasePath-testToString");
		fileInputDescriptor.setFileName("unit-setFileName-testToString");
		String stringDescriptor = fileInputDescriptor.toString();
		Assert.assertEquals(stringDescriptor, "unit-setBasePath-testToString/unit-setFileName-testToString");
	}

	@Test
	public void testGetterSetter() {
		FileInputDescriptor fileInputDescriptor = new FileInputDescriptor();
		GetterSetterTestHelper.doTest(fileInputDescriptor, "entityName", "unit-entityName");
	}

	private String convertToSystemPath(String input) {
		return input.replace("/", File.separator);
	}

}
