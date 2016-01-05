/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

import org.mockito.Mock;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import io.bigdime.core.ActionEvent.Status;

public class ActionEventTest {
	ActionEvent actionEvent = null;

	@Mock
	Map<String, String> mockHeaders;

	@BeforeMethod
	public void init() {
		actionEvent = new ActionEvent();// .getInstance();
	}

	/**
	 * @formatter:off
	 * Assert that getBody returns the same string that was set using setBody method.
	 * Assert that getHeaders returns the same object that was set using setHeaders method.
	 * @formatter:off
	 */
	@Test
	public void testConstructor() {
		actionEvent.setBody("unit-test-body".getBytes(Charset.defaultCharset()));
		Map<String, String> headers = new HashMap<>();
		actionEvent.setHeaders(headers);
		ActionEvent newActionEvent = new ActionEvent(actionEvent);
		Assert.assertEquals(newActionEvent.getHeaders(), headers);
		Assert.assertEquals(newActionEvent.getStatus(), Status.READY);
		Assert.assertEquals(newActionEvent.getBody(), "unit-test-body".getBytes(Charset.defaultCharset()));
	}
	@Test
	public void testToString() {
		ActionEvent newActionEvent = new ActionEvent(actionEvent);
		Assert.assertTrue(newActionEvent.toString().startsWith("[Event headers = "));
	}

	@Test
	public void testBodyGetterSetter() {
		byte[] mockByteArray = "unit-test-testGetBody".getBytes(Charset.defaultCharset());
		actionEvent.setBody(mockByteArray);
		Assert.assertEquals(actionEvent.getBody(), mockByteArray,
				"The value of body should be set in the object");
	}

	@Test
	public void testHeadersGetterSetter() {
		actionEvent.setHeaders(mockHeaders);
		Assert.assertEquals(actionEvent.getHeaders(), mockHeaders, "The value of headers should be set in the object");
	}

	@Test
	public void testStatusGetterSetter() {
		actionEvent.setStatus(Status.BACKOFF);
		Assert.assertEquals(actionEvent.getStatus(), Status.BACKOFF);
		actionEvent.setStatus(Status.READY);
		Assert.assertEquals(actionEvent.getStatus(), Status.READY);
	}

	@Test
	public void testNewBackoffEvent() {
		ActionEvent backoffEvent = ActionEvent.newBackoffEvent();
		Assert.assertEquals(backoffEvent.getStatus(), Status.BACKOFF);
	}

}
