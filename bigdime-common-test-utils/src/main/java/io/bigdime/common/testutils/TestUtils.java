/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.common.testutils;

import java.io.IOException;
import java.net.ServerSocket;

/**
 * 
 * @author mnamburi
 *
 */
public final class TestUtils {
	private TestUtils (){}
	/** 
	 *  Creates a server socket, bound to the specified port.
	 *  A port number of 0 means that the port number is automatically allocated, typically from an ephemeral port range.
	 *  This port number can then be retrieved by calling getLocalPort.
	 * 
	 * @return
	 * @throws IOException
	 */
	public static int findAvailablePort(int port) throws IOException {
		try (ServerSocket s = new ServerSocket(port)){
			return s.getLocalPort();			
		}  catch (IOException e) {
			throw e;
		}
	}
}
