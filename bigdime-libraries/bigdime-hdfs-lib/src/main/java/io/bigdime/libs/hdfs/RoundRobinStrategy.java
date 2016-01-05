/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.libs.hdfs;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 
 * @author mnamburi
 *
 */
public class RoundRobinStrategy {
	private static RoundRobinStrategy instance = null;
	List<String> hostList = null;
	private final static String COMMA = ",";
	AtomicInteger atomicIndex = new AtomicInteger(0);

	protected RoundRobinStrategy() {
	}

	public synchronized static RoundRobinStrategy getInstance() {
		if (instance == null) {
			instance = new RoundRobinStrategy();
		}
		return instance;
	}

	public void setHosts(String hosts) {
		if (hosts == null || hosts.trim().isEmpty())
			throw new IllegalArgumentException("hosts can't be null or empty");
		hostList = decodeHostList(hosts);
	}

	/**
	 * Keeps track of the last index over multiple dispatches. Each invocation
	 * of this method will increment the index by one, overflowing at
	 * <code>size</code>.
	 */
	public synchronized String getNextServiceHost() {
		int size = hostList.size();
		int index = 0;
		int indexTail = atomicIndex.getAndIncrement() % size;
		index = indexTail < 0 ? indexTail + size : indexTail;
		return hostList.get(index);
	}

	private List<String> decodeHostList(String str) {
		List<String> values = new ArrayList<String>();
		for (String host : str.split(COMMA)) {
			values.add(host.trim());
		}

		// if (values.size() == 0) {
		// values.add(str);
		// }
		return values;
	}
}
