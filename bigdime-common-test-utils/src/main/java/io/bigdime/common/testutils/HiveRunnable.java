/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.common.testutils;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveRunnable implements Runnable {
	private Logger logger = LoggerFactory.getLogger(HiveRunnable.class); 
	private final String msPort;
	private List<String> args = new ArrayList<String>();

	public HiveRunnable(String msPort) {
		this.msPort = msPort;
		this.args.add("-v");
		this.args.add("-p");
		this.args.add(this.msPort);
	}

	public HiveRunnable arg(String arg) {
		this.args.add(arg);
		return this;
	}

	@Override
	public void run() {
		try {
			HiveMetaStore.main(args.toArray(new String[args.size()]));
		} catch (Throwable e) {
			logger.error(e.getLocalizedMessage());
		}
	}

}
