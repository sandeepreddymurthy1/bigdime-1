/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core;

import org.apache.flume.NamedComponent;
import org.apache.flume.lifecycle.LifecycleAware;

/**
 * Sink is meant to read data from channel and write to disk. It executes a
 * chain of handlers to perform the tasks. It does NOT extend the
 * {@link org.apache.flume.Sink} interface since it doesn't deal directly with
 * the DataChannel, instead it lets one of the handlers deal with the channels.
 *
 * @author Neeraj Jain
 *
 */
public interface Sink extends NamedComponent, HasHandlers, LifecycleAware {
}
