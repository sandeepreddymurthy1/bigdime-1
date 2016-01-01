/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core;

import java.util.Observer;

import org.apache.flume.NamedComponent;
import org.apache.flume.lifecycle.LifecycleAware;

/**
 * Source is meant to read the data from the external data source and write it
 * to outputChannel after processing. It executes a chain of handlers to perform
 * the tasks. It does NOT extend the {@link org.apache.flume.Source} interface
 * since it doesn't deal directly with the DataChannel, instead it lets one of
 * the handlers deal with the channels.
 *
 * // TODO Revisit on how to implement the Observer pattern here, we might need
 * a MQ or a distributed cache based system for notification
 * 
 * @author Neeraj Jain
 *
 */
public interface Source extends NamedComponent, HasHandlers, LifecycleAware, Observer {

}
