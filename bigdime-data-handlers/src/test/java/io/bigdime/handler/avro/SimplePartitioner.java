/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.handler.avro;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

public class SimplePartitioner implements Partitioner {
    public SimplePartitioner (VerifiableProperties props) {

    }

    public int partition(Object key, int a_numPartitions) {
        int partition = 0;
        return partition;
    }

}
