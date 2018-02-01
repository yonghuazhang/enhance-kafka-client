package org.apache.kafka.clients.enhance.consumer;

/**
 * Created by steven03.zhang on 2017/12/20.
 */
public enum LoadOffsetType {
    LOAD_FROM_BROKER,
    LOAD_FROM_LOCAL_FILE,
    LOAD_BROKER_THEN_FILE
}
