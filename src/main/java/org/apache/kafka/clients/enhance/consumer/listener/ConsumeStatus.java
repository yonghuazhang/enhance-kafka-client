package org.apache.kafka.clients.enhance.consumer.listener;

public enum ConsumeStatus {
    CONSUME_SUCCESS,
    CONSUME_RETRY_LATER,
    SUSPEND_FOR_PARTITION,
    CONSUME_DISCARD
}