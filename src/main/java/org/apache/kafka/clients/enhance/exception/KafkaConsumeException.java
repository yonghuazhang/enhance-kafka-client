package org.apache.kafka.clients.enhance.exception;

import org.apache.kafka.common.KafkaException;

public class KafkaConsumeException extends KafkaException {
    public KafkaConsumeException(String message, Throwable cause) {
        super(message, cause);
    }

    public KafkaConsumeException(String message) {
        super(message);
    }

    public KafkaConsumeException(Throwable cause) {
        super(cause);
    }

    public KafkaConsumeException() {
        super();
    }
}
