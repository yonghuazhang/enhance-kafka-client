package org.apache.kafka.clients.enhance.exception;

import org.apache.kafka.common.KafkaException;

public class IllegalPropertyException extends KafkaException {
	public IllegalPropertyException(String message, Throwable cause) {
		super(message, cause);
	}

	public IllegalPropertyException(String message) {
		super(message);
	}

	public IllegalPropertyException(Throwable cause) {
		super(cause);
	}

	public IllegalPropertyException() {
		super();
	}
}
