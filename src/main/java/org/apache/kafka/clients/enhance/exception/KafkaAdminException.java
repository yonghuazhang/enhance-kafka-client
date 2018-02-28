package org.apache.kafka.clients.enhance.exception;

import org.apache.kafka.common.KafkaException;

public class KafkaAdminException extends KafkaException {
	private static final long serialVersionUID = 1L;

	public KafkaAdminException(String message, Throwable cause) {
		super(message, cause);
	}

	public KafkaAdminException(String message) {
		super(message);
	}

	public KafkaAdminException(Throwable cause) {
		super(cause);
	}

	public KafkaAdminException() {
		super();
	}

	/* avoid the expensive and useless stack trace for api exceptions */
	@Override
	public Throwable fillInStackTrace() {
		return this;
	}
}
