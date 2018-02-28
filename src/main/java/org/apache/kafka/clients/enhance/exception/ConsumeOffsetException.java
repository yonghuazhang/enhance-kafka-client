package org.apache.kafka.clients.enhance.exception;

import org.apache.kafka.common.errors.ApiException;

public class ConsumeOffsetException extends ApiException {
	public ConsumeOffsetException(String message, Throwable cause) {
		super(message, cause);
	}

	public ConsumeOffsetException(String message) {
		super(message);
	}

	public ConsumeOffsetException(Throwable cause) {
		super(cause);
	}

	public ConsumeOffsetException() {
		super();
	}
}
