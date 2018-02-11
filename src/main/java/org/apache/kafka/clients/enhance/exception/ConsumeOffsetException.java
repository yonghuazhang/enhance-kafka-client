package org.apache.kafka.clients.enhance.exception;

import org.apache.kafka.common.errors.ApiException;

/**
 * Created by steven03.zhang on 2018/1/13.
 */
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
