package org.apache.kafka.clients.enhance.exception;

public class PartitionDataFullException extends Exception {
	final int fromIdx;

	public PartitionDataFullException(int fromIdx) {
		super();
		this.fromIdx = fromIdx;
	}

	public PartitionDataFullException(String message, int fromIdx) {
		super(message);
		this.fromIdx = fromIdx;
	}

	public PartitionDataFullException(String message) {
		super(message);
		this.fromIdx = -1;
	}

	public PartitionDataFullException(String message, Throwable cause, int fromIdx) {
		super(message, cause);
		this.fromIdx = fromIdx;
	}

	public PartitionDataFullException(Throwable cause, int fromIdx) {
		super(cause);
		this.fromIdx = fromIdx;
	}

	public int getFromIdx() {
		return fromIdx;
	}
}
