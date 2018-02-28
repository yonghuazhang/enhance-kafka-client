package org.apache.kafka.clients.enhance.consumer.listener;

import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.internals.ConsumerInterceptors;
import org.apache.kafka.clients.enhance.ExtMessage;

import java.util.ArrayList;
import java.util.List;

public class ConsumeMessageHooks<K> extends ConsumerInterceptors<K, ExtMessage<K>> {

	public ConsumeMessageHooks() {
		super(new ArrayList<ConsumerInterceptor<K, ExtMessage<K>>>());
	}

	public void addConsumeMessageHook(final ConsumerInterceptor<K, ExtMessage<K>> interceptor) {
		if (!interceptors.contains(interceptor)) {
			interceptors.add(interceptor);
		}
	}

	public void addConsumeMessageHook(final List<ConsumerInterceptor<K, ExtMessage<K>>> interceptors) {
		for (ConsumerInterceptor<K, ExtMessage<K>> ci : interceptors) {
			addConsumeMessageHook(ci);
		}
	}

	public void clearHooks() {
		this.interceptors.clear();
	}

	public boolean isEmpty() {
		return this.interceptors.isEmpty();
	}

}
