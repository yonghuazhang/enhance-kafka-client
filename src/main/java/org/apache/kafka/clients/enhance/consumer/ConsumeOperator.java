package org.apache.kafka.clients.enhance.consumer;

import org.apache.kafka.clients.enhance.AbstractExtMessageFilter;
import org.apache.kafka.clients.enhance.ClientOperator;
import org.apache.kafka.clients.enhance.consumer.listener.ConsumeMessageHook;
import org.apache.kafka.clients.enhance.consumer.listener.MessageHandler;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.Set;

public interface ConsumeOperator<K> extends ClientOperator {

	String groupId();

	ConsumeGroupModel consumeModel();

	ExtResetStrategy consumeResetStarategy();

	void subscribe(String topic);

	void subscribe(String topic, String filterPattern);

	void subscribe(String topic, AbstractExtMessageFilter<K> filter);

	void subscribe(Collection<String> topics, String filterPattern);

	void subscribe(Collection<String> topics, AbstractExtMessageFilter<K> filter);

	void unsubscribe();

	Set<String> subscription();

	void seek(TopicPartition partition, long offset);

	void seekToTime(long timestamp);

	//format example: '2018-01-24T12:12:55.234'
	void seekToTime(String date);

	void seekToBeginning();

	void seekToEnd();

	void registerHandler(MessageHandler<K, ?> handler);

	void addConsumeHook(ConsumeMessageHook<K> consumeHook);

}
