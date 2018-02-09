package org.apache.kafka.clients.enhance.consumer;

import org.apache.kafka.clients.enhance.AbsExtMessageFilter;
import org.apache.kafka.clients.enhance.consumer.listener.ConsumeMessageHook;
import org.apache.kafka.clients.enhance.consumer.listener.MessageHandler;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Created by steven03.zhang on 2017/12/13.
 */
public interface ConsumeOperator<K> {

    String groupId();

    String clientId();

    ConsumeModel consumeModel();

    ExtResetStrategy consumeResetStarategy();

    void subscribe(String topic);

    void subscribe(String topic, String filterPattern);

    void subscribe(String topic, AbsExtMessageFilter<K> filter);

    void subscribe(Collection<String> topics, String filterPattern);

    void subscribe(Collection<String> topics, AbsExtMessageFilter<K> filter);

    void unsubscribe();

    Set<String> subscription();

    void shutdownNow();

    void shutdown(long timeout, TimeUnit unit);

    void start();

    void seek(TopicPartition partition, long offset);

    void seekToTime(long timestamp);

    //format example: '2018-01-24T12:12:55.234'
    void seekToTime(String date);

    void seekToBeginning();

    void seekToEnd();

    void registerHandler(MessageHandler handler);

    void addConsumeHook(ConsumeMessageHook consumeHook);

    void suspend();

    void resume();

}
