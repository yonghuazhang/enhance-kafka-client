package org.apache.kafka.clients.enhance.producer;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.enhance.ClientOperator;
import org.apache.kafka.clients.enhance.ExtMessage;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

public interface ProduceOperator<K> extends ClientOperator {

    void flush();

    List<PartitionInfo> partitionsForTopic(String topic);

    void beginTransaction();

    void commitTransaction();

    void abortTransaction();

    void sendGroupOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets, String groupId);

    Future<RecordMetadata> sendMessage(ExtMessage<K> message);

    Future<RecordMetadata> sendMessage(ExtMessage<K> message, Callback callback);

    void addSendMessageHook(SendMessageHook<K> sendHook);

}
