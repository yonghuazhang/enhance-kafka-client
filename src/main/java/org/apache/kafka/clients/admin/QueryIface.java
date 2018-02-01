package org.apache.kafka.clients.admin;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.Deserializer;

/**
 * Created by steven03.zhang on 2017/8/28.
 */
public interface QueryIface {
    int FETCH_MESG_MAX_BYTES = 1 * 1024;
    /**
     * Get messages about the specified offset of the topicPartion.
     *
     * @param tp            The label of the topic and partition.
     * @param offset        The message's offset in the partition
     * @param kDeserializer The deserializer of message's key
     * @param vDeserializer The deserializer of message's value
     * @throws InterruptedException If the thread receives an InterruptedException before finishing.
     * @throws TimeoutException     If the thread times out before receiving a result.
     * @throws KafkaException       If there was an error getting the group information
     */

    <K, V> RetrieveRecordsResult<K, V> retrieveMessagesByOffset(final TopicPartition tp, long offset, int size,
                                                               final Deserializer<K> kDeserializer, final Deserializer<V> vDeserializer, final RetrieveRecordsOptions options);

    <K, V> RetrieveRecordsResult<K, V> retrieveMessagesByTimeSpan(final TopicPartition tp, long bTimestamp, int size, final Deserializer<K> kDeserializer,
                                                                  final Deserializer<V> vDeserializer, final RetrieveRecordsOptions options);

    /**
     *
     * @param tp The label of the topic and partition.
     * @param sTimestamp the time which begin to search from
     * @return FetchOffsetResult
     */
    FetchOffsetResult fetchPartitionOffsetByTime(final TopicPartition tp, final long sTimestamp, final FetchOffsetOptions options);
}
