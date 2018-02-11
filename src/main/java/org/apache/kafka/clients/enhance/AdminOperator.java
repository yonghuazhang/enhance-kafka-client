package org.apache.kafka.clients.enhance;

import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.enhance.exception.KafkaAdminException;
import org.apache.kafka.common.TopicPartition;

import java.util.List;
import java.util.Map;

public interface AdminOperator<K> {

    /**
     * Describe topic
     *
     * @param topic   topic name
     * @param timeout access timeout
     */
    TopicDescription describeTopic(final String topic, long timeout);

    /**
     * Describe cluster
     *
     * @param timeout access timeout
     */
    ClusterDescription describeCluster(long timeout);

    /**
     * check whether the topic is exists
     *
     * @param topic which topic will be checked
     */
    boolean isTopicExists(final String topic);


    /**
     * Create an topic
     *
     * @param newTopic   topic name
     * @param partitions topic's partition number
     * @param replicas   topic's replications
     */
    boolean createTopic(final String newTopic, final int partitions, final short replicas, final Map<String, String> props, long timeout);

    /**
     * Creates an topic
     *
     * @param newTopic   topic name
     * @param partitions topic's queue number
     */
    boolean createTopic(final String newTopic, final int partitions, final short replicas);

    /**
     * Creates an topic
     *
     * @param newTopic topic name
     */
    boolean createTopic(final String newTopic);

    /**
     * Gets the message queue offset according to some time in milliseconds<br>
     * be cautious to call because of more IO overhead
     *
     * @param tp        Instance of TopicPartition
     * @param timestamp from when in milliseconds.
     * @return offset
     */
    long searchOffset(final TopicPartition tp, final long timestamp);

    /**
     * Gets the max offset of the TopicPartition
     *
     * @param tp Instance of MessageQueue
     * @return the max offset
     */
    long beginningOffsets(final TopicPartition tp);

    /**
     * Gets the minimum offset of the TopicPartition
     *
     * @param tp Instance of MessageQueue
     * @return the minimum offset
     */
    long endOffsets(final TopicPartition tp);

    /**
     * Query message according to TopicPartition and offset
     *
     * @param tp     topic and partition
     * @param offset the position in topicPartion
     * @return ConsumerRecord
     * @throws KafkaAdminException
     */
    ExtMessage<K> viewMessage(final TopicPartition tp, final long offset) throws KafkaAdminException;


    /**
     * Query messages
     *
     * @param tp         topic and partition
     * @param size       max message number
     * @param bTimestamp from when
     * @return Instance of QueryResult
     * @throws KafkaAdminException
     */
    List<ExtMessage<K>> queryMessages(final TopicPartition tp, final long bTimestamp, final int size) throws KafkaAdminException;

}
