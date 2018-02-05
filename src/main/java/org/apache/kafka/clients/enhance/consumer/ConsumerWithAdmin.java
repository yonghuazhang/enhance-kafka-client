package org.apache.kafka.clients.enhance.consumer;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.RetrieveRecordsOptions;
import org.apache.kafka.clients.admin.RetrieveRecordsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.enhance.AdminOperator;
import org.apache.kafka.clients.enhance.ClusterDescription;
import org.apache.kafka.clients.enhance.ExtMessage;
import org.apache.kafka.clients.enhance.ExtMessageEncoder;
import org.apache.kafka.clients.enhance.ExtMessageUtils;
import org.apache.kafka.clients.enhance.exception.KafkaAdminException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.requests.IsolationLevel;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;

import static org.apache.kafka.clients.enhance.ExtMessageDef.INVALID_OFFSET_VALUE;

/**
 * ConsumerWithAdmin include admin client and thread-safe
 */
class ConsumerWithAdmin<K> extends KafkaConsumer<K, ExtMessage<K>> implements AdminOperator<K> {
    private static final Logger log = LoggerFactory.getLogger(ConsumerWithAdmin.class);
    private AdminClient adminClient;
    private final ReentrantLock adminLock = new ReentrantLock();
    private final ReentrantLock consumerLock = new ReentrantLock();

    public ConsumerWithAdmin(Map<String, Object> configs) {
        this(configs, null);
    }

    public ConsumerWithAdmin(Map<String, Object> configs, Deserializer<K> keyDeserializer) {
        super(configs, keyDeserializer, new ExtMessageEncoder<K>());
        initAdminClient();
    }

    public ConsumerWithAdmin(Properties properties) {
        this(properties, null);
    }

    public ConsumerWithAdmin(Properties properties, Deserializer<K> keyDeserializer) {
        super(properties, keyDeserializer, new ExtMessageEncoder<K>());
        initAdminClient();
    }

    private void initAdminClient() {
        adminClient = KafkaAdminClient.create(this.getConsumerConfig().originals());
    }

    @Override
    public Set<TopicPartition> assignment() {
        consumerLock.lock();
        try {
            return super.assignment();
        } finally {
            consumerLock.unlock();
        }
    }

    @Override
    public Set<String> subscription() {
        consumerLock.lock();
        try {
            return super.subscription();
        } finally {
            consumerLock.unlock();
        }
    }

    @Override
    public void subscribe(Collection<String> topics, ConsumerRebalanceListener listener) {
        consumerLock.lock();
        try {
            super.subscribe(topics, listener);
        } finally {
            consumerLock.unlock();
        }
    }

    @Override
    public void subscribe(Collection<String> topics) {
        consumerLock.lock();
        try {
            super.subscribe(topics);
        } finally {
            consumerLock.unlock();
        }
    }

    @Override
    public void subscribe(Pattern pattern, ConsumerRebalanceListener listener) {
        consumerLock.lock();
        try {
            super.subscribe(pattern, listener);
        } finally {
            consumerLock.unlock();
        }
    }

    @Override
    public void subscribe(Pattern pattern) {
        consumerLock.lock();
        try {
            super.subscribe(pattern);
        } finally {
            consumerLock.unlock();
        }
    }

    @Override
    public void unsubscribe() {
        consumerLock.lock();
        try {
            super.unsubscribe();
        } finally {
            consumerLock.unlock();
        }
    }

    @Override
    public void assign(Collection<TopicPartition> partitions) {
        consumerLock.lock();
        try {
            super.assign(partitions);
        } finally {
            consumerLock.unlock();
        }
    }

    public ConsumerConfig getConsumerConfig() {
        return this.config;
    }

    public String clientId() {
        return this.clientId;
    }

    public String groupId() {
        return this.config.getString(ConsumerConfig.GROUP_ID_CONFIG);
    }

    public long getRequestTimoutMs() {
        return this.requestTimeoutMs;
    }

    public long getRetryBackoffMs() {
        return this.retryBackoffMs;
    }

    @Override
    public TopicDescription describeTopic(String topic, long timeout) {
        adminLock.lock();
        try {
            DescribeTopicsResult result = adminClient.describeTopics(Arrays.asList(topic));
            return result.all().get(timeout, TimeUnit.MILLISECONDS).get(topic);
        } catch (Exception ex) {
            log.warn("describeTopic topic [{}] failed, caused by [{}].", topic, ex);
            return null;
        } finally {
            adminLock.unlock();
        }
    }

    @Override
    public ClusterDescription describeCluster(long timeout) {
        adminLock.lock();
        try {
            DescribeClusterResult result = adminClient.describeCluster();
            Collection<Node> nodes = result.nodes().get(timeout, TimeUnit.MILLISECONDS);
            String clusterId = result.clusterId().get(timeout, TimeUnit.MILLISECONDS);
            Node controller = result.controller().get(timeout, TimeUnit.MILLISECONDS);
            return new ClusterDescription(nodes, controller, clusterId);
        } catch (Exception ex) {
            log.warn("describe cluster failed, caused by [{}].", ex);
            return null;
        } finally {
            adminLock.unlock();
        }
    }

    @Override
    public boolean isTopicExists(String topic) {
        TopicDescription topicInfo = describeTopic(topic, 3000L);
        if (null != topicInfo && topicInfo.name().equals(topic)){
            return true;
        }
        return false;
    }

    @Override
    public boolean createTopic(String newTopic, int partitions, short replicas, final Map<String, String> configs, long timeoutInMs) {
        adminLock.lock();
        try {
            NewTopic topic = new NewTopic(newTopic, partitions, replicas);
            topic.configs(configs);
            CreateTopicsResult result = adminClient.createTopics(Arrays.asList(topic));
            if (timeoutInMs > 0) {
                result.all().get(timeoutInMs, TimeUnit.MILLISECONDS);
            } else {
                result.all().get();
            }
            return true;
        } catch (Exception ex) {
            log.error("createTopic for topic [{}] failed, caused by [{}].", newTopic, ex);
            return false;
        } finally {
            adminLock.unlock();
        }
    }

    @Override
    public boolean createTopic(String newTopic, int partitions, short replicas) {
        return this.createTopic(newTopic, partitions, replicas, null, this.requestTimeoutMs);
    }

    @Override
    public boolean createTopic(String newTopic) {
        adminLock.lock();
        try {
            DescribeClusterResult result = adminClient.describeCluster();
            int bNum = result.nodes().get().size();
            short repNum = (short) ((bNum > 3) ? 3 : bNum);
            return this.createTopic(newTopic, bNum << 1L, repNum);
        } catch (Exception ex) {
            log.error("createTopic for topic [{}] failed, caused by [{}].", newTopic, ex);
            return false;
        } finally {
            adminLock.unlock();
        }
    }

    @Override
    public long searchOffset(TopicPartition tp, long timestamp) {
        if (null == tp) return INVALID_OFFSET_VALUE;

        adminLock.lock();
        try {
            Map<TopicPartition, Long> searchRequest = new HashMap<>(1);
            searchRequest.put(tp, timestamp);
            OffsetAndTimestamp offsetAndTimestamp = this.offsetsForTimes(searchRequest).get(tp);
            return (null == offsetAndTimestamp) ? INVALID_OFFSET_VALUE : offsetAndTimestamp.offset();
        } catch (Exception ex) {
            log.error("searchOffset for TopicPartition [{}] failed, caused by [{}].", tp, ex);
            throw new KafkaAdminException(ex);
        } finally {
            adminLock.unlock();
        }
    }

    @Override
    public long beginningOffsets(TopicPartition tp) throws KafkaAdminException {
        if (null == tp) return INVALID_OFFSET_VALUE;
        Long searchResult;

        adminLock.lock();
        try {
            searchResult = this.beginningOffsets(Arrays.asList(tp)).get(tp);
        } catch (Exception e) {
            log.error("beginningOffsets for TopicPartition [{}] failed, caused by [{}].", tp, e);
            throw new KafkaAdminException(e);
        } finally {
            adminLock.unlock();
        }
        return (null == searchResult) ? INVALID_OFFSET_VALUE : searchResult.longValue();
    }

    @Override
    public long endOffsets(TopicPartition tp) throws KafkaAdminException {
        if (null == tp) return INVALID_OFFSET_VALUE;
        Long searchResult;

        adminLock.lock();
        try {
            searchResult = this.endOffsets(Arrays.asList(tp)).get(tp);
        } catch (Exception e) {
            log.error("endOffsets for TopicPartition [{}] failed, caused by [{}].", tp, e);
            throw new KafkaAdminException(e);
        } finally {
            adminLock.unlock();
        }
        return (null == searchResult) ? INVALID_OFFSET_VALUE : searchResult.longValue();
    }

    @Override
    public ExtMessage<K> viewMessage(TopicPartition tp, long offset) throws KafkaAdminException {
        ExtMessage<K> message = null;
        adminLock.lock();
        try {
            RetrieveRecordsOptions options = new RetrieveRecordsOptions();
            options.timeoutMs((int) this.requestTimeoutMs);
            RetrieveRecordsResult<K, ExtMessage<K>> result = adminClient.retrieveMessagesByOffset(tp, offset, 1, this.keyDeserializer, this.valueDeserializer, options);
            for (ConsumerRecord<K, ExtMessage<K>> record : result.values().get()) {
                message = record.value();
                ExtMessageUtils.updateByRecord(record.value(), record);
            }

        } catch (Exception e) {
            log.error("viewMessage for TopicPartition [{}] failed, caused by [{}].", tp, e);
            throw new KafkaAdminException(e);
        } finally {
            adminLock.unlock();
        }
        return message;
    }

    @Override
    public List<ExtMessage<K>> queryMessages(TopicPartition tp, long bTimestamp, int size) throws KafkaAdminException {
        List<ExtMessage<K>> messages = new ArrayList<>();
        adminLock.lock();
        try {
            RetrieveRecordsOptions options = new RetrieveRecordsOptions();
            options.timeoutMs((int) this.requestTimeoutMs);
            RetrieveRecordsResult<K, ExtMessage<K>> result = adminClient.retrieveMessagesByTimeSpan(tp, bTimestamp, size, this.keyDeserializer, this.valueDeserializer, options);
            for (ConsumerRecord<K, ExtMessage<K>> record : result.values().get()) {
                messages.add(ExtMessageUtils.updateByRecord(record.value(), record));
            }
        } catch (Exception e) {
            log.error("queryMessages for TopicPartition [{}] failed, caused by [{}].", tp, e);
            throw new KafkaAdminException(e);
        } finally {
            adminLock.unlock();
        }
        return messages;
    }

    @Override
    public ConsumerRecords<K, ExtMessage<K>> poll(long timeout) {
        ConsumerRecords<K, ExtMessage<K>> records = null;
        try {
            if (consumerLock.tryLock(timeout, TimeUnit.MILLISECONDS)) {
                try {
                    records = super.poll(timeout);
                    for (ConsumerRecord<K, ExtMessage<K>> record : records) {
                        ExtMessageUtils.updateByRecord(record.value(), record);
                    }
                } finally {
                    consumerLock.unlock();
                }
            }
        } catch (InterruptedException e) {
            log.warn("poll message service is interrupted.");
        }
        if (null == records) {
            records = ConsumerRecords.empty();
        }
        return records;
    }

    @Override
    public void commitSync() {
        consumerLock.lock();
        try {
            super.commitSync();
        } finally {
            consumerLock.unlock();
        }
    }

    @Override
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
        consumerLock.lock();
        try {
            super.commitSync(offsets);
        } finally {
            consumerLock.unlock();
        }
    }

    @Override
    public void commitAsync() {
        consumerLock.lock();
        try {
            super.commitAsync();
        } finally {
            consumerLock.unlock();
        }
    }

    @Override
    public void commitAsync(OffsetCommitCallback callback) {
        consumerLock.lock();
        try {
            super.commitAsync(callback);
        } finally {
            consumerLock.unlock();
        }
    }

    @Override
    public void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
        consumerLock.lock();
        try {
            super.commitAsync(offsets, callback);
        } finally {
            consumerLock.unlock();
        }
    }

    @Override
    public void seek(TopicPartition partition, long offset) {
        consumerLock.lock();
        try {
            super.seek(partition, offset);
        } finally {
            consumerLock.unlock();
        }
    }

    @Override
    public void seekToBeginning(Collection<TopicPartition> partitions) {
        consumerLock.lock();
        try {
            super.seekToBeginning(partitions);
        } finally {
            consumerLock.unlock();
        }
    }

    @Override
    public void seekToEnd(Collection<TopicPartition> partitions) {
        consumerLock.lock();
        try {
            super.seekToEnd(partitions);
        } finally {
            consumerLock.unlock();
        }
    }

    @Override
    public long position(TopicPartition partition) {
        consumerLock.lock();
        try {
            return super.position(partition);
        } finally {
            consumerLock.unlock();
        }
    }

    @Override
    public OffsetAndMetadata committed(TopicPartition partition) {
        consumerLock.lock();
        try {
            return super.committed(partition);
        } finally {
            consumerLock.unlock();
        }
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        consumerLock.lock();
        try {
            return super.partitionsFor(topic);
        } finally {
            consumerLock.unlock();
        }
    }

    @Override
    public Map<String, List<PartitionInfo>> listTopics() {
        consumerLock.lock();
        try {
            return super.listTopics();
        } finally {
            consumerLock.unlock();
        }
    }

    @Override
    public void pause(Collection<TopicPartition> partitions) {
        consumerLock.lock();
        try {
            super.pause(partitions);
        } finally {
            consumerLock.unlock();
        }
    }

    @Override
    public void resume(Collection<TopicPartition> partitions) {
        consumerLock.lock();
        try {
            super.resume(partitions);
        } finally {
            consumerLock.unlock();
        }
    }

    @Override
    public Set<TopicPartition> paused() {
        consumerLock.lock();
        try {
            return super.paused();
        } finally {
            consumerLock.unlock();
        }
    }

    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch) {
        consumerLock.lock();
        try {
            return super.offsetsForTimes(timestampsToSearch);
        } finally {
            consumerLock.unlock();
        }
    }

    @Override
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions) {
        consumerLock.lock();
        try {
            return super.beginningOffsets(partitions);
        } finally {
            consumerLock.unlock();
        }
    }

    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {
        consumerLock.lock();
        try {
            return super.endOffsets(partitions);
        } finally {
            consumerLock.unlock();
        }
    }

    @Override
    public void close() {
        consumerLock.lock();
        try {
            super.close();
        } finally {
            consumerLock.unlock();
        }
    }

    @Override
    public void close(long timeout, TimeUnit timeUnit) {
        consumerLock.lock();
        try {
            super.close(timeout, timeUnit);
        } finally {
            consumerLock.unlock();
        }
    }

    public long getPartitionLag(TopicPartition tp, boolean committed) {
        if (committed) {
            return this.subscriptions.partitionLag(tp, IsolationLevel.READ_COMMITTED);
        } else
            return this.subscriptions.partitionLag(tp, IsolationLevel.READ_UNCOMMITTED);
    }

}
