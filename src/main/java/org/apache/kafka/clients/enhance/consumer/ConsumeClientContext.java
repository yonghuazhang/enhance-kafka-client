package org.apache.kafka.clients.enhance.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.enhance.AbsExtMessageFilter;
import org.apache.kafka.clients.enhance.Utility;
import org.apache.kafka.clients.enhance.consumer.listener.ConcurrentMessageHandler;
import org.apache.kafka.clients.enhance.consumer.listener.ConsumeMessageHook;
import org.apache.kafka.clients.enhance.consumer.listener.ConsumeMessageHooks;
import org.apache.kafka.clients.enhance.consumer.listener.MessageHandler;
import org.apache.kafka.clients.enhance.consumer.listener.OrdinalMessageHandler;
import org.apache.kafka.clients.enhance.exception.KafkaConsumeException;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.clients.enhance.ExtMessageDef.DLQ_TOPIC_PREFFIX;
import static org.apache.kafka.clients.enhance.ExtMessageDef.RETRY_TOPIC_PREFFIX;
import static org.apache.kafka.clients.enhance.consumer.ConsumeType.CONSUME_CONCURRENT;
import static org.apache.kafka.clients.enhance.consumer.ConsumeType.CONSUME_ORDINAL;
import static org.apache.kafka.clients.enhance.consumer.ConsumeType.CONSUME_UNKNOWN;

public final class ConsumeClientContext<K> {
    private static final Logger logger = LoggerFactory.getLogger(ConsumeClientContext.class);
    private static final String INNER_PRODUCER_NAME_SUFFIX = "_inner_producer";
    public static final int INVALID_PROPERTY_VALUE = Integer.MIN_VALUE;
    public static final long TIME_WAIT_FOR_POLL_REC_MS = 200L;
    public static final int DEFAULT_CONSUME_BATCH_SIZE = 1;
    public static final long DEFAULT_OFFSET_STORE_INTERVALS = 5000L;
    public static final long CLIENT_RETRY_BACKOFF_MS = 3000L;
    public static final long DEFAULT_MAX_MESSAGE_DEAL_TIME_MS = 5 * 60 * 1000L;

    private final Map<String, Object> innerConsumeConfig = new HashMap<>();
    private final Map<String, Object> innerProducerConfig = new HashMap<>();

    private ConsumeModel consumeModel = ConsumeModel.GROUP_CLUSTERING;
    private ConsumeType consumeType = CONSUME_UNKNOWN;
    private ExtResetStrategy strategy = ExtResetStrategy.RESET_NONE;
    private volatile int consumeBatchSize = DEFAULT_CONSUME_BATCH_SIZE;
    private int consumeCoreThreadNum = Utility.getCpuCores();
    private int consumeQueueSize = consumeCoreThreadNum * 256;
    private int consumeRequestTimeoutMs = INVALID_PROPERTY_VALUE;
    private Set<String> subTopics = Collections.synchronizedSet(new HashSet<String>());
    private volatile AbsExtMessageFilter<K> messageFilter;
    private volatile MessageHandler<K, ?> messageHandler;
    private final ConsumeMessageHooks<K> consumeHooks = new ConsumeMessageHooks<>();
    private Deserializer<K> keyDeserializer = null;
    private String groupId = null;
    private String clientId = null;
    private long offsetStoreIntervals = DEFAULT_OFFSET_STORE_INTERVALS; //every 5s store current offsets
    private long pollMessageAwaitTimeoutMs = TIME_WAIT_FOR_POLL_REC_MS;
    private long clientRetryBackoffMs = CLIENT_RETRY_BACKOFF_MS;
    private long maxMessageDealTimeMs = DEFAULT_MAX_MESSAGE_DEAL_TIME_MS;

    private Serializer<K> keySerializer = null;
    private int producerRequestTimeoutMs = INVALID_PROPERTY_VALUE;
    private int producerMaxBlockMs = INVALID_PROPERTY_VALUE;
    private int producerAcks = INVALID_PROPERTY_VALUE;
    private int producerRetries = INVALID_PROPERTY_VALUE;
    private int producerBatchSize = INVALID_PROPERTY_VALUE;

    public ConsumeClientContext clientRetryBackoffMs(long backoffTime, TimeUnit unit) {
        try {
            clientRetryBackoffMs = unit.toMillis(backoffTime);
        } catch (Exception ex) {
            logger.warn("Setting retry backoff time failed, use default value. due to ", ex);
        }
        return this;
    }

    public long clientRetryBackoffMs() {
        return clientRetryBackoffMs;
    }

    public ConsumeClientContext maxMessageDealTimeMs(long dealTime, TimeUnit unit) {
        try {
            maxMessageDealTimeMs = unit.toMillis(dealTime);
        } catch (Exception ex) {
            logger.warn("Setting maxMessageDealTimeMs time failed, use default value. due to ", ex);
        }
        return this;
    }

    public long maxMessageDealTimeMs() {
        return maxMessageDealTimeMs;
    }

    public ConsumeClientContext addConsumeHook(ConsumeMessageHook<K> consumeHook) {
        if (null != consumeHook) {
            consumeHooks.addMessageHook(consumeHook);
        }
        return this;
    }

    public ConsumeMessageHooks<K> consumeHooks() {
        return this.consumeHooks;
    }

    public ConsumeClientContext messageHandler(MessageHandler messageHandler) {
        ConsumeType handlerConsumeType = CONSUME_UNKNOWN;
        if (messageHandler instanceof ConcurrentMessageHandler) {
            handlerConsumeType = CONSUME_CONCURRENT;
        } else if (messageHandler instanceof OrdinalMessageHandler) {
            handlerConsumeType = CONSUME_ORDINAL;
        }

        if (CONSUME_UNKNOWN == handlerConsumeType && CONSUME_UNKNOWN == this.consumeType) {
            throw new KafkaConsumeException("message handler error. please implement from [OrdinalMessageHandler, ConcurrentMessageHandler].");
        } else if (CONSUME_UNKNOWN != handlerConsumeType && CONSUME_UNKNOWN == this.consumeType) {
            this.consumeType = handlerConsumeType;
            this.messageHandler = messageHandler;
        } else {
            logger.warn("Consume type isn't matched, couldn't be changed in runtime.");
        }
        return this;
    }

    public MessageHandler<K, ?> messageHandler() {
        return messageHandler;
    }

    public ConsumeClientContext consumeBatchSize(int consumeBatchSize) {
        if (consumeBatchSize <= DEFAULT_CONSUME_BATCH_SIZE) {
            this.consumeBatchSize = DEFAULT_CONSUME_BATCH_SIZE;
        } else {
            this.consumeBatchSize = consumeBatchSize;
        }
        return this;
    }

    public int consumeBatchSize() {
        return consumeBatchSize;
    }

    public ConsumeClientContext pollMessageAwaitTimeMs(long awaitTimeout) {
        this.pollMessageAwaitTimeoutMs = awaitTimeout;
        return this;
    }

    public long pollMessageAwaitTimeoutMs() {
        return pollMessageAwaitTimeoutMs;
    }

    public ConsumeClientContext offsetStoreIntervals(long storeIntervalsMs) {
        this.offsetStoreIntervals = storeIntervalsMs;
        return this;
    }

    public long offsetStoreIntervals() {
        return this.offsetStoreIntervals;
    }

    public ConsumeClientContext groupId(String groupId) {
        if (null != groupId && !groupId.isEmpty()) {
            this.groupId = groupId;
            updateConfigByProp(innerConsumeConfig, ConsumerConfig.GROUP_ID_CONFIG, groupId);
        }
        return this;
    }

    public String groupId() {
        return this.groupId;
    }

    public ConsumeClientContext clientId(String clientId) {
        this.clientId = clientId;
        if (null != clientId && !clientId.isEmpty()) {
            updateConfigByProp(innerConsumeConfig, ConsumerConfig.CLIENT_ID_CONFIG, clientId);
            updateConfigByProp(innerProducerConfig, ProducerConfig.CLIENT_ID_CONFIG,
                    clientId + INNER_PRODUCER_NAME_SUFFIX);
        }
        return this;
    }

    public String clientId() {
        return this.clientId;
    }

    public ConsumeClientContext consumeConfig(Map<String, Object> originalConfig) {
        selectConfigItem(originalConfig);
        return this;
    }

    public ConsumeClientContext consumeConfig(Properties originalProps) {
        Map<String, Object> propsMap = new HashMap<>();
        for (String key : originalProps.stringPropertyNames()) {
            propsMap.put(key, originalProps.getProperty(key));
        }
        consumeConfig(propsMap);
        return this;
    }

    private void selectConfigItem(Map<String, Object> originalConfig) {
        Set<String> consumerKeys = ConsumerConfig.configNames();
        Set<String> producerKeys = ProducerConfig.configNames();

        for (String key : originalConfig.keySet()) {
            try {
                int i = 0;
                if (consumerKeys.contains(key)) {
                    innerConsumeConfig.put(key, String.valueOf(originalConfig.get(key)));
                    i++;
                }

                if (producerKeys.contains(key)) {
                    innerProducerConfig.put(key, String.valueOf(originalConfig.get(key)));
                    i++;
                }

                if (0 == i){
                    logger.info("Invalid property: key = [{}], value = [{}].", key, originalConfig.get(key));
                }
            } catch (Exception ex) {
                logger.warn("Invalid property type: key = [{}], value = [{}].", key, originalConfig.get(key));
            }
        }
        innerConsumeConfig.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        updateContext();
    }

    private void updateContext() {
        //update internal consumer property
        try {
            Set<String> consumeCfgKeys = innerConsumeConfig.keySet();
            if (consumeCfgKeys.contains(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG)) {
                String offsetResetCfg = String.valueOf(innerConsumeConfig.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));
                strategy = ExtResetStrategy.parseFrom(offsetResetCfg);
            } else {
                strategy = ExtResetStrategy.RESET_FROM_LATEST;
            }
            if (consumeCfgKeys.contains(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG)) {
                consumeRequestTimeoutMs = Integer.valueOf((String) innerConsumeConfig.get(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG));
            } else {
                consumeRequestTimeoutMs = 305000;
            }

            if (consumeCfgKeys.contains(ConsumerConfig.GROUP_ID_CONFIG)) {
                groupId = String.valueOf(innerConsumeConfig.get(ConsumerConfig.GROUP_ID_CONFIG));
            }

            if (consumeCfgKeys.contains(ConsumerConfig.CLIENT_ID_CONFIG)) {
                clientId = String.valueOf(innerConsumeConfig.get(ConsumerConfig.CLIENT_ID_CONFIG));
                //refresh inner producer clientId
                updateConfigByProp(innerProducerConfig, ProducerConfig.CLIENT_ID_CONFIG,
                        clientId + INNER_PRODUCER_NAME_SUFFIX);
            }
        } catch (Exception ex) {
            logger.info("update consumer context error.", ex);
        }

        //update internal producer property
        Set<String> producerCfgKeys = innerProducerConfig.keySet();

        try {
            if (producerCfgKeys.contains(ProducerConfig.ACKS_CONFIG)) {
                String acksValue = (String) innerProducerConfig.get(ProducerConfig.ACKS_CONFIG);
                try {
                    producerAcks = Integer.valueOf(acksValue);
                } catch (Exception ex) {
                    producerAcks = -1;
                }
            } else {
                producerAcks = -1;
            }

            if (producerCfgKeys.contains(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG)) {
                producerRequestTimeoutMs = Integer.valueOf((String) innerProducerConfig.get(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG));
            } else {
                producerRequestTimeoutMs = 30 * 1000;
            }

            if (producerCfgKeys.contains(ProducerConfig.RETRIES_CONFIG)) {
                producerRetries = Integer.valueOf((String) innerProducerConfig.get(ProducerConfig.RETRIES_CONFIG));
            } else {
                producerRetries = 3;
            }

            if (producerCfgKeys.contains(ProducerConfig.BATCH_SIZE_CONFIG)) {
                producerBatchSize = Integer.valueOf((String) innerProducerConfig.get(ProducerConfig.BATCH_SIZE_CONFIG));
            } else {
                producerBatchSize = 16384;
            }

            if (producerCfgKeys.contains(ProducerConfig.MAX_BLOCK_MS_CONFIG)) {
                producerMaxBlockMs = Integer.valueOf((String) innerProducerConfig.get(ProducerConfig.MAX_BLOCK_MS_CONFIG));
            } else {
                producerMaxBlockMs = 60 * 1000;
            }
        } catch (Exception ex) {
            logger.info("update consumer context error.", ex);
        }
    }

    private void updateConfigByProp(Map<String, Object> config, String propKey, Object propVal) {
        if (null == propVal || propKey.isEmpty()) {
            return;
        }

        try {
            config.put(propKey, String.valueOf(propVal));
        } catch (Exception ex) {
            logger.warn("update property failed. key = [{}], val = [{}]", propKey, propVal);
        }
    }

    public ConsumeClientContext consumeModel(ConsumeModel model) {
        this.consumeModel = model;
        updateConfigByProp(innerConsumeConfig, ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
                BroadcastAssignor.class.getName());
        return this;
    }

    public ConsumeType consumeType() {
        return this.consumeType;
    }

    public ConsumeClientContext messageFilter(AbsExtMessageFilter<K> messageFilter) {
        this.messageFilter = messageFilter;
        return this;
    }

    public AbsExtMessageFilter<K> messageFilter() {
        return messageFilter;
    }

    public Deserializer<K> keyDeserializer() {
        return keyDeserializer;
    }

    public Serializer<K> keySerializer() {
        return keySerializer;
    }

    ConsumeClientContext keySerializer(Serializer<K> keySerializer) {
        this.keySerializer = keySerializer;
        return this;
    }

    ConsumeClientContext keyDeserializer(Deserializer<K> keyDeserializer) {
        this.keyDeserializer = keyDeserializer;
        return this;
    }

    public ConsumeClientContext resetStrategy(ExtResetStrategy strategy) {
        this.strategy = strategy;
        return this;
    }

    public ConsumeClientContext coreThreadNum(int num) {
        this.consumeCoreThreadNum = num;
        return this;
    }

    public ConsumeClientContext consumeRequestTimeout(int consumeRequestTimeoutMs) {
        this.consumeRequestTimeoutMs = consumeRequestTimeoutMs;
        updateConfigByProp(innerConsumeConfig, ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, consumeRequestTimeoutMs);
        return this;
    }

    public ConsumeClientContext producerRequestTimeout(int producerRequestTimeoutMs) {
        this.producerRequestTimeoutMs = producerRequestTimeoutMs;
        updateConfigByProp(innerProducerConfig, ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, producerRequestTimeoutMs);
        return this;
    }

    public int producerRequestTimeout() {
        return this.producerRequestTimeoutMs;
    }

    public ConsumeClientContext producerAcks(int acks) {
        this.producerAcks = acks;
        updateConfigByProp(innerProducerConfig, ProducerConfig.ACKS_CONFIG, acks);
        return this;
    }

    public int producerAcks() {
        return this.producerAcks;
    }

    public ConsumeClientContext producerRetries(int retries) {
        this.producerRetries = retries;
        updateConfigByProp(innerProducerConfig, ProducerConfig.RETRIES_CONFIG, retries);
        return this;
    }

    public int producerRetries() {
        return this.producerRetries;
    }

    public ConsumeClientContext producerBatchSize(int batchSize) {
        this.producerBatchSize = batchSize;
        updateConfigByProp(innerProducerConfig, ProducerConfig.BATCH_SIZE_CONFIG, batchSize);
        return this;
    }

    public int producerBatchSize() {
        return this.producerBatchSize;
    }

    public ConsumeClientContext producerMaxBlockMs(int maxBlockMs) {
        this.producerMaxBlockMs = maxBlockMs;
        updateConfigByProp(innerProducerConfig, ProducerConfig.MAX_BLOCK_MS_CONFIG, maxBlockMs);
        return this;
    }

    public int producerMaxBlockMs() {
        return this.producerMaxBlockMs;
    }

    public ConsumeModel consumeModel() {
        return consumeModel;
    }

    public ExtResetStrategy resetStrategy() {
        return strategy;
    }

    public int consumeThreadNum() {
        return this.consumeCoreThreadNum;
    }

    public int consumeQueueSize() {
        return this.consumeQueueSize;
    }

    public int consumeRequestTimeoutMs() {
        return this.consumeRequestTimeoutMs;
    }

    public static boolean isValidValue(int value) {
        return INVALID_PROPERTY_VALUE != value;
    }

    public Map<String, Object> getInternalProducerProps() {
        return Collections.unmodifiableMap(innerProducerConfig);
    }

    public Map<String, Object> getInternalConsumerProps() {
        return Collections.unmodifiableMap(innerConsumeConfig);
    }

    public void addTopic(String topic) {
        if (Utility.isInvalidString(topic)) return;
        subTopics.add(topic);
    }

    public void addTopic(Collection<String> topics) {
        for (String topic : topics) {
            addTopic(topic);
        }
    }

    public void clearTopic() {
        subTopics.clear();
    }

    public Set<String> getTopics() {
        return Collections.unmodifiableSet(subTopics);
    }

    public String retryTopicName() {
        return Utility.normalizeTopicName(RETRY_TOPIC_PREFFIX + groupId);
    }

    public String deadLetterTopicName() {
        return Utility.normalizeTopicName(DLQ_TOPIC_PREFFIX + groupId);
    }

}
