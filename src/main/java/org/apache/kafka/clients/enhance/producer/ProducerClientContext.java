package org.apache.kafka.clients.enhance.producer;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public final class ProducerClientContext<K> {
    private static final Logger logger = LoggerFactory.getLogger(ProducerClientContext.class);
    private final Map<String, Object> innerProducerConfig = new HashMap<>();
    private final SendMessageHooks<K> hooks = new SendMessageHooks<>();

    private Serializer<K> keySerializer = null;

    public ProducerClientContext producerConfig(Map<String, Object> originalConfig) {
        selectConfigItem(originalConfig);
        return this;
    }

    public ProducerClientContext producerConfig(Properties originalProps) {
        Map<String, Object> propsMap = new HashMap<>();
        for (Map.Entry entry : originalProps.entrySet()) {
            propsMap.put(String.valueOf(entry.getKey()), entry.getValue());
        }
        producerConfig(propsMap);
        return this;
    }

    private void selectConfigItem(Map<String, Object> originalConfig) {
        Set<String> producerKeys = ProducerConfig.configNames();

        for (String key : originalConfig.keySet()) {
            try {
                if (producerKeys.contains(key)) {
                    innerProducerConfig.put(key, String.valueOf(originalConfig.get(key)));
                } else {
                    logger.info("[ProducerClientContext] Invalid property: key = [{}], value = [{}].", key, originalConfig.get(key));

                }
            } catch (Exception ex) {
                logger.warn("[ProducerClientContext] Invalid property type: key = [{}], value = [{}].", key, originalConfig.get(key));
            }
        }
    }

    public boolean isTransactionProducer() {
        return innerProducerConfig.containsKey(ProducerConfig.TRANSACTIONAL_ID_CONFIG);
    }

    public boolean isIdempotenceProducer() {
        try {
            return Boolean.valueOf(innerProducerConfig.containsKey(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG));
        } catch (Exception e) {
            return false;
        }
    }

    public ProducerClientContext setTransactionId(String transactionId) {
        innerProducerConfig.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionId);
        return this;
    }

    public ProducerClientContext setIdempotence(boolean isIdempotence) {
        innerProducerConfig.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, String.valueOf(isIdempotence));
        return this;
    }

    ProducerClientContext keySerializer(Serializer<K> keySerializer) {
        this.keySerializer = keySerializer;
        innerProducerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer.getClass().getName());
        return this;
    }

    public Serializer<K> keySerializer() {
        return keySerializer;
    }

    public ProducerClientContext transactionTimeoutMs(long timeoutMs) {
        innerProducerConfig.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, String.valueOf(timeoutMs));
        return this;
    }

    public ProducerClientContext requestTimeoutMs(long timeoutms) {
        innerProducerConfig.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, String.valueOf(timeoutms));
        return this;
    }

    public ProducerClientContext maxInFlight(int maxInFlight) {
        innerProducerConfig.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, String.valueOf(maxInFlight));
        return this;
    }

    public ProducerClientContext maxRequestByteSize(int maxRequestByteSize) {
        innerProducerConfig.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, String.valueOf(maxRequestByteSize));
        return this;
    }

    public ProducerClientContext maxBlockMs(long maxBlockMs) {
        innerProducerConfig.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, String.valueOf(maxBlockMs));
        return this;
    }

    public ProducerClientContext acks(String acks) {
        innerProducerConfig.put(ProducerConfig.ACKS_CONFIG, acks);
        return this;
    }

    public ProducerClientContext bufferMemory(long bufferSize) {
        innerProducerConfig.put(ProducerConfig.BUFFER_MEMORY_CONFIG, String.valueOf(bufferSize));
        return this;
    }

    public ProducerClientContext retries(int retryTimes) {
        innerProducerConfig.put(ProducerConfig.RETRIES_CONFIG, retryTimes);
        return this;
    }

    public ProducerClientContext compressionType(String type) {
        innerProducerConfig.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, type);
        return this;
    }

    public ProducerClientContext batchSize(int batchSize) {
        innerProducerConfig.put(ProducerConfig.BATCH_SIZE_CONFIG, String.valueOf(batchSize));
        return this;
    }

    public ProducerClientContext clientId(String clientId) {
        innerProducerConfig.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
        return this;
    }

    public ProducerClientContext maxIdleMsForConnections(long idleMs) {
        innerProducerConfig.put(ProducerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, String.valueOf(idleMs));
        return this;
    }

    public ProducerClientContext lingerMs(long lingerMs) {
        innerProducerConfig.put(ProducerConfig.LINGER_MS_CONFIG, String.valueOf(lingerMs));
        return this;
    }

    Map<String, Object> getProducerConfig(){
        return this.innerProducerConfig;
    }

    public void addSendMessageHook(SendMessageHook<K> hook) {
        this.hooks.addSendMessageHook(hook);
    }
}
