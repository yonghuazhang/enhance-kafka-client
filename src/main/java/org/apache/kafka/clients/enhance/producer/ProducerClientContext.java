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
}
