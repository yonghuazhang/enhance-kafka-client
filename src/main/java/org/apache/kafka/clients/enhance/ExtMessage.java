package org.apache.kafka.clients.enhance;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.apache.kafka.clients.enhance.ExtMessageDef.joinTagsOrKeys;
import static org.apache.kafka.clients.enhance.ExtMessageDef.splitTagsOrKeys;
import static org.apache.kafka.clients.enhance.ExtMessageDef.validateTagOrKey;

/**
 * Created by steven03.zhang on 2017/12/11.
 */
public class ExtMessage<K> {
    private static final Logger log = LoggerFactory.getLogger(ExtMessage.class);

    private String topic;
    private K mesgKey;
    private int partition;
    private long offset;
    private long logAppendTimeMs;
    private long startConsumingTimeMs;

    //the below properties will be persisted in record.
    private int retryTimes;
    private int delayedLevel;
    private byte[] body;
    private final Map<String, String> properties = new HashMap<>();

    public long getLogAppendTimeMs() {
        return logAppendTimeMs;
    }

    void setLogAppendTimeMs(long logAppendTimeMs) {
        this.logAppendTimeMs = logAppendTimeMs;
    }

    public long getStartConsumingTimeMs() {
        return startConsumingTimeMs;
    }

    void setStartConsumingTimeMs(long lastConsumingTimestamp) {
        this.startConsumingTimeMs = lastConsumingTimestamp;
    }

    void setPartion(int partition) {
        this.partition = partition;
    }

    public int getPartion() {
        return this.partition;
    }

    void setOffset(long offset) {
        this.offset = offset;
    }

    public long getOffset() {
        return this.offset;
    }

    public void addProperty(String hName, String hValue) {
        if (isValidHeaders(hName, hValue)) {
            properties.put(hName, hValue);
        } else {
            log.warn("Invalid params for key = {}, value = {}.", hName, hValue);
        }
    }

    public void addProperties(Map<String, String> props) {
        for (Map.Entry<String, String> entry : props.entrySet()) {
            addProperty(entry.getKey(), entry.getValue());
        }
    }

    public void addProperties(Properties props) {
        for (Map.Entry<Object, Object> entry : props.entrySet()) {
            String key = String.valueOf(entry.getKey());
            String val = String.valueOf(entry.getValue());
            addProperty(key, val);
        }
    }

    private boolean isValidHeaders(String hName, String hValue) {
        if (hName == null || hName.isEmpty() ||
                hValue == null || hValue.isEmpty())
            return false;
        if (this.properties.size() > Short.MAX_VALUE) {
            log.warn("Don't support too much properties for single message, the num of perporties le {}.", Short.MAX_VALUE);
            return false;
        }
        return true;
    }

    void removeProperty(final String name) {
        this.properties.remove(name);
    }

    void clearProperty() {
        this.properties.clear();
    }

    public void addUserProperty(final String name, final String value) {
        if (ExtMessageDef.STRING_HASH_SET.contains(name)) {
            throw new RuntimeException(String.format(
                    "The Property <%s> is used by system, please rename the name of property key", name));
        }
        this.addProperty(name, value);
    }

    public String getUserProperty(final String name) {
        return this.getProperty(name);
    }

    public String getProperty(final String name) {
        return null == this.properties ? ExtMessageDef.STRING_EMPTY : this.properties.get(name);
    }

    public Collection<String> getTags() {
        return splitTagsOrKeys(this.getProperty(ExtMessageDef.PROPERTY_TAGS));
    }

    public void setTags(String tag) {
        this.addProperty(ExtMessageDef.PROPERTY_TAGS, validateTagOrKey(tag));
    }

    public void setTags(Collection<String> tags) {
        this.addProperty(ExtMessageDef.PROPERTY_TAGS, joinTagsOrKeys(tags));
    }

    public Collection<String> getKeys() {
        return splitTagsOrKeys(this.getProperty(ExtMessageDef.PROPERTY_KEYS));
    }

    public void setKeys(String key) {
        this.addProperty(ExtMessageDef.PROPERTY_KEYS, validateTagOrKey(key));
    }

    public void setKeys(Collection<String> keys) {
        this.addProperty(ExtMessageDef.PROPERTY_KEYS, joinTagsOrKeys(keys));
    }

    public int getDelayedLevel() {
        return this.delayedLevel;
    }

    public void setDelayedLevel(int level) {
        if (level <= 0) {
            this.delayedLevel = 0;
        } else if (level > ExtMessageDef.MAX_DELAY_TIME_LEVEL) {
            this.delayedLevel = ExtMessageDef.MAX_DELAY_TIME_LEVEL;
        } else {
            this.delayedLevel = level;
        }
    }

    public int getRetryTimes() {
        return retryTimes;
    }

    public void setRetryTimes(int retryTimes) {
        if (retryTimes < 0) {
            this.retryTimes = 0;
        } else if (retryTimes > ExtMessageDef.MAX_RECONSUME_TIMES) {
            this.retryTimes = ExtMessageDef.MAX_RECONSUME_TIMES;
        } else {
            this.retryTimes = retryTimes;
        }
    }

    public ExtMessage<K> updateByRecord(ConsumerRecord<K, ExtMessage<K>> record) {
        this.mesgKey = record.key();
        this.offset = record.offset();
        this.partition = record.partition();
        this.logAppendTimeMs = record.timestamp();
        return this;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public byte[] getBody() {
        return body;
    }

    public void setBody(byte[] body) {
        this.body = null == body ? new byte[0] : body;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public String getBuyerId() {
        return getProperty(ExtMessageDef.PROPERTY_BUYER_ID);
    }

    public void setBuyerId(String buyerId) {
        addProperty(ExtMessageDef.PROPERTY_BUYER_ID, buyerId);
    }

    @Override
    public String toString() {
        return topic + "-" + partition + "-" + offset;
    }
}
