package org.apache.kafka.clients.enhance.consumer;

import org.apache.kafka.clients.enhance.ExtMessage;
import org.apache.kafka.clients.enhance.ExtMessageEncoder;
import org.apache.kafka.clients.enhance.AbsExtMessageFilter;
import org.apache.kafka.clients.enhance.Utility;
import org.apache.kafka.clients.enhance.consumer.listener.ConsumeMessageHook;
import org.apache.kafka.clients.enhance.consumer.listener.MessageHandler;
import org.apache.kafka.clients.enhance.exception.KafkaConsumeException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by steven03.zhang on 2018/1/17.
 */
public class KafkaPushConsumer<K> implements ConsumeOperator<K> {
    private static final Logger logger = LoggerFactory.getLogger(KafkaPushConsumer.class);
    private static final long DEFAULT_CLOSE_TIMEOUT_MS = 30 * 1000;
    private final ConsumeClientContext<K> clientContext = new ConsumeClientContext<>();

    private KafkaProducer<K, ExtMessage<K>> innerSender;
    private EnhanceConsumer<K> safeConsumer;
    private ConsumeService<K> consumeService;

    private final ReentrantLock lock = new ReentrantLock();
    private volatile boolean isRunning = false;

    public KafkaPushConsumer(Map<String, Object> config) {
        this(config, null);
    }

    public KafkaPushConsumer(Map<String, Object> config, Class<K> cls) {
        clientContext.consumeConfig(config);
        if (null != cls) {
            clientContext.keySerializer(Serdes.serdeFrom(cls).serializer());
            clientContext.keyDeserializer(Serdes.serdeFrom(cls).deserializer());
        }
    }

    public KafkaPushConsumer(Properties config) {
        this(config, null);
    }

    public KafkaPushConsumer(Properties config, Class<K> cls) {
        clientContext.consumeConfig(config);
        if (null != cls) {
            clientContext.keySerializer(Serdes.serdeFrom(cls).serializer());
            clientContext.keyDeserializer(Serdes.serdeFrom(cls).deserializer());
        }
    }

    public KafkaPushConsumer(Properties config, Serializer<K> keySerializer,
                             Deserializer<K> keyDeserializer) {
        clientContext.consumeConfig(config);
        clientContext.keySerializer(keySerializer);
        clientContext.keyDeserializer(keyDeserializer);
    }

    public ConsumeClientContext consumeSetting() {
        return clientContext;
    }

    public int getServiceThreadNum() {
        if (isRunning && null != consumeService) {
            return consumeService.getThreadCores();
        }
        return clientContext.consumeThreadNum();
    }

    public int getServiceQueueSize() {
        if (isRunning && null != consumeService) {
            return consumeService.getQueueSize();
        }
        return clientContext.consumeQueueSize();
    }

    @Override
    public String groupId() {
        if (isRunning && null != safeConsumer) {
            return safeConsumer.groupId();
        }
        return clientContext.groupId();
    }

    @Override
    public String clientId() {
        if (isRunning && null != safeConsumer) {
            return safeConsumer.clientId();
        }
        return clientContext.clientId();
    }

    @Override
    public ConsumeModel consumeModel() {
        return this.clientContext.consumeModel();
    }

    @Override
    public ExtResetStrategy consumeResetStarategy() {
        return this.clientContext.resetStrategy();
    }

    @Override
    public void subscribe(String topic) {
        this.subscribe(topic, "*");
    }

    @Override
    public void subscribe(String topic, String filterPattern) {
        this.subscribe(Arrays.asList(topic), new DefaultMessageFilter<K>(filterPattern));
    }

    @Override
    public void subscribe(String topic, AbsExtMessageFilter<K> filter) {
        this.subscribe(Arrays.asList(topic), filter);
    }

    @Override
    public void subscribe(Collection<String> topics, String filterPattern) {
        this.subscribe(topics, new DefaultMessageFilter<K>(filterPattern));
    }

    @Override
    public void subscribe(Collection<String> topics, AbsExtMessageFilter<K> filter) {
        clientContext.addTopic(topics);
        setMessageFilter(filter);
        if (isRunning && null != consumeService) {
            consumeService.subscribe(clientContext.getTopics());
        }
    }

    private void setMessageFilter(AbsExtMessageFilter<K> filter) {
        if (null != filter) {
            clientContext.messageFilter(filter);
        } else {
            clientContext.messageFilter(new DefaultMessageFilter<K>("*"));
        }
    }

    @Override
    public void unsubscribe() {
        clientContext.clearTopic();
        if (isRunning && null != consumeService) {
            consumeService.unsubscribe();
        }
    }

    @Override
    public Set<String> subscription() {
        if (isRunning && null != safeConsumer) {
            return safeConsumer.subscription();
        }
        return clientContext.getTopics();
    }

    @Override
    public void shutdownNow() {
        shutdown(0, TimeUnit.MILLISECONDS);
    }

    @Override
    public void shutdown(long timeout, TimeUnit unit) {
        lock.lock();
        if (isRunning) {
            try {
                if (null != consumeService) {
                    consumeService.shutdown(timeout, unit);
                }
                if (null != safeConsumer) {
                    safeConsumer.close(timeout, TimeUnit.MILLISECONDS);
                }
                if (null != innerSender) {
                    innerSender.close(timeout, TimeUnit.MILLISECONDS);
                }
                isRunning = false;
            } finally {
                consumeService = null;
                safeConsumer = null;
                innerSender = null;
                lock.unlock();
            }
        }
    }

    @Override
    public void start() {
        lock.lock();
        if (!isRunning) {
            try {
                if (null == clientContext.messageHandler()) {
                    throw new KafkaConsumeException("Message handler couldn't be null, please reset it and restart.");
                }
                safeConsumer = new EnhanceConsumer<>(clientContext.getInternalConsumerProps(),
                        clientContext.keyDeserializer());
                innerSender = new KafkaProducer<>(clientContext.getInternalProducerProps(), clientContext.keySerializer(), new ExtMessageEncoder<K>());

                //根据消费顺序性选择不同的服务
                switch (clientContext.consumeType()) {
                    case CONSUME_ORDINAL:
                        consumeService = new OrdinalConsumeService<>(safeConsumer, innerSender, clientContext);
                        break;
                    case CONSUME_CONCURRENT:
                        consumeService = new ConcurrentConsumeService<>(safeConsumer, innerSender, clientContext);
                        break;
                    default:
                        throw new KafkaConsumeException("unsupported Consume type. please check code.");
                }
                consumeService.start();
                isRunning = true;
            } catch (Exception ex) {
                logger.error("KafkaPushConsumer initialize failed. due to", ex);
                if (null != safeConsumer) {
                    safeConsumer.close(DEFAULT_CLOSE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                    safeConsumer = null;
                }
                if (null != innerSender) {
                    innerSender.close(DEFAULT_CLOSE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                    innerSender = null;
                }
                if (null != consumeService) {
                    consumeService.shutdownNow();
                    consumeService = null;
                }
            } finally {
                lock.unlock();
            }
        } else {
            logger.info("KafkaPushConsumer has been started.");
        }
    }

    @Override
    public void seek(TopicPartition partition, long offset) {
        if (isRunning && null != consumeService) {
            consumeService.seek(partition, offset);
        } else {
            logger.info("KafkaPushConsumer hasn't been initialized.");
        }
    }

    @Override
    public void seekToTime(long timestamp) {
        if (isRunning && null != consumeService) {
            this.consumeService.seekToTime(timestamp);
        } else {
            logger.info("KafkaPushConsumer hasn't been initialized.");
        }
    }

    @Override
    public void seekToTime(String dateString) { // date string  format such as: "yyyy-MM-dd'T'HH:mm:ss.SSS"
        if (isRunning && null != consumeService) {
            try {
                long timestamp = Utility.convertTimeByString(dateString);
                seekToTime(timestamp);
            } catch (ParseException e) {
                logger.warn("seekToTime failed. due to parsing [{}] date format error. ", dateString);
            }
        } else {
            logger.info("KafkaPushConsumer hasn't been initialized.");
        }
    }

    @Override
    public void seekToBeginning() {
        if (isRunning && null != consumeService) {
            consumeService.seekToBeginning();
        } else {
            logger.info("KafkaPushConsumer hasn't been initialized.");
        }
    }

    @Override
    public void seekToEnd() {
        if (isRunning && null != consumeService) {
            this.consumeService.seekToEnd();
        } else {
            logger.info("KafkaPushConsumer hasn't been initialized.");
        }
    }

    @Override
    public void registerHandler(MessageHandler handler) {
        this.clientContext.messageHandler(handler);
    }

    @Override
    public void addConsumeHook(ConsumeMessageHook consumeHook) {
        this.clientContext.addConsumeHook(consumeHook);
    }

    @Override
    public void suspend() {
        if (isRunning && null != consumeService) {

        }
    }

    @Override
    public void resume() {
        if (isRunning && null != consumeService){

        }
    }

}