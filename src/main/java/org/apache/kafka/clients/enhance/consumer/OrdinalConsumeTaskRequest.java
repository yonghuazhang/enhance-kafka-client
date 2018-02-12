package org.apache.kafka.clients.enhance.consumer;

import org.apache.kafka.clients.enhance.ExtMessage;
import org.apache.kafka.clients.enhance.consumer.listener.ConsumeStatus;
import org.apache.kafka.clients.enhance.consumer.listener.OrdinalConsumeHandlerContext;
import org.apache.kafka.clients.enhance.consumer.listener.OrdinalMessageHandler;
import org.apache.kafka.common.TopicPartition;

import java.util.List;

public class OrdinalConsumeTaskRequest<K> extends AbstractConsumeTaskRequest<K> {
    private final OrdinalConsumeHandlerContext consumeContext;
    private final OrdinalMessageHandler<K> handler;

    public OrdinalConsumeTaskRequest(AbstractConsumeService<K> service, PartitionDataManager manager,
                                     List<ExtMessage<K>> extMessages, TopicPartition topicPartition,
                                     ConsumeClientContext<K> clientContext, OrdinalConsumeHandlerContext consumeContext,
                                     OrdinalMessageHandler<K> handler) {
        super(service, manager, extMessages, topicPartition, clientContext);
        this.consumeContext = consumeContext;
        this.handler = handler;
    }

    @Override
    public void processConsumeStatus(ConsumeStatus status) {

    }

    @Override
    public ConsumeTaskResponse call() throws Exception {
        return null;
    }
}
