package org.apache.kafka.clients.enhance.consumer;

import org.apache.kafka.clients.enhance.ExtMessage;
import org.apache.kafka.clients.enhance.consumer.listener.ConcurrentConsumeHandlerContext;
import org.apache.kafka.clients.enhance.consumer.listener.ConcurrentMessageHandler;
import org.apache.kafka.clients.enhance.consumer.listener.ConsumeStatus;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class ConcurrentConsumeTaskRequest<K> extends AbsConsumeTaskRequest<K> {
    private final ConcurrentConsumeHandlerContext consumeContext;
    private final ConcurrentMessageHandler<K> handler;

    public ConcurrentConsumeTaskRequest(AbsConsumeService<K> service, PartitionDataManager manager,
                                        List<ExtMessage<K>> extMessages, TopicPartition topicPartition,
                                        ConsumeClientContext<K> clientContext, ConcurrentMessageHandler<K> handler) {
        super(service, manager, extMessages, topicPartition, clientContext);
        long firstOffsetInBatch = messages.get(FIRST_MESSAGE_IDX).getOffset();
        this.consumeContext = new ConcurrentConsumeHandlerContext(topicPartition, firstOffsetInBatch);
        this.handler = handler;
    }


    @Override
    public void dealWithConsumeStatus(ConsumeStatus status) {
        switch (status) {
            case CONSUME_RETRY_LATER:
                List<ExtMessage<K>> failedRecords = new ArrayList<>();
                for (ExtMessage<K> mesg : messages) {
                    boolean sendOk = consumeService.sendMessageBack(mesg, 1);
                    if (!sendOk) {
                        failedRecords.add(mesg);
                    }
                }
                if (!failedRecords.isEmpty()) {
                    logger.trace("sending message back failed list:" + Arrays.toString(failedRecords.toArray(new Object[0])));
                    consumeService.dispatchTaskLater(new ConcurrentConsumeTaskRequest<K>(consumeService, manager, failedRecords,
                            topicPartition, clientContext, handler), clientContext.clientRetryBackoffMs(), TimeUnit.MILLISECONDS);
                }

                final List<Long> offsets = new ArrayList<>(messages.size());
                for (ExtMessage<K> message : messages) {
                    offsets.add(message.getOffset());
                }
                logger.trace("start commitoffsets ---------------> " + offsets.toString());
                manager.commitSlidingWinOffset(topicPartition, offsets);


                break;
            case CONSUME_SUCCESS:
                break;
            case CONSUME_DISCARD:
                break;
            default:

                break;
        }
    }

    @Override
    public ConsumeTaskResponse call() throws Exception {
        return null;
    }
}
