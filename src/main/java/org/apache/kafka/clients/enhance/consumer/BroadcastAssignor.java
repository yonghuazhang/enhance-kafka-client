package org.apache.kafka.clients.enhance.consumer;

import org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignor;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BroadcastAssignor extends AbstractPartitionAssignor {

    @Override
    public String name() {
        return "broadcast";
    }

    @Override
    public Map<String, List<TopicPartition>> assign(Map<String, Integer> partitionsPerTopic, Map<String, Subscription> subscriptions) {
        Map<String, List<TopicPartition>> assignment = new HashMap<>();
        for (String memberId : subscriptions.keySet())
            assignment.put(memberId, new ArrayList<TopicPartition>());

        for (Map.Entry<String, Subscription> subEntry : subscriptions.entrySet()) {
            String memberId = subEntry.getKey();
            for (String topic : subEntry.getValue().topics()) {
                Integer numPartitionsForTopic = partitionsPerTopic.get(topic);
                if (numPartitionsForTopic == null || numPartitionsForTopic.equals(0)) {
                    continue;
                }
                List<TopicPartition> topicPartitions = partitions(topic, numPartitionsForTopic);
                if (!topicPartitions.isEmpty()) {
                    assignment.get(memberId).addAll(topicPartitions);
                }
            }
        }
        return assignment;
    }
}
