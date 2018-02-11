package org.apache.kafka.clients.enhance.consumer;

import java.util.concurrent.TimeUnit;

public enum DelayedMessageTopic {
    SYS_DELAYED_TOPIC_5S(5, TimeUnit.SECONDS),
    SYS_DELAYED_TOPIC_10S(10, TimeUnit.SECONDS),
    SYS_DELAYED_TOPIC_15S(15, TimeUnit.SECONDS),
    SYS_DELAYED_TOPIC_30S(30, TimeUnit.SECONDS),
    SYS_DELAYED_TOPIC_45S(45, TimeUnit.SECONDS),
    SYS_DELAYED_TOPIC_1M(1, TimeUnit.MINUTES),
    SYS_DELAYED_TOPIC_2M(2, TimeUnit.MINUTES),
    SYS_DELAYED_TOPIC_3M(3, TimeUnit.MINUTES),
    SYS_DELAYED_TOPIC_4M(4, TimeUnit.MINUTES),
    SYS_DELAYED_TOPIC_5M(5, TimeUnit.MINUTES),
    SYS_DELAYED_TOPIC_6M(6, TimeUnit.MINUTES),
    SYS_DELAYED_TOPIC_7M(7, TimeUnit.MINUTES),
    SYS_DELAYED_TOPIC_8M(8, TimeUnit.MINUTES),
    SYS_DELAYED_TOPIC_9M(9, TimeUnit.MINUTES),
    SYS_DELAYED_TOPIC_10M(10, TimeUnit.MINUTES),
    SYS_DELAYED_TOPIC_20M(20, TimeUnit.MINUTES),
    SYS_DELAYED_TOPIC_30M(30, TimeUnit.MINUTES),
    SYS_DELAYED_TOPIC_45M(45, TimeUnit.MINUTES),
    SYS_DELAYED_TOPIC_1H(1, TimeUnit.HOURS),
    SYS_DELAYED_TOPIC_2H(2, TimeUnit.HOURS);

    private final long duration;
    private final TimeUnit unit;

    DelayedMessageTopic(long duration, TimeUnit unit) {
        this.duration = duration;
        this.unit = unit;
    }

    public long getDurationMs() {
        return unit.toMillis(duration);
    }

    public String getDelayedTopicName(String prefix, String suffix) {
        StringBuffer builder = new StringBuffer();

        if (null != prefix && !prefix.isEmpty()) {
            builder.append(prefix);
        }

        builder.append(this.name());

        if (null != suffix && !suffix.isEmpty()) {
            builder.append(suffix);
        }

        return builder.toString();
    }

    // level from 1 to 20
    public String getDelayedTopicByLevel(final int level, final String prefix, final String suffix) {
        DelayedMessageTopic[] allDelayedTopics = DelayedMessageTopic.values();
        int idx = level - 1;
        if (idx < 0) {
            idx = 0;
        } else if(idx > allDelayedTopics.length - 1) {
            idx = allDelayedTopics.length - 1;
        }
        return allDelayedTopics[idx].getDelayedTopicName(prefix, suffix);
    }

    public static DelayedMessageTopic parseFromDelayLevel(int level) {
        DelayedMessageTopic[] allDelayedTopics = DelayedMessageTopic.values();
        int idx = level - 1;
        if (idx < 0) {
            idx = 0;
        } else if(idx > allDelayedTopics.length - 1) {
            idx = allDelayedTopics.length - 1;
        }
        return allDelayedTopics[idx];
    }
}
