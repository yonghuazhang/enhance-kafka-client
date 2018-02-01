package org.apache.kafka.clients.enhance.consumer;

import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.enhance.Utility;

import java.text.ParseException;

import static org.apache.kafka.clients.enhance.Utility.INVALID_TIMESTAMP;

/**
 * Created by steven03.zhang on 2017/12/13.
 */
public enum ExtResetStrategy {
    RESET_FROM_EARLIEST(OffsetResetStrategy.EARLIEST, "earliest", INVALID_TIMESTAMP),
    RESET_FROM_LATEST(OffsetResetStrategy.LATEST, "latest", INVALID_TIMESTAMP),
    RESET_FROM_TIMESTAMP(OffsetResetStrategy.LATEST, "timestamp", INVALID_TIMESTAMP),
    RESET_NONE(OffsetResetStrategy.NONE, "none", INVALID_TIMESTAMP);

    ExtResetStrategy(OffsetResetStrategy resetStrategy, String strategyName, long timeMs) {
        this.resetStrategy = resetStrategy;
        this.strategyName = strategyName;
        this.timestamp = timeMs;
    }

    private final OffsetResetStrategy resetStrategy;
    private final String strategyName;
    private long timestamp;

    public OffsetResetStrategy getResetStrategy() {
        return resetStrategy;
    }

    public ExtResetStrategy timestamp(long timeMs) {
        this.timestamp = timeMs;
        return this;
    }

    public ExtResetStrategy timestamp(String timeString) throws ParseException {
        this.timestamp = Utility.convertTimeByString(timeString);
        return this;
    }

    public long timestamp() {
        if (RESET_FROM_TIMESTAMP == this) {
            return timestamp;
        } else {
            return INVALID_TIMESTAMP;
        }
    }

    public boolean isValidTimestamp() {
        return timestamp != INVALID_TIMESTAMP;
    }

    public String getStrategyName() {
        return strategyName;
    }

    public static ExtResetStrategy parseFrom(String name) {
        if (null == name || name.isEmpty()) return ExtResetStrategy.RESET_NONE;
        for (ExtResetStrategy strategy : ExtResetStrategy.values()) {
            if (strategy.strategyName.equals(name))
                return strategy;
        }
        return ExtResetStrategy.RESET_NONE;
    }

}
