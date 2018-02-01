package org.apache.kafka.clients.enhance.consumer;

import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ExtResetStrategyTest {
    @Test
    public void getResetStrategy() throws Exception {
        assertEquals(OffsetResetStrategy.LATEST, ExtResetStrategy.RESET_FROM_LATEST.getResetStrategy());
        assertEquals(OffsetResetStrategy.EARLIEST, ExtResetStrategy.RESET_FROM_EARLIEST.getResetStrategy());
        assertEquals(OffsetResetStrategy.LATEST, ExtResetStrategy.RESET_FROM_TIMESTAMP.getResetStrategy());
        assertEquals(OffsetResetStrategy.NONE, ExtResetStrategy.RESET_NONE.getResetStrategy());
    }

    @Test
    public void getStrategyName() throws Exception {
        assertEquals("latest", ExtResetStrategy.RESET_FROM_LATEST.getStrategyName());
        assertEquals("earliest", ExtResetStrategy.RESET_FROM_EARLIEST.getStrategyName());
        assertEquals("timestamp", ExtResetStrategy.RESET_FROM_TIMESTAMP.getStrategyName());
        assertEquals("none", ExtResetStrategy.RESET_NONE.getStrategyName());
    }

    @Test
    public void parseFrom() throws Exception {
        String testName = "latest";
        ExtResetStrategy strategy = ExtResetStrategy.parseFrom(testName);
        assertEquals(testName, strategy.getStrategyName());

        testName = "earliest";
        strategy = ExtResetStrategy.parseFrom(testName);
        assertEquals(testName, strategy.getStrategyName());

        testName = "randomName";
        strategy = ExtResetStrategy.parseFrom(testName);
        assertEquals(ExtResetStrategy.RESET_NONE, strategy);

        testName = "timestamp";
        strategy = ExtResetStrategy.parseFrom(testName);
        assertEquals(ExtResetStrategy.RESET_FROM_TIMESTAMP, strategy);
    }

}