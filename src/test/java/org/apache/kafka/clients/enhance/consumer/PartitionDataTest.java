package org.apache.kafka.clients.enhance.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class PartitionDataTest {
    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void getLastPutMessageTimestamp() throws Exception {
    }

    @Test
    public void getLastConsumedTimestamp() throws Exception {
    }


    private List<ConsumerRecord<String, String>> produceRecords() {

        ConsumerRecord<String, String> record = new ConsumerRecord<String, String>("test", 0, 1, "test_key", "test_val");
        return null;

    }

    @Test
    public void putRecords() throws Exception {

    }

    @Test
    public void removeRecords() throws Exception {
    }

    @Test
    public void takeRecords() throws Exception {
    }

    @Test
    public void getTotalTakeMessageNum() throws Exception {
    }

    @Test
    public void getMinOffset() throws Exception {
    }

    @Test
    public void getMaxOffset() throws Exception {
    }

    @Test
    public void getOffsetSpan() throws Exception {
    }

    @Test
    public void getLastAckOffset() throws Exception {
    }

    @Test
    public void getWinSize() throws Exception {
    }

    @Test
    public void clear() throws Exception {
    }

    @Test
    public void resetPartition() throws Exception {
    }

}