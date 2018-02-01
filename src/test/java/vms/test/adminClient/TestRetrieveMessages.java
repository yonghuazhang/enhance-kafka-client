package vms.test.adminClient;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Created by steven03.zhang on 2017/9/2.
 */
public class TestRetrieveMessages {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "10.198.195.144:9092");
        props.put(AdminClientConfig.RETRIES_CONFIG, 5);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "1000000");
      /*   props.put("group.id", "Zyh");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props = ConsumerConfig.addDeserializerToConfig(props, new StringDeserializer(), new StringDeserializer());

        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Collections.singleton("test"));
        int i =0;
        while(i++ < 100) {
            ConsumerRecords<String, String> aa = consumer.poll(2000);
            for(ConsumerRecord a : aa) {
                System.out.printf("%s ---> %s\n", a.key(), a.value());
            }
        }
*/
        AdminClient kafkaAdminClient = KafkaAdminClient.create(props);
        Deserializer decoder = new StringDeserializer();
        RetrieveRecordsOptions options = new RetrieveRecordsOptions().timeoutMs(Integer.valueOf(3000));
        DescribeTopicsResult topicsResult = kafkaAdminClient.describeTopics(Collections.singleton("test"));
        Map<String, TopicDescription> td = topicsResult.all().get();
      /*  int i = 0;
        while(i++ < 1000) {

            try {
                DescribeTopicsResult result = kafkaAdminClient.describeTopics(Collections.singleton("test"), new DescribeTopicsOptions().timeoutMs(Integer.valueOf(1000)));
                TopicDescription td1 = result.values().get("test").get();
                System.out.printf("%s  ->  %d\n", td1.name(), td1.partitions().size());

            } catch (Exception e) {
                System.out.println("---------->err.");
            }
        }*/
/*
        int i = 0;
        while(i++ < 100) {
            kafkaAdminClient.describeTopics(Collections.singleton("test"));
            kafkaAdminClient.describeTopics(Collections.singleton("test"));
            kafkaAdminClient.describeTopics(Collections.singleton("test"));
            kafkaAdminClient.describeTopics(Collections.singleton("test"));
            TimeUnit.SECONDS.sleep(1);
        }
*/


        RetrieveRecordsResult<String, String> recs = kafkaAdminClient.retrieveMessagesByOffset(new TopicPartition("test", 0), 120, 120, decoder, decoder, options);
        ConsumerRecords<String, String> consumerRecords = null;
        try {
            consumerRecords = recs.values().get();
            for (ConsumerRecord irec : consumerRecords) {
                System.out.printf("%d = %s\n", irec.offset(), irec.value());
            }
        } catch (Exception e) {
            System.out.printf("-----------error------------>\n");
        }
        int i = 0;
 /*       while (i++ < 1000) {
            System.out.printf("----------------------->\n");
            try {
                recs = kafkaAdminClient.retrieveMessagesByOffset(new TopicPartition("test", 0), 120, 500, decoder, decoder, options);
                consumerRecords = recs.values().get();
                for (ConsumerRecord irec : consumerRecords) {
                    System.out.printf("%d = %s\n", irec.offset(), irec.value());
                }
            } catch (Exception e) {
                System.out.printf("-----------error------------>\n" + e.getMessage());
            }
        }*/


        while (i++ < 10) {
            System.out.printf("----------------------->\n");
            long bTime = System.currentTimeMillis() - 2 * 60 * 1000L;
            try {
                recs = kafkaAdminClient.retrieveMessagesByTimeSpan(new TopicPartition("test", 0), bTime, 1000, decoder, decoder, options);
                consumerRecords = recs.values().get();
                for (ConsumerRecord irec : consumerRecords) {
                    System.out.printf("%d = %s\n", irec.offset(), irec.value());
                }
            } catch (Exception e) {
                System.out.printf("-----------error------------>\n" + e.getMessage());
            }
        }


        kafkaAdminClient.close();
    }
}
