package org.apache.kafka.clients.enhance.consumer;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.enhance.Utility;
import org.apache.kafka.clients.enhance.exception.ConsumeOffsetException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Utils;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * file format
 * topicName partitonId offset
 * topic1 0 120
 * topic1 1 1200
 * topic2 0 122
 * topic2 1 1222
 * topic2 2 12222
 * Created by steven03.zhang on 2018/2/6.
 */
public class OffsetFileStorage<K> extends AbstractOffsetStorage<K> {
    private static final Pattern OFFSET_FILE_STORE_PATTERN = Pattern.compile("^(?<topic>\\S+)\\s(?<partition>\\d+)\\s(?<offset>\\d+)$",
            Pattern.MULTILINE | Pattern.CASE_INSENSITIVE);
    private final String offsetFilePath;
    private final String offsetBkFilePath;

    protected OffsetFileStorage(EnhanceConsumer<K> safeConsumer, PartitionDataManager partitionDataManager, ConsumeClientContext clientContext) {
        super(safeConsumer, partitionDataManager, clientContext, LoadOffsetType.LOAD_FROM_LOCAL_FILE);
        this.offsetFilePath = localOffsetFileName();
        this.offsetBkFilePath = localOffsetBkFileName();
    }

    @Override
    public boolean load() throws ConsumeOffsetException {
        String fileContent = null;

        if (!Utility.fileExists(offsetFilePath) && !Utility.fileExists(offsetBkFilePath)) return false;

        try {
            fileContent = Utils.readFileAsString(offsetFilePath, StandardCharsets.UTF_8);
        } catch (IOException ex) {
            logger.info("Load offset store file [{}] failed, find backup file and reload. err=[{}]", offsetFilePath, ex);
            try {
                fileContent = Utils.readFileAsString(offsetBkFilePath, StandardCharsets.UTF_8);
            } catch (IOException e1) {
                logger.info("No offset store backup file [{}] be found.", offsetBkFilePath);
            }
        }
        if (null != fileContent && !fileContent.isEmpty()) {
            Matcher offsetMatcher = OFFSET_FILE_STORE_PATTERN.matcher(fileContent);
            synchronized (lock) {
                while (offsetMatcher.find()) {
                    try {
                        String topicName = offsetMatcher.group("topic");
                        int partitionId = Integer.parseInt(offsetMatcher.group("partition"));
                        long offset = Long.parseLong(offsetMatcher.group("offset"));
                        commitedOffsetSnapshot.put(new TopicPartition(topicName, partitionId), new OffsetAndMetadata(offset));
                    } catch (NumberFormatException ex) {
                        logger.warn("local offset format is wrong. the record is [{}]", offsetMatcher.group());
                    }
                }
            }
            return true;
        }
        return false;
    }

    @Override
    public void storeOffsetMeta(Map<TopicPartition, OffsetAndMetadata> ackOffsetMeta) throws ConsumeOffsetException {
        if (commitedOffsetSnapshot.isEmpty()) return;

        if (Utility.fileExists(offsetFilePath)) {
            Utility.moveFile(offsetFilePath, offsetBkFilePath);
        } else {
            try {
                Utility.createFile(offsetFilePath);
            } catch (IOException e) {
                logger.warn("create user offset store file failed. path = " + offsetFilePath);
            }
        }
        BufferedOutputStream bf = null;
        try {
            bf = new BufferedOutputStream(new FileOutputStream(offsetFilePath, true));
            bf.write("topicName partitonId offset\n".getBytes(StandardCharsets.UTF_8));
            synchronized (lock) {
                for (TopicPartition tp : commitedOffsetSnapshot.keySet()) {
                    OffsetAndMetadata offsetMeta = commitedOffsetSnapshot.get(tp);
                    bf.write((tp.topic() + " " + tp.partition() + " " + offsetMeta.offset() + "\n").getBytes(StandardCharsets.UTF_8));
                }
            }
        } catch (Exception e) {
            logger.warn("storeOffsetMeta error. du to ", e);
        } finally {
            if (null != bf) {
                try {
                    bf.close();
                } catch (IOException e) {
                    logger.warn("close offset file error. du to ", e);
                }
            }
        }
    }

}
