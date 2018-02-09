package org.apache.kafka.clients.enhance;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

public final class Utility {
    private static final Logger logger = LoggerFactory.getLogger(Utility.class);
    public static final SimpleDateFormat SIMPLE_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
    public static final long INVALID_TIMESTAMP = -1L;
    private static final Pattern NORMALIZE_TOPIC_NAME_PATTERN = Pattern.compile("[^a-zA-Z0-9\\.\\-\\_]+");
    private static final String TOPIC_JOIN_CHAR = "_";

    public static int getCpuCores() {
        return Runtime.getRuntime().availableProcessors();
    }

    public static boolean isInvalidString(String value) {
        return (null == value) || value.isEmpty() || value.trim().isEmpty();
    }

    public static long convertTimeByString(String dateString) throws ParseException {
        return Utility.SIMPLE_DATE_FORMAT.parse(dateString).getTime();
    }

    public static boolean fileExists(String fileName) {
        return Files.exists(new File(fileName).toPath());
    }

    public static File createFile(String fileName) throws IOException {
        File file = new File(fileName);
        if (!file.exists()) {
            if (!file.getParentFile().exists()) {
                file.getParentFile().mkdirs();
            }
            file.createNewFile();
        }
        return file;
    }

    public static boolean moveFile(String fromFn, String ToFn) {
        try {
            Path fromPath = (new File(fromFn)).toPath();
            Path toPath = (new File(ToFn)).toPath();
            Files.move(fromPath, toPath, REPLACE_EXISTING);
            return true;
        } catch (IOException e) {
            logger.warn("move file failed. due to ", e);
        }
        return false;
    }

    public static String normalizeTopicName(String topicName) {
        return NORMALIZE_TOPIC_NAME_PATTERN.matcher(topicName).replaceAll(TOPIC_JOIN_CHAR);
    }

    public static void sleep(long durationMs) {
        try {
            TimeUnit.MILLISECONDS.sleep(durationMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
