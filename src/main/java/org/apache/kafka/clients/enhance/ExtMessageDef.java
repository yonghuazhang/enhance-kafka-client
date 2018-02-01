package org.apache.kafka.clients.enhance;

import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by steven03.zhang on 2017/12/11.
 */
public class ExtMessageDef {
    private static final Logger logger = LoggerFactory.getLogger(ExtMessageDef.class);

    public static final String RETRY_TOPIC_PREFFIX = "RETRY.";
    public static final String DLQ_TOPIC_PREFFIX = "DLQ.";
    public static final Charset STRING_ENCODE = Charset.forName("UTF-8");
    public static final String STRING_EMPTY = "";
    public static final String PROPERTY_KEYS = "KEYS";
    public static final String PROPERTY_TAGS = "TAGS";
    public static final String PROPERTY_SEP = "|";
    public static final String PROPERTY_DELAY_TIME_LEVEL = "DELAY_LEVEL";
    public static final String PROPERTY_RETRY_TOPIC = "RETRY_TOPIC";
    public static final String PROPERTY_REAL_TOPIC = "REAL_TOPIC";
    public static final String PROPERTY_REAL_QUEUE_ID = "REAL_QID";
    public static final String PROPERTY_TRANSACTION_PREPARED = "TRAN_MSG";

    public static final String PROPERTY_MIN_OFFSET = "MIN_OFFSET";
    public static final String PROPERTY_MAX_OFFSET = "MAX_OFFSET";
    public static final String PROPERTY_BUYER_ID = "BUYER_ID";
    public static final String PROPERTY_ORIGIN_MESSAGE_ID = "ORIGIN_MESSAGE_ID";
    public static final String PROPERTY_RECONSUME_TIME = "RECONSUME_TIME";
    public static final String PROPERTY_MSG_REGION = "MSG_REGION";
    public static final String PROPERTY_TRACE_SWITCH = "TRACE_ON";
    public static final String PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX = "UNIQ_KEY";

    public static final int MAX_RECONSUME_TIMES = 16;
    public static final int MAX_DELAY_TIME_LEVEL = MAX_RECONSUME_TIMES;

    public final static String MESSAGE_BODY_FIELD = "FLD_BODY_CONTENT";
    public final static String MESSAGE_PROPS_FIELD = "FLD_PROPS_CONTENT";
    public final static String MESSAGE_PROPS_KEY = "PROPS_KEY";
    public final static String MESSAGE_PROPS_VAL = "PROPS_VAL";
    public final static long INVALID_OFFSET_VALUE = -1L;

    public static final HashSet<String> STRING_HASH_SET = new HashSet<String>();

    static {
        STRING_HASH_SET.add(PROPERTY_TRACE_SWITCH);
        STRING_HASH_SET.add(PROPERTY_MSG_REGION);
        STRING_HASH_SET.add(PROPERTY_KEYS);
        STRING_HASH_SET.add(PROPERTY_TAGS);
        STRING_HASH_SET.add(PROPERTY_DELAY_TIME_LEVEL);
        STRING_HASH_SET.add(PROPERTY_RETRY_TOPIC);
        STRING_HASH_SET.add(PROPERTY_REAL_TOPIC);
        STRING_HASH_SET.add(PROPERTY_REAL_QUEUE_ID);
        STRING_HASH_SET.add(PROPERTY_TRANSACTION_PREPARED);
        STRING_HASH_SET.add(PROPERTY_MIN_OFFSET);
        STRING_HASH_SET.add(PROPERTY_MAX_OFFSET);
        STRING_HASH_SET.add(PROPERTY_BUYER_ID);
        STRING_HASH_SET.add(PROPERTY_ORIGIN_MESSAGE_ID);
        STRING_HASH_SET.add(PROPERTY_RECONSUME_TIME);
        STRING_HASH_SET.add(PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
    }


    public static String validateTagOrKey(String tagOrKey) {
        if (tagOrKey.contains(PROPERTY_SEP)) {
            logger.warn("Tags or Keys [ {} ] includes invalid character '|', will be replaced by empty char.", tagOrKey);
            return tagOrKey.replaceAll(PROPERTY_SEP, STRING_EMPTY);
        }
        return tagOrKey;
    }

    public static Collection<String> validateTagsOrKeys(Collection<String> tagsOrKeys) {
        Set<String> tmpSet = new HashSet<>();
        for (String tagOrKey : tagsOrKeys) {
            tmpSet.add(validateTagOrKey(tagOrKey));
        }
        return tmpSet;
    }

    public static String joinTagsOrKeys(Collection<String> tagsOrKeys) {
        Collection<String> tmpTagsOrKeys = validateTagsOrKeys(tagsOrKeys);
        return Utils.join(tmpTagsOrKeys, ExtMessageDef.PROPERTY_SEP);
    }

    public static Collection<String> splitTagsOrKeys(String tagsOrKeys) {
        if (null == tagsOrKeys || tagsOrKeys.isEmpty()) {
            return Collections.emptyList();
        }
        if (!tagsOrKeys.contains(PROPERTY_SEP)) {
            return Collections.singletonList(tagsOrKeys);
        }
        return Arrays.asList(tagsOrKeys.split(PROPERTY_SEP, 0));
    }
}
