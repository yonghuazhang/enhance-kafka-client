package org.apache.kafka.clients.enhance;

import org.apache.kafka.common.protocol.types.ArrayOf;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.protocol.types.Type;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.clients.enhance.ExtMessageDef.MESSAGE_BODY_FIELD;
import static org.apache.kafka.clients.enhance.ExtMessageDef.MESSAGE_ATTR_FIELD;
import static org.apache.kafka.clients.enhance.ExtMessageDef.MESSAGE_ATTR_KEY;
import static org.apache.kafka.clients.enhance.ExtMessageDef.MESSAGE_ATTR_VAL;
import static org.apache.kafka.clients.enhance.ExtMessageDef.PROPERTY_DELAY_TIME_LEVEL;
import static org.apache.kafka.clients.enhance.ExtMessageDef.PROPERTY_RECONSUME_TIME;


/**
 * Created by steven03.zhang on 2017/12/12.
 */
public class ExtMessageEncoder<K> implements Serializer<ExtMessage<K>>, Deserializer<ExtMessage<K>> {
    private static final Logger logger = LoggerFactory.getLogger(ExtMessageEncoder.class);
    private static final Schema MESSAGE_PROP_KV = new Schema(new Field(MESSAGE_ATTR_KEY, Type.NULLABLE_STRING),
            new Field(MESSAGE_ATTR_VAL, Type.NULLABLE_STRING));
    private final static Schema MESSAGE_SCHEMA = new Schema(new Field(PROPERTY_RECONSUME_TIME, Type.INT8),
            new Field(PROPERTY_DELAY_TIME_LEVEL, Type.INT8),
            new Field(MESSAGE_BODY_FIELD, Type.BYTES),
            new Field(MESSAGE_ATTR_FIELD, new ArrayOf(MESSAGE_PROP_KV)));

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public ExtMessage<K> deserialize(String topic, byte[] data) {
        ExtMessage<K> extMessage = new ExtMessage<>();
        extMessage.setTopic(topic);
        try {
            Struct mesgStruct = MESSAGE_SCHEMA.read(ByteBuffer.wrap(data));
            extMessage.setRetryCount((int) mesgStruct.get(PROPERTY_RECONSUME_TIME));
            extMessage.setDelayedLevel((int) mesgStruct.get(PROPERTY_DELAY_TIME_LEVEL));
            ByteBuffer bodyBuffer = (ByteBuffer) mesgStruct.get(MESSAGE_BODY_FIELD);
            extMessage.setBody(bodyBuffer.array());

            for (Object objProp : mesgStruct.getArray(MESSAGE_ATTR_FIELD)) {
                Struct pStruct = (Struct) objProp;
                String key = pStruct.getString(MESSAGE_ATTR_KEY);
                String val = pStruct.getString(MESSAGE_ATTR_VAL);
                extMessage.addProperty(key, val);
            }
        } catch (Exception ex) {
            logger.warn("message format isn't the format of ExtMessage.");
            extMessage.setBody(data);
        }
        return extMessage;
    }

    @Override
    public byte[] serialize(String topic, ExtMessage<K> data) {
        Struct mesgStruct = new Struct(MESSAGE_SCHEMA);

        mesgStruct.set(PROPERTY_RECONSUME_TIME, data.getRetryCount());
        mesgStruct.set(PROPERTY_DELAY_TIME_LEVEL, data.getDelayedLevel());
        mesgStruct.set(MESSAGE_BODY_FIELD, ByteBuffer.wrap(data.getBody()));

        List<Struct> propStructList = new ArrayList<>();
        for (Map.Entry<String, String> item : data.getProperties().entrySet()) {
            Struct propStruct = mesgStruct.instance(MESSAGE_ATTR_FIELD);
            propStruct.set(MESSAGE_ATTR_KEY, item.getKey());
            propStruct.set(MESSAGE_ATTR_VAL, item.getValue());
            propStructList.add(propStruct);
        }

        mesgStruct.set(MESSAGE_ATTR_FIELD, propStructList);
        int byteSize = mesgStruct.sizeOf();

        //use bufferpool to improve allocate performance, and reduce jvm gc.
        ByteBuffer buffer = ByteBuffer.allocate(byteSize);
        mesgStruct.writeTo(buffer);
        return buffer.array();
    }

    @Override
    public void close() {

    }
}
