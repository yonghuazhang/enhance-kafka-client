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

import static org.apache.kafka.clients.enhance.ExtMessageDef.EXT_MESSAGE_ATTR_FIELD;
import static org.apache.kafka.clients.enhance.ExtMessageDef.EXT_MESSAGE_ATTR_KEY;
import static org.apache.kafka.clients.enhance.ExtMessageDef.EXT_MESSAGE_ATTR_VAL;
import static org.apache.kafka.clients.enhance.ExtMessageDef.EXT_MESSAGE_BODY_FIELD;
import static org.apache.kafka.clients.enhance.ExtMessageDef.EXT_MESSAGE_DELAY_LEVEL_FIELD;
import static org.apache.kafka.clients.enhance.ExtMessageDef.EXT_MESSAGE_RETRY_COUNT_FIELD;


public class ExtMessageEncoder<K> implements Serializer<ExtMessage<K>>, Deserializer<ExtMessage<K>> {
    private static final Logger logger = LoggerFactory.getLogger(ExtMessageEncoder.class);
    private final static Schema EXT_MESSAGE_SCHEMA = new Schema(new Field(EXT_MESSAGE_RETRY_COUNT_FIELD, Type.INT8),
            new Field(EXT_MESSAGE_DELAY_LEVEL_FIELD, Type.INT8),
            new Field(EXT_MESSAGE_BODY_FIELD, Type.BYTES),
            new Field(EXT_MESSAGE_ATTR_FIELD, new ArrayOf(new Schema(new Field(EXT_MESSAGE_ATTR_KEY, Type.NULLABLE_STRING),
                    new Field(EXT_MESSAGE_ATTR_VAL, Type.NULLABLE_STRING)))));

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public ExtMessage<K> deserialize(String topic, byte[] data) {
        ExtMessage<K> extMessage = new ExtMessage<>();
        extMessage.setTopic(topic);
        try {
            Struct mesgStruct = EXT_MESSAGE_SCHEMA.read(ByteBuffer.wrap(data));
            extMessage.setRetryCount((byte)mesgStruct.get(EXT_MESSAGE_RETRY_COUNT_FIELD));
            extMessage.setDelayedLevel((byte) mesgStruct.get(EXT_MESSAGE_DELAY_LEVEL_FIELD));
            ByteBuffer bodyBuffer = (ByteBuffer) mesgStruct.get(EXT_MESSAGE_BODY_FIELD);
            extMessage.setBody(bodyBuffer.array());

            for (Object objProp : mesgStruct.getArray(EXT_MESSAGE_ATTR_FIELD)) {
                Struct pStruct = (Struct) objProp;
                String key = pStruct.getString(EXT_MESSAGE_ATTR_KEY);
                String val = pStruct.getString(EXT_MESSAGE_ATTR_VAL);
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
        Struct msgStruct = new Struct(EXT_MESSAGE_SCHEMA);

        msgStruct.set(EXT_MESSAGE_RETRY_COUNT_FIELD, (byte)data.getRetryCount());
        msgStruct.set(EXT_MESSAGE_DELAY_LEVEL_FIELD, (byte)data.getDelayedLevel());
        msgStruct.set(EXT_MESSAGE_BODY_FIELD, ByteBuffer.wrap(data.getBody()));

        List<Struct> propStructList = new ArrayList<>();
        for (Map.Entry<String, String> item : data.getProperties().entrySet()) {
            Struct propStruct = msgStruct.instance(EXT_MESSAGE_ATTR_FIELD);
            propStruct.set(EXT_MESSAGE_ATTR_KEY, item.getKey());
            propStruct.set(EXT_MESSAGE_ATTR_VAL, item.getValue());
            propStructList.add(propStruct);
        }

        msgStruct.set(EXT_MESSAGE_ATTR_FIELD, propStructList.toArray());
        int byteSize = msgStruct.sizeOf();

        //use bufferpool to improve allocate performance, and reduce jvm gc.
        ByteBuffer buffer = ByteBuffer.allocate(byteSize);
        msgStruct.writeTo(buffer);
        buffer.rewind();
        return buffer.array();
    }

    @Override
    public void close() {

    }
}
