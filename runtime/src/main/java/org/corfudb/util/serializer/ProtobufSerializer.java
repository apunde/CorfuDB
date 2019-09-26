package org.corfudb.util.serializer;

import com.google.protobuf.Any;
import com.google.protobuf.Message;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata.Record;
import org.corfudb.runtime.collections.CorfuRecord;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ProtobufSerializer implements ISerializer {

    private final byte type;
    private final Map<String, Class<? extends Message>> classMap;

    public ProtobufSerializer(byte type, Map<String, Class<? extends Message>> classMap) {
        this.type = type;
        this.classMap = classMap;
    }

    private enum MessageType {
        KEY(1),
        VALUE(2);

        private static final Map<Integer, MessageType> valToTypeMap = new HashMap<>();

        static {
            for (MessageType type : MessageType.values()) {
                valToTypeMap.put(type.val, type);
            }
        }

        private final int val;

        MessageType(int val) {
            this.val = val;
        }

        public static MessageType valueOf(int val) {
            return valToTypeMap.get(val);
        }
    }

    @Override
    public byte getType() {
        return type;
    }

    /**
     * Deserialize an object from a given byte buffer.
     *
     * @param b The bytebuf to deserialize.
     * @return The deserialized object.
     */
    @Override
    public Object deserialize(ByteBuf b, CorfuRuntime rt) {

        try (ByteBufInputStream bbis = new ByteBufInputStream(b)) {
            MessageType type = MessageType.valueOf(bbis.readInt());
            int size = bbis.readInt();
            byte[] data = new byte[size];
            bbis.readFully(data);
            Record record = Record.parseFrom(data);
            Any payload = record.getPayload();
            Message value = payload.unpack(classMap.get(payload.getTypeUrl()));

            if (type.equals(MessageType.KEY)) {
                return value;
            } else {
                Message metadata = null;
                if (record.hasMetadata()) {
                    Any anyMetadata = record.getMetadata();
                    metadata = anyMetadata.unpack(classMap.get(anyMetadata.getTypeUrl()));
                }
                return new CorfuRecord(value, metadata);
            }
        } catch (IOException ie) {
            log.error("Exception during deserialization!", ie);
            throw new RuntimeException(ie);
        }
    }

    /**
     * Serialize an object into a given byte buffer.
     *
     * @param o The object to serialize.
     * @param b The bytebuf to serialize it into.
     */
    @Override
    public void serialize(Object o, ByteBuf b) {

        Record record;
        MessageType type;

        if (o instanceof CorfuRecord) {
            CorfuRecord corfuRecord = (CorfuRecord) o;
            Any message = Any.pack(corfuRecord.getPayload());
            Record.Builder recordBuilder = Record.newBuilder()
                    .setPayload(message);
            if (corfuRecord.getMetadata() != null) {
                Any metadata = Any.pack(corfuRecord.getMetadata());
                recordBuilder.setMetadata(metadata);
            }
            record = recordBuilder.build();
            type = MessageType.VALUE;
        } else {
            Any message = Any.pack(((Message) o));
            record = Record.newBuilder()
                    .setPayload(message)
                    .build();
            type = MessageType.KEY;
        }
        byte[] data = record.toByteArray();

        try (ByteBufOutputStream bbos = new ByteBufOutputStream(b)) {
            bbos.writeInt(type.val);
            bbos.writeInt(data.length);
            bbos.write(data);
        } catch (IOException ie) {
            log.error("Exception during serialization!", ie);
        }
    }
}
