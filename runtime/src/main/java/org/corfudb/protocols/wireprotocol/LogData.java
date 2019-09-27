package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.logprotocol.CheckpointEntry;
import org.corfudb.protocols.logprotocol.LogEntry;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.WriteSizeException;
import org.corfudb.util.serializer.Serializers;

import java.util.EnumMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by mwei on 8/15/16.
 */
@Slf4j
public class LogData implements ICorfuPayload<LogData>, IMetadata, ILogData {

    public final static LogData EMPTY = new LogData(DataType.EMPTY);

    public static final int NOT_KNOWN = -1;

    @Getter
    final DataType type;

    @Getter
    byte[] data;

    private ByteBuf serializedCache = null;

    private int lastKnownSize = NOT_KNOWN;

    private final transient AtomicReference<Object> payload = new AtomicReference<>();

    public static LogData getHole(long address) {
        LogData logData = new LogData(DataType.HOLE);
        logData.setGlobalAddress(address);
        return logData;
    }

    public static LogData getHole(Token token) {
        LogData logData = new LogData(DataType.HOLE);
        logData.useToken(token);
        return logData;
    }

    public static LogData getEmpty(long address) {
        LogData logData = new LogData(DataType.EMPTY);
        logData.setGlobalAddress(address);
        return logData;
    }

    public static LogData getCompacted(long address) {
        LogData logData = new LogData(DataType.COMPACTED);
        logData.setGlobalAddress(address);
        return logData;
    }

    /**
     * Return the payload.
     */
    public Object getPayload(CorfuRuntime runtime) {
        Object value = payload.get();
        if (value == null) {
            synchronized (this.payload) {
                value = this.payload.get();
                if (value == null) {
                    if (data == null) {
                        this.payload.set(null);
                    } else {
                        ByteBuf copyBuf = Unpooled.wrappedBuffer(data);
                        final Object actualValue =
                                Serializers.CORFU.deserialize(copyBuf, runtime);
                        if (actualValue instanceof LogEntry) {
                            ((LogEntry) actualValue).setGlobalAddress(getGlobalAddress());
                            ((LogEntry) actualValue).setRuntime(runtime);
                        }
                        value = actualValue == null ? this.payload : actualValue;
                        this.payload.set(value);
                        copyBuf.release();
                        lastKnownSize = data.length;
                        data = null;
                    }
                }
            }
        }

        return value;
    }

    /**
     * Serialize the payload. NOT thread-safe.
     *
     * @param buf the buffer to serialize to
     */
    public void serializePayload(ByteBuf buf) {
        if (payload.get() == null) {
            return;
        }
        Serializers.CORFU.serialize(payload.get(), buf);
    }

    @Override
    public synchronized void releaseBuffer() {
        if (serializedCache != null) {
            serializedCache.release();
            if (serializedCache.refCnt() == 0) {
                serializedCache = null;
            }
        }
    }

    @Override
    public synchronized void acquireBuffer() {
        if (serializedCache == null) {
            serializedCache = Unpooled.buffer();
            doSerializeInternal(serializedCache);
            lastKnownSize = serializedCache.array().length;
        } else {
            serializedCache.retain();
        }
    }

    @Override
    public int getSizeEstimate() {
        byte[] tempData = data;
        if (tempData != null) {
            return tempData.length;
        } else if (lastKnownSize != NOT_KNOWN) {
            return lastKnownSize;
        }
        log.warn("getSizeEstimate: LogData size estimate is defaulting to 1,"
                + " this might cause leaks in the cache!");
        return 1;
    }

    @Getter
    final EnumMap<LogUnitMetadataType, Object> metadataMap;

    /**
     * Return the payload.
     */
    public LogData(ByteBuf buf) {
        type = ICorfuPayload.fromBuffer(buf, DataType.class);
        if (type == DataType.DATA || type == DataType.GARBAGE) {
            data = ICorfuPayload.fromBuffer(buf, byte[].class);
        } else {
            data = null;
        }
        if (type.isMetadataAware()) {
            metadataMap = ICorfuPayload.enumMapFromBuffer(buf, IMetadata.LogUnitMetadataType.class);
        } else {
            metadataMap = new EnumMap<>(IMetadata.LogUnitMetadataType.class);
        }
    }

    /**
     * Constructor for generating LogData.
     *
     * @param type The type of log data to instantiate.
     */
    public LogData(DataType type) {
        this.type = type;
        this.data = null;
        this.metadataMap = new EnumMap<>(IMetadata.LogUnitMetadataType.class);
    }

    /**
     * Constructor for generating LogData.
     *
     * @param type   The type of log data to instantiate.
     * @param object The actual data/value
     */
    public LogData(DataType type, final Object object) {
        if (object instanceof ByteBuf) {
            this.type = type;
            this.data = byteArrayFromBuf((ByteBuf) object);
            this.metadataMap = new EnumMap<>(IMetadata.LogUnitMetadataType.class);
        } else {
            this.type = type;
            this.data = null;
            this.payload.set(object);
            this.metadataMap = new EnumMap<>(IMetadata.LogUnitMetadataType.class);
            if (object instanceof CheckpointEntry) {
                CheckpointEntry cp = (CheckpointEntry) object;
                setCheckpointType(cp.getCpType());
                setCheckpointId(cp.getCheckpointId());
                setCheckpointedStreamId(cp.getStreamId());
                setCheckpointedStreamStartLogAddress(
                        Long.parseLong(cp.getDict()
                                .get(CheckpointEntry.CheckpointDictKey.START_LOG_ADDRESS)));
            }
        }
    }

    /**
     * Assign a given token to this log data.
     *
     * @param token the token to use
     */
    @Override
    public void useToken(IToken token) {
        setGlobalAddress(token.getSequence());
        setEpoch(token.getEpoch());
        if (token.getBackpointerMap().size() > 0) {
            setBackpointerMap(token.getBackpointerMap());
        }
        if (payload.get() instanceof LogEntry) {
            ((LogEntry) payload.get()).setGlobalAddress(token.getSequence());
        }
    }

    /**
     * Reset the payload, assuming data type not changed.
     *
     * @param newPayload new payload to reset.
     */
    public void resetPayload(Object newPayload) {
        data = null;
        payload.set(newPayload);
        serializedCache = null;
        lastKnownSize = NOT_KNOWN;
    }

    /**
     * Return a byte array from buffer.
     *
     * @param buf The buffer to read from
     */
    public byte[] byteArrayFromBuf(final ByteBuf buf) {
        ByteBuf readOnlyCopy = buf.asReadOnly();
        readOnlyCopy.resetReaderIndex();
        byte[] outArray = new byte[readOnlyCopy.readableBytes()];
        readOnlyCopy.readBytes(outArray);
        return outArray;
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        if (serializedCache != null) {
            serializedCache.resetReaderIndex();
            buf.writeBytes(serializedCache);
        } else {
            doSerializeInternal(buf);
        }
    }

    private void doSerializeInternal(ByteBuf buf) {
        ICorfuPayload.serialize(buf, type);
        if (type == DataType.DATA || type == DataType.GARBAGE) {
            if (data == null) {
                int lengthIndex = buf.writerIndex();
                buf.writeInt(0);
                serializePayload(buf);
                int size = buf.writerIndex() - (lengthIndex + 4);
                buf.writerIndex(lengthIndex);
                buf.writeInt(size);
                buf.writerIndex(lengthIndex + size + 4);
            } else {
                ICorfuPayload.serialize(buf, data);
            }
        }
        if (type.isMetadataAware()) {
            ICorfuPayload.serialize(buf, metadataMap);
        }
    }

    /**
     * LogData are considered equals if clientId and threadId are equal.
     * Here, it means or both of them are null or both of them are the same.
     */
    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        } else if (!(o instanceof LogData)) {
            return false;
        } else {
            LogData other = (LogData) o;
            if (compareTo(other) == 0) {
                boolean sameClientId = getClientId() == null ? other.getClientId() == null :
                        getClientId().equals(other.getClientId());
                boolean sameThreadId = getThreadId() == null ? other.getThreadId() == null :
                        getThreadId().equals(other.getThreadId());

                return sameClientId && sameThreadId;
            }

            return false;
        }
    }

    @Override
    public String toString() {
        return String.format("LogData.%s[%d]", type, getGlobalAddress());
    }

    /**
     * Verify that max payload is enforced for the specified limit.
     *
     * @param limit Max write limit.
     */
    public void checkMaxWriteSize(int limit) {
        try (ILogData.SerializationHandle sh = this.getSerializedForm()) {
            if (limit != 0 && getSizeEstimate() > limit) {
                throw new WriteSizeException(getSizeEstimate(), limit);
            }
        }
    }
}
