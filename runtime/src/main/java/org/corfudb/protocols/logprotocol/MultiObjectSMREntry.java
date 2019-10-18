package org.corfudb.protocols.logprotocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.util.serializer.Serializers;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkState;


/**
 * A log entry structure which contains a collection of multiSMRentries,
 * each one contains a list of updates for one object.
 */
@SuppressWarnings("checkstyle:abbreviation")
@ToString
@Slf4j
public class MultiObjectSMREntry extends LogEntry implements ISMRConsumable {

    // map from stream-ID to a list of updates encapsulated as MultiSMREntry
    private Map<UUID, MultiSMREntry> streamUpdates = new HashMap<>();

    /**
     *
     */
    private final Map<UUID, byte[]> streamBuffers = new HashMap<>();

    public MultiObjectSMREntry() {
        this.type = LogEntryType.MULTIOBJSMR;
    }

    /**
     * Add one SMR-update to one object's update-list.
     *
     * @param streamID    StreamID
     * @param updateEntry SMREntry to add
     */
    public synchronized void addTo(UUID streamID, SMREntry updateEntry) {
        checkState(streamBuffers.isEmpty());
        MultiSMREntry multiSMREntry = streamUpdates.computeIfAbsent(streamID, k -> new MultiSMREntry());
        multiSMREntry.addTo(updateEntry);
    }

    /**
     * merge two MultiObjectSMREntry records.
     * merging is done object-by-object
     *
     * @param other Object to merge.
     */
    public synchronized void mergeInto(MultiObjectSMREntry other) {
        checkState(streamBuffers.isEmpty());

        if (other == null) {
            return;
        }

        other.getEntryMap().forEach((otherStreamID, otherMultiSmrEntry) -> {
            MultiSMREntry multiSMREntry = streamUpdates.computeIfAbsent(otherStreamID, k -> new MultiSMREntry());
            multiSMREntry.mergeInto(otherMultiSmrEntry);
        });
    }

    /**
     * This function provides the remaining buffer.
     *
     * @param b The remaining buffer.
     */
    @Override
    public synchronized void deserializeBuffer(ByteBuf b, CorfuRuntime rt) {
        super.deserializeBuffer(b, rt);
        int numStreams = b.readInt();
        for (int i = 0; i < numStreams; i++) {
            UUID streamId = new UUID(b.readLong(), b.readLong());

            int updateLength = b.readInt();
            byte[] streamUpdates = new byte[updateLength];
            b.readBytes(streamUpdates);
            streamBuffers.put(streamId, streamUpdates);
        }
    }

    @Override
    public synchronized void serialize(ByteBuf b) {
        super.serialize(b);
        b.writeInt(streamUpdates.size());
        streamUpdates.entrySet().stream()
                .forEach(x -> {
                    b.writeLong(x.getKey().getMostSignificantBits());
                    b.writeLong(x.getKey().getLeastSignificantBits());

                    int payloadSizeIndex = b.writerIndex();
                    b.writeInt(0);
                    int payloadIndex = b.writerIndex();
                    b.writeInt(x.getValue().getUpdates().size());
                    x.getValue().getUpdates().stream().forEach(smrEntry -> Serializers.CORFU.serialize(smrEntry, b));
                    int length = b.writerIndex() - payloadIndex;
                    b.writerIndex(payloadSizeIndex);
                    b.writeInt(length);
                    b.writerIndex(payloadIndex + length);
                });
    }

    /**
     * Get the list of SMR updates for a particular object.
     *
     * @param id StreamID
     * @return an empty list if object has no updates; a list of updates if exists
     */
    @Override
    public synchronized List<SMREntry> getSMRUpdates(UUID id) {
        MultiSMREntry entry = streamUpdates.get(id);

        if (entry != null) {
            return entry.getUpdates();
        }

        if (streamBuffers.containsKey(id)) {
            byte[] streamUpdatesBuf = streamBuffers.get(id);
            ByteBuf buf = Unpooled.wrappedBuffer(streamUpdatesBuf);
            int numUpdates = buf.readInt();
            List<SMREntry> smrEntries = new ArrayList<>(numUpdates);
            for (int update = 0; update < numUpdates; update++) {
                smrEntries.add((SMREntry) Serializers.CORFU.deserialize(buf, null));
            }

            smrEntries.stream().forEach(x -> x.setGlobalAddress(getGlobalAddress()));

            streamUpdates.put(id, new MultiSMREntry(smrEntries));
            streamBuffers.remove(id);
            return smrEntries;
        }

        return Collections.emptyList();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void setGlobalAddress(long address) {
        super.setGlobalAddress(address);
        streamUpdates.values().forEach(x -> x.setGlobalAddress(address));
    }

    public synchronized Map<UUID, MultiSMREntry> getEntryMap() {
        // Deserialize all streams
        for (UUID id : new HashSet<>(streamBuffers.keySet())) {
            getSMRUpdates(id);
        }

        return this.streamUpdates;
    }
}
