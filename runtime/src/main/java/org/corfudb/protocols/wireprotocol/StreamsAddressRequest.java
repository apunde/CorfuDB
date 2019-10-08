package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.NonNull;

import java.util.Collections;
import java.util.List;
import java.util.UUID;

/**
 * Represents the request sent to the sequencer to retrieve one or several streams address map.
 *
 * Created by annym on 02/06/2019
 */
@Data
@AllArgsConstructor
@CorfuPayload
public class StreamsAddressRequest implements ICorfuPayload<StreamsAddressRequest>{

    // To request specific streams
    public static final byte STREAMS = 0;
    // To request all streams
    public static final byte ALL_STREAMS = 1;

    public static final StreamsAddressRequest TOTAL = new StreamsAddressRequest();

    // The type of request, one of the above.
    final byte reqType;

    // To request address space only before compaction mark
    final boolean beforeCompactionMark;

    private final List<StreamAddressRange> streamsRanges;

    public StreamsAddressRequest() {
        this.reqType = ALL_STREAMS;
        this.streamsRanges = Collections.EMPTY_LIST;
        beforeCompactionMark = false;
    }

    public StreamsAddressRequest(@NonNull List<StreamAddressRange> streamsRanges, boolean beforeCompactionMark) {
        this.reqType = STREAMS;
        this.streamsRanges = streamsRanges;
        this.beforeCompactionMark = beforeCompactionMark;
    }

    public StreamsAddressRequest(@NonNull List<StreamAddressRange> streamsRanges) {
        this(streamsRanges, false);
    }


    public StreamsAddressRequest(Byte reqType) {
        this.reqType = reqType;
        this.streamsRanges = Collections.EMPTY_LIST;
        this.beforeCompactionMark = false;
    }

    /**
     * Deserialization Constructor from Bytebuf to StreamsAddressRequest.
     *
     * @param buf The buffer to deserialize
     */
    public StreamsAddressRequest(ByteBuf buf) {
        this.reqType = ICorfuPayload.fromBuffer(buf, Byte.class);
        this.beforeCompactionMark = ICorfuPayload.fromBuffer(buf, Boolean.class);
        if (reqType == STREAMS) {
            this.streamsRanges = ICorfuPayload.listFromBuffer(buf, StreamAddressRange.class);
        } else {
            this.streamsRanges = Collections.EMPTY_LIST;
        }
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, reqType);
        ICorfuPayload.serialize(buf, beforeCompactionMark);
        if (reqType == STREAMS) {
            ICorfuPayload.serialize(buf, streamsRanges);
        }
    }
}
