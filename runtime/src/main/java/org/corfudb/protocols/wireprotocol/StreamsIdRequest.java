package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;

/**
 * Represents the request sent to the sequencer to retrieve stream id of one or several streams.
 */
public class StreamsIdRequest extends StreamsAddressRequest{
    public StreamsIdRequest() {
        super();
    }
    public StreamsIdRequest(Byte reqType) {
        super(reqType);
    }
}
