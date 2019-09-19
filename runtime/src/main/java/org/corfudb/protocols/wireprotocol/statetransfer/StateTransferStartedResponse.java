package org.corfudb.protocols.wireprotocol.statetransfer;

import lombok.Getter;

import java.nio.ByteBuffer;

import static org.corfudb.protocols.wireprotocol.statetransfer.StateTransferResponseType.TRANSFER_STARTED;

public class StateTransferStartedResponse implements Response {
    @Getter
    private final long addressStart;

    @Getter
    private final long addressEnd;

    public StateTransferStartedResponse(long addressStart, long addressEnd){
        this.addressStart = addressStart;
        this.addressEnd = addressEnd;
    }

    public StateTransferStartedResponse(byte [] buf){
        ByteBuffer bytes = ByteBuffer.wrap(buf);
        this.addressStart = bytes.getLong();
        this.addressEnd = bytes.getLong();
    }
    @Override
    public StateTransferResponseType getResponseType() {
        return TRANSFER_STARTED;
    }

    @Override
    public byte[] getSerialized() {
        ByteBuffer buf = ByteBuffer.allocate(Long.BYTES * 2);
        buf.putLong(addressStart);
        buf.putLong(addressEnd);
        return buf.array();
    }
}
