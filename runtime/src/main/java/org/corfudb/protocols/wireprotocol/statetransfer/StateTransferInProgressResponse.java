package org.corfudb.protocols.wireprotocol.statetransfer;

import lombok.Getter;

import java.nio.ByteBuffer;

import static org.corfudb.protocols.wireprotocol.statetransfer.StateTransferResponseType.TRANSFER_IN_PROGRESS;

public class StateTransferInProgressResponse implements Response {

    @Getter
    private final long latestAddressTransferred;

    public StateTransferInProgressResponse(long latestAddressTransferred){
        this.latestAddressTransferred = latestAddressTransferred;
    }

    public StateTransferInProgressResponse(byte [] buf){
        ByteBuffer bytes = ByteBuffer.wrap(buf);
        this.latestAddressTransferred = bytes.getLong();
    }

    @Override
    public StateTransferResponseType getResponseType() {
        return TRANSFER_IN_PROGRESS;
    }

    @Override
    public byte[] getSerialized() {
        ByteBuffer buf = ByteBuffer.allocate(Long.BYTES);
        buf.putLong(latestAddressTransferred);
        return buf.array();
    }


}
