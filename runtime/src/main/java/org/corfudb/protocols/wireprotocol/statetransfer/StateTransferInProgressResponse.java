package org.corfudb.protocols.wireprotocol.statetransfer;

import lombok.Getter;

import java.nio.ByteBuffer;

import static org.corfudb.protocols.wireprotocol.statetransfer.StateTransferResponseType.TRANSFER_IN_PROGRESS;

public class StateTransferInProgressResponse extends StateTransferBaseResponse {

    @Getter
    private final long latestAddressTransferred;

    public StateTransferInProgressResponse(long latestAddressTransferred, long startAddress, long endAddress){
        super(startAddress, endAddress);
        this.latestAddressTransferred = latestAddressTransferred;
    }

    public StateTransferInProgressResponse(byte [] buf){
        super(buf);
        ByteBuffer bytes = ByteBuffer.wrap(buf);
        this.latestAddressTransferred = bytes.getLong();
    }

    @Override
    public StateTransferResponseType getResponseType() {
        return TRANSFER_IN_PROGRESS;
    }

    @Override
    public byte[] getSerialized() {
        ByteBuffer buf = ByteBuffer.allocate(Long.BYTES * 3);
        buf.putLong(addressStart);
        buf.putLong(addressEnd);
        buf.putLong(latestAddressTransferred);
        return buf.array();
    }


}
