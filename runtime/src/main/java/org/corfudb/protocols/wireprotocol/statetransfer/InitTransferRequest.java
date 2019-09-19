package org.corfudb.protocols.wireprotocol.statetransfer;

import lombok.Getter;

import java.nio.ByteBuffer;

import static org.corfudb.protocols.wireprotocol.statetransfer.StateTransferRequestType.*;

public class InitTransferRequest implements Request{

    @Getter
    private final long addressStart;

    @Getter
    private final long addressEnd;

    public InitTransferRequest(long addressStart, long addressEnd){
        this.addressStart = addressStart;
        this.addressEnd = addressEnd;
    }

    public InitTransferRequest(byte [] buf){
        ByteBuffer bytes = ByteBuffer.wrap(buf);
        this.addressStart = bytes.getLong();
        this.addressEnd = bytes.getLong();
    }

    @Override
    public StateTransferRequestType getRequestType() {
        return INIT_TRANSFER;
    }

    @Override
    public byte[] getSerialized() {
        ByteBuffer buf = ByteBuffer.allocate(Long.BYTES * 2);
        buf.putLong(addressStart);
        buf.putLong(addressEnd);
        return buf.array();
    }

}
