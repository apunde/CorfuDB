package org.corfudb.protocols.wireprotocol.statetransfer;

import lombok.Getter;

import java.nio.ByteBuffer;

public abstract class StateTransferBaseResponse implements Response {

    @Getter
    public final long addressStart;

    @Getter
    public final long addressEnd;

    public StateTransferBaseResponse(long addressStart, long addressEnd){
        this.addressStart = addressStart;
        this.addressEnd = addressEnd;
    }

    public StateTransferBaseResponse(byte [] buf){
        ByteBuffer bytes = ByteBuffer.wrap(buf);
        this.addressStart = bytes.getLong();
        this.addressEnd = bytes.getLong();
    }

    @Override
    public abstract StateTransferResponseType getResponseType();

    @Override
    public byte[] getSerialized(){
        ByteBuffer buf = ByteBuffer.allocate(Long.BYTES * 2);
        buf.putLong(addressStart);
        buf.putLong(addressEnd);
        return buf.array();
    }
}
