package org.corfudb.protocols.wireprotocol.statetransfer;

import lombok.Getter;

import java.nio.ByteBuffer;


public abstract class StateTransferBaseRequest implements Request {
    @Getter
    private final long addressStart;

    @Getter
    private final long addressEnd;

    public StateTransferBaseRequest(long addressStart, long addressEnd){
        this.addressStart = addressStart;
        this.addressEnd = addressEnd;
    }

    public StateTransferBaseRequest(byte [] buf){
        ByteBuffer bytes = ByteBuffer.wrap(buf);
        this.addressStart = bytes.getLong();
        this.addressEnd = bytes.getLong();
    }

    /**
     * Returns the type of the request.
     *
     * @return type of request
     */
    abstract StateTransferRequestType getRequestType();

    @Override
    public byte[] getSerialized() {
        ByteBuffer buf = ByteBuffer.allocate(Long.BYTES * 2);
        buf.putLong(addressStart);
        buf.putLong(addressEnd);
        return buf.array();
    }
}
