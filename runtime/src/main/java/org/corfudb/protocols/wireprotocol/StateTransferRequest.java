package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;
import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * Request to initiate a state transfer from the start address up to the end address.
 */
@Getter
@AllArgsConstructor
public class StateTransferRequest implements ICorfuPayload<StateTransferRequest> {

    private final Long start;
    private final Long end;

    /**
     * Deserialization Constructor from Bytebuf to KnownAddressRequest.
     *
     * @param buf The buffer to deserialize
     */
    public StateTransferRequest(ByteBuf buf) {
        start = ICorfuPayload.fromBuffer(buf, Long.class);
        end = ICorfuPayload.fromBuffer(buf, Long.class);
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, start);
        ICorfuPayload.serialize(buf, end);
    }
}
