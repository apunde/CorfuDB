package org.corfudb.protocols.wireprotocol.statetransfer;

import io.netty.buffer.ByteBuf;
import lombok.Getter;
import org.corfudb.protocols.wireprotocol.ICorfuPayload;

public class StateTransferResponseMsg implements ICorfuPayload<StateTransferResponseMsg> {

    @Getter
    private final Response response;

    public StateTransferResponseMsg(Response response){
        this.response = response;
    }

    public StateTransferResponseMsg(ByteBuf buf){
        StateTransferResponseType responseType = StateTransferResponseType.typeMap.get(buf.readInt());
        byte[] bytes = new byte[buf.readInt()];
        buf.readBytes(bytes);
        response = responseType.getMapping().apply(bytes);
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        buf.writeInt(response.getResponseType().getResponseType());
        byte[] bytes = response.getSerialized();
        buf.writeInt(bytes.length);
        buf.writeBytes(bytes);
    }
}
