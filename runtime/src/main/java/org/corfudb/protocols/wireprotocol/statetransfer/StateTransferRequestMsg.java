package org.corfudb.protocols.wireprotocol.statetransfer;

import io.netty.buffer.ByteBuf;
import lombok.Getter;
import org.corfudb.protocols.wireprotocol.ICorfuPayload;

public class StateTransferRequestMsg implements ICorfuPayload<StateTransferRequestMsg> {

    @Getter
    private final Request request;

    public StateTransferRequestMsg(Request request){
        this.request = request;
    }

    public StateTransferRequestMsg(ByteBuf buf){
        StateTransferRequestType stateTransferRequestType = StateTransferRequestType.typeMap.get(buf.readInt());
        byte[] bytes = new byte[buf.readInt()];
        buf.readBytes(bytes);
        request = stateTransferRequestType.getMapping().apply(bytes);
    }
    @Override
    public void doSerialize(ByteBuf buf) {
        buf.writeInt(request.getRequestType().getRequestType());
        byte[] bytes = request.getSerialized();
        buf.writeInt(bytes.length);
        buf.writeBytes(bytes);
    }
}
