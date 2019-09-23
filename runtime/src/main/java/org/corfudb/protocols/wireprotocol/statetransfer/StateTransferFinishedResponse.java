package org.corfudb.protocols.wireprotocol.statetransfer;

public class StateTransferFinishedResponse implements Response {

    public StateTransferFinishedResponse(){

    }

    StateTransferFinishedResponse(byte [] buf){

    }

    @Override
    public StateTransferResponseType getResponseType() {
        return StateTransferResponseType.TRANSFER_FINISHED;
    }

    @Override
    public byte[] getSerialized() {
        return new byte[0];
    }
}
