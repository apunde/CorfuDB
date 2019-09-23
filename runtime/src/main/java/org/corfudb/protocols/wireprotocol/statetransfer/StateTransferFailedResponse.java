package org.corfudb.protocols.wireprotocol.statetransfer;

public class StateTransferFailedResponse implements Response {

    public StateTransferFailedResponse(){

    }

    StateTransferFailedResponse(byte [] buf){

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