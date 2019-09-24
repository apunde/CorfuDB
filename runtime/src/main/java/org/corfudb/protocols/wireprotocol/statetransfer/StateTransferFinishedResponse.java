package org.corfudb.protocols.wireprotocol.statetransfer;

public class StateTransferFinishedResponse extends StateTransferBaseResponse {

    public StateTransferFinishedResponse(long startAddress, long endAddress){
        super(startAddress, endAddress);
    }

    StateTransferFinishedResponse(byte [] buf){
        super(buf);
    }

    @Override
    public StateTransferResponseType getResponseType() {
        return StateTransferResponseType.TRANSFER_FINISHED;
    }

}
