package org.corfudb.protocols.wireprotocol.statetransfer;


public class StateTransferFailedResponse extends StateTransferBaseResponse {

    public StateTransferFailedResponse(long startAddress, long endAddress){
        super(startAddress, endAddress);
    }

    StateTransferFailedResponse(byte [] buf){
        super(buf);
    }

    @Override
    public StateTransferResponseType getResponseType() {
        return StateTransferResponseType.TRANSFER_FINISHED;
    }

}