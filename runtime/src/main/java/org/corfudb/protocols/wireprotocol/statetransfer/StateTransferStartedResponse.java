package org.corfudb.protocols.wireprotocol.statetransfer;


import static org.corfudb.protocols.wireprotocol.statetransfer.StateTransferResponseType.TRANSFER_STARTED;

public class StateTransferStartedResponse extends StateTransferBaseResponse {

    public StateTransferStartedResponse(long addressStart, long addressEnd){
        super(addressStart, addressEnd);
    }

    public StateTransferStartedResponse(byte [] buf){
        super(buf);
    }
    @Override
    public StateTransferResponseType getResponseType() {
        return TRANSFER_STARTED;
    }

}
