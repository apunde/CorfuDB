package org.corfudb.protocols.wireprotocol.statetransfer;


import static org.corfudb.protocols.wireprotocol.statetransfer.StateTransferRequestType.*;

public class InitTransferRequest extends StateTransferBaseRequest{

    public InitTransferRequest(long addressStart, long addressEnd){
        super(addressStart, addressEnd);
    }

    InitTransferRequest(byte [] buf){
        super(buf);
    }

    @Override
    public StateTransferRequestType getRequestType() {
        return INIT_TRANSFER;
    }
}
