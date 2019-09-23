package org.corfudb.protocols.wireprotocol.statetransfer;


import static org.corfudb.protocols.wireprotocol.statetransfer.StateTransferRequestType.*;

public class InitTransferRequest extends StateTransferBaseRequest{

    InitTransferRequest(long addressStart, long addressEnd){
        super(addressStart, addressEnd);
    }

    InitTransferRequest(byte [] buf){
        super(buf);
    }

    @Override
    StateTransferRequestType getRequestType() {
        return INIT_TRANSFER;
    }
}
