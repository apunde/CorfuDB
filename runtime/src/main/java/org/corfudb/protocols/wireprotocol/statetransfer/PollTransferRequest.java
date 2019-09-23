package org.corfudb.protocols.wireprotocol.statetransfer;

import static org.corfudb.protocols.wireprotocol.statetransfer.StateTransferRequestType.INIT_TRANSFER;

public class PollTransferRequest extends StateTransferBaseRequest  {
    PollTransferRequest(long addressStart, long addressEnd){
        super(addressStart, addressEnd);
    }

    PollTransferRequest(byte [] buf){
        super(buf);
    }

    @Override
    StateTransferRequestType getRequestType() {
        return INIT_TRANSFER;
    }
}
