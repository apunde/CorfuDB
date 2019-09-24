package org.corfudb.protocols.wireprotocol.statetransfer;

import static org.corfudb.protocols.wireprotocol.statetransfer.StateTransferRequestType.POLL_TRANSFER;

public class PollTransferRequest extends StateTransferBaseRequest  {
    public PollTransferRequest(long addressStart, long addressEnd){
        super(addressStart, addressEnd);
    }

    PollTransferRequest(byte [] buf){
        super(buf);
    }

    @Override
    public StateTransferRequestType getRequestType() {
        return POLL_TRANSFER;
    }
}
