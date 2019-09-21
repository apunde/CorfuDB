package org.corfudb.infrastructure.log.statetransfer;

import org.corfudb.protocols.wireprotocol.LogData;

import java.util.List;

public class RejectedDataException extends RejectedAppendException {
    RejectedDataException(List<LogData> dataEntries){
        super(dataEntries);
    }
}
