package org.corfudb.infrastructure.log.statetransfer.exceptions;

import org.corfudb.protocols.wireprotocol.LogData;

import java.util.List;

public class RejectedGarbageException extends RejectedAppendException {
    public RejectedGarbageException(List<LogData> dataEntries) {
        super(dataEntries);
    }
}
