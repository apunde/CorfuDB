package org.corfudb.infrastructure.log.statetransfer;

import org.corfudb.protocols.wireprotocol.LogData;

import java.util.List;

public class RejectedGarbageException extends RejectedAppendException {
    RejectedGarbageException(List<LogData> dataEntries) {
        super(dataEntries);
    }
}
