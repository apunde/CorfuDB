package org.corfudb.infrastructure.log.statetransfer;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.corfudb.protocols.wireprotocol.LogData;

import java.util.List;

@AllArgsConstructor
public class RejectedAppendException extends StateTransferException {
    @Getter
    private final List<LogData> dataEntries;
}
