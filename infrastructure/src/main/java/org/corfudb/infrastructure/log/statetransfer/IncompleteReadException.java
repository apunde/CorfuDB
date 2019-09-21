package org.corfudb.infrastructure.log.statetransfer;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.Set;

@AllArgsConstructor
public class IncompleteReadException extends StateTransferException {
    @Getter
    private final Set<Long> missingAddresses;
}
