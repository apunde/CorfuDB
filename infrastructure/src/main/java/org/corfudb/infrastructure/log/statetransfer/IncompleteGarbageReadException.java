package org.corfudb.infrastructure.log.statetransfer;

import lombok.Getter;

import java.util.Set;

public class IncompleteGarbageReadException extends IncompleteReadException {
    @Getter
    private final String hostAddress;

    IncompleteGarbageReadException(String hostAddress, Set<Long> missingAddresses){
        super(missingAddresses);
        this.hostAddress = hostAddress;
    }
}
