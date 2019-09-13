package org.corfudb.infrastructure.log.statetransfer.exceptions;

import lombok.Getter;

import java.util.Set;

public class IncompleteGarbageReadException extends IncompleteReadException {
    @Getter
    private final String hostAddress;

    public IncompleteGarbageReadException(String hostAddress, Set<Long> missingAddresses){
        super(missingAddresses);
        this.hostAddress = hostAddress;
    }
}
