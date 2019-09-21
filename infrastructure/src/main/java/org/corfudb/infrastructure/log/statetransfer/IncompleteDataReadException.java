package org.corfudb.infrastructure.log.statetransfer;

import java.util.Set;

public class IncompleteDataReadException extends IncompleteReadException {
    IncompleteDataReadException(Set<Long> missingAddresses) {
        super(missingAddresses);
    }
}
