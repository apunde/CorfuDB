package org.corfudb.protocols.wireprotocol.statetransfer;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.function.Function;

/**
 * The type of requests that can be made during the state transfer process.
 */
@AllArgsConstructor
public enum StateTransferRequestType {

    INIT_TRANSFER(0, InitTransferRequest::new);

    @Getter
    private final int requestType;

    @Getter
    private final Function<byte [], Request> mapping;



}
