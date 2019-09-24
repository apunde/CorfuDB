package org.corfudb.protocols.wireprotocol.statetransfer;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * The type of requests that can be made during the state transfer process.
 */
@AllArgsConstructor
public enum StateTransferRequestType {

    INIT_TRANSFER(0, InitTransferRequest::new),
    POLL_TRANSFER(1, PollTransferRequest::new);

    @Getter
    private final int requestType;

    @Getter
    private final Function<byte [], StateTransferBaseRequest> mapping;

    static final Map<Integer, StateTransferRequestType> typeMap =
            Arrays.stream(StateTransferRequestType.values())
                    .collect(Collectors.toMap(StateTransferRequestType::getRequestType, Function.identity()));

}
