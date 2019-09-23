package org.corfudb.protocols.wireprotocol.statetransfer;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@AllArgsConstructor
public enum StateTransferResponseType {

    TRANSFER_STARTED(0, StateTransferStartedResponse::new),
    TRANSFER_IN_PROGRESS(1, StateTransferInProgressResponse::new),
    TRANSFER_FINISHED(2, StateTransferFinishedResponse::new),
    TRANSFER_FAILED(3, StateTransferFailedResponse::new);

    @Getter
    private final int responseType;

    @Getter
    private final Function<byte [], Response> mapping;

    static final Map<Integer, StateTransferResponseType> typeMap =
            Arrays.stream(StateTransferResponseType.values())
                    .collect(Collectors.toMap(StateTransferResponseType::getResponseType, Function.identity()));

}
