package org.corfudb.infrastructure.log;

import io.netty.channel.ChannelHandlerContext;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.math.LongRange;
import org.corfudb.infrastructure.IServerRouter;
import org.corfudb.protocols.wireprotocol.CorfuPayloadMsg;
import org.corfudb.protocols.wireprotocol.statetransfer.StateTransferRequestMsg;
import org.corfudb.protocols.wireprotocol.statetransfer.StateTransferRequestType;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

@Slf4j
@Builder
/**
 * This class is responsible for managing a state transfer on the current node.
 */
public class StateTransferManager {

    public enum SegmentStateTransferState{
        TRANSFERRING,
        TRANSFERRED,
        FAILED
    }

    @AllArgsConstructor
    @EqualsAndHashCode
    @Getter
    private static class CurrentTransferSegment{
        private final long startAddress;
        private final long endAddress;
    }
    @AllArgsConstructor
    @Getter
    private static class CurrentTransferSegmentStatus{
        private SegmentStateTransferState segmentStateTransferState;
        private long lastTransferredAddress;
    }

    @Getter
    private final Map<CurrentTransferSegment, CurrentTransferSegmentStatus>
            currentTransferSegmentStatusMap = new ConcurrentHashMap<>();

    @Getter
    @NonNull
    private StreamLog streamLog;

    @Getter
    @NonNull
    private StateTransferWriter stateTransferWriter;

    private List<Long> getUnknownAddressesInRange(long rangeStart, long rangeEnd) {

        Set<Long> knownAddresses = streamLog
                .getKnownAddressesInRange(rangeStart, rangeEnd);

        return LongStream.range(rangeStart, rangeEnd + 1)
                .filter(address -> !knownAddresses.contains(address))
                .boxed()
                .collect(Collectors.toList());
    }

    public void handleMessage(@Nonnull CorfuPayloadMsg<StateTransferRequestMsg> msg,
                              @Nonnull ChannelHandlerContext ctx,
                              @Nonnull IServerRouter r){
        StateTransferRequestMsg stateTransferRequestMsg = msg.getPayload();
        StateTransferRequestType stateTransferRequestType = stateTransferRequestMsg
                .getRequest()
                .getRequestType();

        switch(stateTransferRequestType){
            case INIT_TRANSFER:

        }
    }
}
