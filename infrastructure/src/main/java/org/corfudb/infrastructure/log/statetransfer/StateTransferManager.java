package org.corfudb.infrastructure.log.statetransfer;

import io.netty.channel.ChannelHandlerContext;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.IServerRouter;
import org.corfudb.infrastructure.log.StreamLog;
import org.corfudb.protocols.wireprotocol.CorfuPayloadMsg;
import org.corfudb.protocols.wireprotocol.statetransfer.InitTransferRequest;
import org.corfudb.protocols.wireprotocol.statetransfer.Response;
import org.corfudb.protocols.wireprotocol.statetransfer.StateTransferRequestMsg;
import org.corfudb.protocols.wireprotocol.statetransfer.StateTransferRequestType;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.SegmentStateTransferState.TRANSFERRING;

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
    public static class CurrentTransferSegment{
        private final long startAddress;
        private final long endAddress;
    }
    @AllArgsConstructor
    @Getter
    @Setter
    public static class CurrentTransferSegmentStatus{
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

    private CompletableFuture<Response> handleInitTransfer(InitTransferRequest request){
        CurrentTransferSegment segment =
                new CurrentTransferSegment(request.getAddressStart(), request.getAddressEnd());
        // 1. NO-STATUS: initialize and update map
        if(!currentTransferSegmentStatusMap.containsKey(segment)){
            CurrentTransferSegmentStatus status =
                    new CurrentTransferSegmentStatus(TRANSFERRING, segment.getStartAddress());
            currentTransferSegmentStatusMap.put(segment, status);

        }
        // 2. It's there -> report status
        else{

        }
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
                InitTransferRequest request = (InitTransferRequest) stateTransferRequestMsg.getRequest();
                handleInitTransfer(request);


        }
    }
}
