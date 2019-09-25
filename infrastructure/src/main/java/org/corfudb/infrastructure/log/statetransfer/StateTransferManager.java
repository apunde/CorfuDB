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
import org.corfudb.infrastructure.orchestrator.actions.RestoreRedundancyMergeSegments;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.CorfuPayloadMsg;
import org.corfudb.protocols.wireprotocol.statetransfer.InitTransferRequest;
import org.corfudb.protocols.wireprotocol.statetransfer.StateTransferFailedResponse;
import org.corfudb.protocols.wireprotocol.statetransfer.StateTransferFinishedResponse;
import org.corfudb.protocols.wireprotocol.statetransfer.StateTransferInProgressResponse;
import org.corfudb.protocols.wireprotocol.statetransfer.StateTransferRequestMsg;
import org.corfudb.protocols.wireprotocol.statetransfer.StateTransferRequestType;
import org.corfudb.protocols.wireprotocol.statetransfer.StateTransferResponseMsg;

import javax.annotation.Nonnull;
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

    public enum SegmentState{
        NOT_TRANSFERRED,
        TRANSFERRING,
        TRANSFERRED,
        RESTORED,
        FAILED
    }

    @AllArgsConstructor
    @EqualsAndHashCode
    @Getter
    public static class CurrentTransferSegment implements Comparable<CurrentTransferSegment>{
        private final long startAddress;
        private final long endAddress;

        @Override
        public int compareTo(CurrentTransferSegment other) {
            return (int) (this.startAddress - other.endAddress);
        }
    }

    @AllArgsConstructor
    @Getter
    public static class CurrentTransferSegmentStatus{
        private SegmentState segmentStateTransferState;
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


    private void handleTransfer()

    private void handleInitTransfer(CorfuPayloadMsg<StateTransferRequestMsg> msg,
                                    ChannelHandlerContext ctx, IServerRouter r){
        InitTransferRequest request = (InitTransferRequest) msg.getPayload().getRequest();
        CurrentTransferSegment segment =
                new CurrentTransferSegment(request.getAddressStart(), request.getAddressEnd());
        // State transfer for the segment is not running -> initialize state and start transfer.
        if(!currentTransferSegmentStatusMap.containsKey(segment)){
            CurrentTransferSegmentStatus status =
                    new CurrentTransferSegmentStatus(TRANSFERRING, segment.getStartAddress());
            currentTransferSegmentStatusMap.put(segment, status);
            List<Long> addressesToTransfer =
                    getUnknownAddressesInRange(request.getAddressStart(), request.getAddressEnd());
            stateTransferWriter.stateTransfer(addressesToTransfer, segment, currentTransferSegmentStatusMap);
        }
        //  Check the state and report appropriately.
        else{
            handlePollTransfer(msg, ctx, r);
        }
    }

    private void handlePollTransfer(CorfuPayloadMsg<StateTransferRequestMsg> msg,
                                    ChannelHandlerContext ctx, IServerRouter r){
        long addressStart = msg.getPayload().getRequest().getAddressStart();
        long addressEnd = msg.getPayload().getRequest().getAddressEnd();
        CurrentTransferSegment segment =
                new CurrentTransferSegment(addressStart, addressEnd);

        CurrentTransferSegmentStatus currentTransferSegmentStatus =
                currentTransferSegmentStatusMap.get(segment);

        SegmentStateTransferState state =
                currentTransferSegmentStatus.getSegmentStateTransferState();

        if(state.equals(TRANSFERRED)){
            currentTransferSegmentStatusMap.remove(segment);
            StateTransferResponseMsg stateTransferFinishedMsg =
                    new StateTransferResponseMsg(new StateTransferFinishedResponse());
            r.sendResponse(ctx,
                    msg,
                    CorfuMsgType.STATE_TRANSFER_RESPONSE.payloadMsg(stateTransferFinishedMsg));

        }
        else if(state.equals(TRANSFERRING)){
            StateTransferResponseMsg stateTransferInProgressMsg =
                    new StateTransferResponseMsg(
                            new StateTransferInProgressResponse
                                    (currentTransferSegmentStatus.getLastTransferredAddress(),
                                            segment.getStartAddress(),
                                            segment.getEndAddress()));
            r.sendResponse(ctx,
                    msg,
                    CorfuMsgType.STATE_TRANSFER_RESPONSE.payloadMsg(stateTransferInProgressMsg));
        }
        else {
            currentTransferSegmentStatusMap.remove(segment);
            StateTransferResponseMsg stateTransferFailedMsg =
                    new StateTransferResponseMsg(
                            new StateTransferFailedResponse
                                    (segment.getStartAddress(), segment.getEndAddress()));
            r.sendResponse(ctx,
                    msg,
                    CorfuMsgType.STATE_TRANSFER_RESPONSE.payloadMsg(stateTransferFailedMsg));
        }
    }

    public void handleMessage(@Nonnull CorfuPayloadMsg<StateTransferRequestMsg> msg,
                              @Nonnull ChannelHandlerContext ctx,
                              @Nonnull IServerRouter r){
        StateTransferRequestMsg stateTransferRequestMsg = msg.getPayload();
        StateTransferRequestType stateTransferRequestType = stateTransferRequestMsg
                .getRequest()
                .getRequestType();
        // SWITCH
        if (stateTransferRequestType == StateTransferRequestType.INIT_TRANSFER) {
            handleInitTransfer(msg, ctx, r);
        } else  {
            handlePollTransfer(msg, ctx, r);
        }
    }
}
