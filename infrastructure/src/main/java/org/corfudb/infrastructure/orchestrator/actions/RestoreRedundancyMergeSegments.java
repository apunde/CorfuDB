package org.corfudb.infrastructure.orchestrator.actions;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.orchestrator.Action;
import org.corfudb.protocols.wireprotocol.statetransfer.StateTransferBaseResponse;
import org.corfudb.protocols.wireprotocol.statetransfer.StateTransferFailedResponse;
import org.corfudb.protocols.wireprotocol.statetransfer.StateTransferFinishedResponse;
import org.corfudb.protocols.wireprotocol.statetransfer.StateTransferResponseType;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.LogUnitClient;
import org.corfudb.runtime.exceptions.OutrankedException;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.Layout.LayoutSegment;
import org.corfudb.runtime.view.Layout.LayoutStripe;
import org.corfudb.runtime.view.LayoutBuilder;
import org.corfudb.util.CFUtils;

import static java.util.AbstractMap.*;
import static org.corfudb.infrastructure.orchestrator.actions.RestoreRedundancyMergeSegments.SegmentState.*;
import static org.corfudb.protocols.wireprotocol.statetransfer.StateTransferResponseType.*;

/**
 * This action attempts to restore redundancy for all servers across all segments
 * starting from the oldest segment. It then collapses the segments once the set of
 * servers in the 2 oldest subsequent segments are equal.
 * Created by Zeeshan on 2019-02-06.
 */
@Slf4j
public class RestoreRedundancyMergeSegments extends Action {

    public enum SegmentState{
        NOT_TRANSFERRED,
        TRANSFERRING,
        TRANSFERRED,
        RESTORED,
        FAILED
    }

    @AllArgsConstructor
    @EqualsAndHashCode
    public static class Segment implements Comparable<Segment>{
        private final long segmentStart;
        private final long segmentEnd;

        @Override
        public int compareTo(Segment other) {
            return (int) (this.segmentStart - other.segmentStart);
        }
    }


    @AllArgsConstructor
    public static class RedundancyCalculator {

        @NonNull
        @Getter
        private final String server;

        public ImmutableMap<Segment, SegmentState> createStateMap(Layout layout){
            return ImmutableMap.copyOf(layout.getSegments().stream().map(segment -> {
                Segment statusSegment = new Segment(segment.getStart(), segment.getEnd());

                if (segmentContainsServer(segment)) {
                    return new SimpleEntry<>(statusSegment, RESTORED);
                } else {
                    return new SimpleEntry<>(statusSegment, NOT_TRANSFERRED);
                }
            }).collect(Collectors.toMap(SimpleEntry::getKey, SimpleEntry::getValue)));
        }

        private boolean segmentContainsServer(LayoutSegment segment){
            // Because we restore the node to the first stripe.
            return segment.getFirstStripe().getLogServers().contains(server);
        }

        private Layout restoreRedundancyForSegment(Segment mapSegment, Layout layout){

            List<LayoutSegment> segments = layout.getSegments().stream().map(layoutSegment -> {
                if (layoutSegment.getStart() == mapSegment.segmentStart &&
                        layoutSegment.getEnd() == mapSegment.segmentEnd) {
                    List<LayoutStripe> newStripes = layoutSegment.getStripes().stream().map(stripe -> {
                        List<String> logServers = new ArrayList<>(stripe.getLogServers());
                        logServers.add(getServer());
                        return new LayoutStripe(logServers);
                    }).collect(Collectors.toList());

                    return new LayoutSegment(layoutSegment.getReplicationMode(),
                            layoutSegment.getStart(), layoutSegment.getEnd(), newStripes);
                } else {
                    return new LayoutSegment(layoutSegment.getReplicationMode(),
                            layoutSegment.getStart(), layoutSegment.getEnd(), layoutSegment.getStripes());
                }
            }).collect(Collectors.toList());
            Layout newLayout = new Layout(layout);
            newLayout.setSegments(segments);
            return newLayout;
        }

        public Layout updateLayoutAfterRedundancyRestoration(List<Segment> segments, Layout initLayout){
            return segments
                    .stream()
                    .reduce(initLayout,
                            (layout, segment) -> restoreRedundancyForSegment(segment, layout),
                            (oldLayout, newLayout) -> newLayout);
        }

        /**
         * Check that the redundancy is restored.
         *
         * @param map The immutable map of segment statuses.
         * @return True if every segment is transferred and false otherwise.
         */
        public boolean redundancyIsRestored(Map<Segment, SegmentState> map){
            return map.values().stream().allMatch(state -> state.equals(RESTORED));
        }

        public boolean canMergeSegments(Layout layout){
            if(layout.getSegments().size() == 1){
                return false;
            }
            else{
                return Sets.difference(
                        layout.getSegments().get(1).getAllLogServers(),
                        layout.getSegments().get(0).getAllLogServers()).isEmpty();
            }
        }
    }

    @Getter
    private final String currentNode;


    private final ScheduledExecutorService scheduledExecutorService =
            Executors.newScheduledThreadPool(4);

    private static final long POLL_INTERVAL_DELAY = 3L;

    private static final TimeUnit POLL_INTERVAL_UNIT = TimeUnit.SECONDS;

    // Prepare a message based on the current status of the segment.
    private CompletableFuture<StateTransferBaseResponse> prepareMsg(Segment segment,
                                                   SegmentState state,
                                                   CorfuRuntime runtime){
        LogUnitClient logUnitClient = runtime
                .getLayoutView()
                .getRuntimeLayout()
                .getLogUnitClient(currentNode);

        if(state.equals(NOT_TRANSFERRED)){
            return logUnitClient.initializeStateTransfer(segment.segmentStart, segment.segmentEnd);
        }
        else if(state.equals(TRANSFERRING)){
            return CFUtils.delayFuture(scheduledExecutorService,
                    () -> logUnitClient.pollStateTransfer(segment.segmentStart, segment.segmentEnd),
                    POLL_INTERVAL_DELAY, POLL_INTERVAL_UNIT);
        }
        else if (state.equals(TRANSFERRED)){
            return CompletableFuture.completedFuture
                    (new StateTransferFinishedResponse(segment.segmentStart, segment.segmentEnd));
        }
        else if(state.equals(FAILED)){
            return CompletableFuture.completedFuture
                    (new StateTransferFailedResponse(segment.segmentStart, segment.segmentEnd));

        }
    }

    // Create status map based on the responses
    private ImmutableMap<Segment, SegmentState> createStatusMapFromResponses(List<StateTransferBaseResponse> responses) {
        Map<Segment, SegmentState> newMap = responses.stream().map(response -> {
            StateTransferResponseType responseType = response.getResponseType();
            long addressStart = response.getAddressStart();
            long addressEnd = response.getAddressEnd();
            Segment segment = new Segment(addressStart, addressEnd);

            if (responseType.equals(TRANSFER_STARTED) || responseType.equals(TRANSFER_IN_PROGRESS)) {
                return new SimpleEntry<>(segment, TRANSFERRING);
            }

            else if (responseType.equals(TRANSFER_FAILED)) {
                return new SimpleEntry<>(segment, FAILED);
            }
            else {
                return new SimpleEntry<>(segment, TRANSFERRED);
            }
        }).collect(Collectors.toMap(SimpleEntry::getKey, SimpleEntry::getValue));
        return ImmutableMap.copyOf(newMap);
    }

    private void tryRestoreRedundancyandMergeSegments(Map<Segment, SegmentState> stateMap,
                                                  CorfuRuntime runtime)
            throws OutrankedException {

        List<Segment> segmentsThatWereTransferred = stateMap.keySet().stream().filter(segment -> {
            SegmentState segmentState = stateMap.get(segment);
            return segmentState.equals(TRANSFERRED);
        }).collect(Collectors.toList());

        if(!segmentsThatWereTransferred.isEmpty()){
            runtime.invalidateLayout();

            Layout oldLayout = runtime.getLayoutView().getLayout();

            RedundancyCalculator calculator = new RedundancyCalculator(currentNode);

            Layout newLayout =
                    calculator.updateLayoutAfterRedundancyRestoration(
                            segmentsThatWereTransferred, oldLayout);

            if(calculator.canMergeSegments(newLayout)){
                runtime.getLayoutManagementView().mergeSegments(newLayout);
            }
            else{
                runtime.getLayoutManagementView()
                        .runLayoutReconfiguration(oldLayout, newLayout, false);
            }

        }
    }



    public RestoreRedundancyMergeSegments(String currentNode) {
        this.currentNode = currentNode;
    }

    @Nonnull
    @Override
    public String getName() {
        return "RestoreRedundancyAndMergeSegments";
    }

    @Override
    public void impl(@Nonnull CorfuRuntime runtime) throws Exception {

        // Refresh layout.
        runtime.invalidateLayout();

        // Get layout.
        Layout layout = runtime.getLayoutView().getLayout();

        // Create a helper class to perform state calculations.
        RedundancyCalculator redundancyCalculator = new RedundancyCalculator(currentNode);

        // Create an initial state map.
        ImmutableMap<Segment, SegmentState> stateMap = redundancyCalculator.createStateMap(layout);

        // While redundancy is not restored for all the segments for this node.
        while(!redundancyCalculator.redundancyIsRestored(stateMap)){

            // Perform actions for each segment based on their current status.

            // make them sorted
            stateMap.entrySet().stream().map(enty -> {
                Segment segment = enty.getKey();
                SegmentState state = enty.getValue();
            })
            CompletableFuture<List<StateTransferBaseResponse>> responses =
                    CFUtils.sequence(segments.stream().map(segment -> {
                SegmentState segmentState = finalStateMap.get(segment);
                return prepareMsg(segment, segmentState, runtime);
            }).collect(Collectors.toList()));

            // Update segment map based on the responses
            CompletableFuture<ImmutableMap<Segment, SegmentState>> map =
                    responses.thenApply(this::createStatusMapFromResponses);

            tryRestoreRedundancyandMergeSegments(map.join(), runtime);

            runtime.invalidateLayout();

            layout = runtime.getLayoutView().getLayout();

            stateMap = redundancyCalculator.createStateMap(layout);
        }

    }
}
