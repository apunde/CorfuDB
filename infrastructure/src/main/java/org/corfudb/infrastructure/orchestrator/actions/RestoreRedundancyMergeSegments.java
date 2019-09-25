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

import com.sun.org.apache.regexp.internal.RE;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.log.StreamLog;
import org.corfudb.infrastructure.log.statetransfer.StateTransferManager;
import org.corfudb.infrastructure.log.statetransfer.StateTransferWriter;
import org.corfudb.infrastructure.orchestrator.Action;
import org.corfudb.infrastructure.orchestrator.RestoreAction;
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
import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.*;
import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.SegmentState.NOT_TRANSFERRED;
import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.SegmentState.RESTORED;
import static org.corfudb.protocols.wireprotocol.statetransfer.StateTransferResponseType.*;

/**
 * This action attempts to restore redundancy for all servers across all segments
 * starting from the oldest segment. It then collapses the segments once the set of
 * servers in the 2 oldest subsequent segments are equal.
 * Created by Zeeshan on 2019-02-06.
 */
@Slf4j
public class RestoreRedundancyMergeSegments extends RestoreAction {


    @AllArgsConstructor
    public static class RedundancyCalculator {

        @NonNull
        @Getter
        private final String server;

        public ImmutableMap<CurrentTransferSegment, CurrentTransferSegmentStatus>
        createStateMap(Layout layout){
            Map<CurrentTransferSegment, CurrentTransferSegmentStatus> map = layout.getSegments().stream().map(segment -> {
                CurrentTransferSegment statusSegment = new CurrentTransferSegment(segment.getStart(), segment.getEnd());

                if (segmentContainsServer(segment)) {
                    return new SimpleEntry<>(statusSegment,
                            new CurrentTransferSegmentStatus(RESTORED, segment.getEnd()));
                } else {
                    return new SimpleEntry<>(statusSegment,
                            new CurrentTransferSegmentStatus(NOT_TRANSFERRED, segment.getStart()));
                }
            }).collect(Collectors.toMap(SimpleEntry::getKey, SimpleEntry::getValue));
            return ImmutableMap.copyOf(map);
        }

        private boolean segmentContainsServer(LayoutSegment segment){
            // Because we restore the node to the first stripe.
            return segment.getFirstStripe().getLogServers().contains(server);
        }

        private Layout restoreRedundancyForSegment(CurrentTransferSegment mapSegment, Layout layout){

            List<LayoutSegment> segments = layout.getSegments().stream().map(layoutSegment -> {
                if (layoutSegment.getStart() == mapSegment.getStartAddress() &&
                        layoutSegment.getEnd() == mapSegment.getEndAddress()) {
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

        public Layout updateLayoutAfterRedundancyRestoration(List<CurrentTransferSegment> segments, Layout initLayout){
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
        public boolean redundancyIsRestored(Map<CurrentTransferSegment, CurrentTransferSegmentStatus> map){
            return map.values().stream().allMatch(state -> state.equals(RESTORED));
        }

        public static boolean canMergeSegments(Layout layout){
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

    // Prepare a message based on the current status of the segment, if possible.
    private CompletableFuture<Optional<StateTransferBaseResponse>> prepareMsg(CurrentTransferSegment segment,
                                                                              CurrentTransferSegmentStatus state,
                                                                              CorfuRuntime runtime){
        LogUnitClient logUnitClient = runtime
                .getLayoutView()
                .getRuntimeLayout()
                .getLogUnitClient(currentNode);

        if(state.equals(NOT_TRANSFERRED)){
            return logUnitClient.initializeStateTransfer(segment.segmentStart, segment.segmentEnd)
                    .thenApply(Optional::of);
        }
        else if(state.equals(TRANSFERRING)){
            return CFUtils.delayFuture(scheduledExecutorService,
                    () -> logUnitClient.pollStateTransfer(segment.segmentStart, segment.segmentEnd),
                    POLL_INTERVAL_DELAY, POLL_INTERVAL_UNIT).thenApply(Optional::of);
        }
        else {
            return CompletableFuture.completedFuture(Optional.empty());
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

    // If this function fails, then the workflow is retried.
    private void tryRestoreRedundancyAndMergeSegments(Map<Segment, SegmentState> stateMap,
                                                  CorfuRuntime runtime)
            throws OutrankedException {

        List<Entry<Segment, SegmentState>> failedSegments =
                stateMap.entrySet()
                        .stream()
                        .filter(entry -> entry.getValue()
                                .equals(FAILED))
                        .collect(Collectors.toList());

        if(!failedSegments.isEmpty()){
            throw new IllegalStateException("Transfer failed for one or more segments.");
        }

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

            if(RedundancyCalculator.canMergeSegments(newLayout)){
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
    public void impl(@Nonnull CorfuRuntime runtime, @NonNull StreamLog streamLog) throws Exception {

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

            // Perform actions for each segment based on their current status, if possible.
            // Filter responses that don't exist.
            CompletableFuture<List<StateTransferBaseResponse>> responses =
                    CFUtils.sequence(stateMap.entrySet()
                    .stream()
                    .sorted(Entry.comparingByKey())
                    .map(entry -> {
                        Segment segment = entry.getKey();
                        SegmentState state = entry.getValue();
                        return prepareMsg(segment, state, runtime);
                    }).collect(Collectors.toList()))
                    .thenApply(list -> list.stream()
                            .filter(Optional::isPresent)
                            .map(Optional::get).collect(Collectors.toList()));

            // Update segment state map based on the responses from the log unit server.
            CompletableFuture<ImmutableMap<Segment, SegmentState>> map =
                    responses.thenApply(this::createStatusMapFromResponses);


            // Try restore redundancy for the segments that are restored.
            // If possible, also merge segments.
            tryRestoreRedundancyAndMergeSegments(map.join(), runtime);

            runtime.invalidateLayout();

            layout = runtime.getLayoutView().getLayout();

            stateMap = redundancyCalculator.createStateMap(layout);
        }

    }
}
