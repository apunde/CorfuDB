package org.corfudb.infrastructure.orchestrator.actions;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.orchestrator.Action;
import org.corfudb.protocols.wireprotocol.statetransfer.Response;
import org.corfudb.protocols.wireprotocol.statetransfer.StateTransferBaseResponse;
import org.corfudb.protocols.wireprotocol.statetransfer.StateTransferFailedResponse;
import org.corfudb.protocols.wireprotocol.statetransfer.StateTransferFinishedResponse;
import org.corfudb.protocols.wireprotocol.statetransfer.StateTransferRequestMsg;
import org.corfudb.protocols.wireprotocol.statetransfer.StateTransferResponseMsg;
import org.corfudb.protocols.wireprotocol.statetransfer.StateTransferResponseType;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.LogUnitClient;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.Layout.LayoutSegment;
import org.corfudb.runtime.view.Layout.LayoutStripe;
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
        FAILED,
        NOT_TRANSFERRED,
        TRANSFERRING,
        TRANSFERRED
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


    @Builder
    public static class RedundancyCalculator {

        @NonNull
        private final Layout layout;

        @NonNull
        private final String server;

        /**
         * Get the stripes of the last segment for which the node is present.
         *
         * @return The stripes of the last segment.
         */
        @VisibleForTesting
        Set<LayoutStripe> getLastSegmentStripes() {
            List<LayoutStripe> stripesOfLastSegment = layout.getLatestSegment().getStripes();
            return stripesOfLastSegment
                    .stream()
                    .filter(stripe -> stripe.getLogServers().contains(server))
                    .collect(Collectors.toSet());
        }

        /**
         *  Get the map from the segments to the set of stripes that do not contain this node.
         *
         * @return The map for all segments to the set of stripes.
         */
        public Map<LayoutSegment, Set<LayoutStripe>> getMissingStripesForAllSegments() {
            Set<LayoutStripe> stripesToBeRestored = getLastSegmentStripes();

            return layout.getSegments()
                    .stream()
                    .collect(Collectors.toMap(Function.identity(), l -> {
                        List<LayoutStripe> stripesForThisSegment = l.getStripes();
                        Set<LayoutStripe> stripesWhereNodeIsPresent = stripesForThisSegment
                                .stream()
                                .filter(stripe -> stripe.getLogServers().contains(server))
                                .collect(Collectors.toSet());
                        return Sets.difference(stripesToBeRestored, stripesWhereNodeIsPresent);

                    }));
        }

        public ImmutableMap<Segment, SegmentState> createStatusMap(Map<LayoutSegment, Set<LayoutStripe>> missingStripesMap){
            Map<Segment, SegmentState> map = missingStripesMap.keySet().stream().map(entryKey -> {
                Segment segment = new Segment(entryKey.getStart(), entryKey.getEnd());
                return new SimpleEntry<>(segment, NOT_TRANSFERRED);
            }).collect(Collectors.toMap(SimpleEntry::getKey, SimpleEntry::getValue));
            return ImmutableMap.copyOf(map);
        }

        public ImmutableMap<Segment, SegmentState> updateStatusMap(Map<Segment, SegmentState> map){

        }

        /**
         * Check that this node is present for all the stripes in all the segments.
         *
         * @param map The immutable map of segment statuses.
         * @return True if every segment is transferred and false otherwise.
         */
        public boolean redundancyIsRestored(Map<Segment, SegmentState> map){
            return map.values().stream().allMatch(state -> state.equals(TRANSFERRED));
        }
    }

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
            return CFUtils.schedule(scheduledExecutorService,
                    logUnitClient.pollStateTransfer(segment.segmentStart, segment.segmentEnd),
                    POLL_INTERVAL_DELAY, POLL_INTERVAL_UNIT);
        }
        else if (state.equals(TRANSFERRED)){
            return CompletableFuture.completedFuture
                    (new StateTransferFinishedResponse(segment.segmentStart, segment.segmentEnd));
        }
        else {
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

        RedundancyCalculator redundancyCalculator = RedundancyCalculator
                .builder().layout(layout).build();

        // Get a map of missing stripes for all segment of this node.
        Map<LayoutSegment, Set<LayoutStripe>> missingStripesForAllSegments
                = redundancyCalculator.getMissingStripesForAllSegments();

        // Create a status map for each segment.
        ImmutableMap<Segment, SegmentState> statusMap =
                redundancyCalculator.createStatusMap(missingStripesForAllSegments);

        // While redundancy is not restored for all the segments for this node.
        while(!redundancyCalculator.redundancyIsRestored(statusMap)){

            // Sort the segments
            List<Segment> segments = Ordering.natural().sortedCopy(statusMap.keySet().asList());

            // Perform actions for each segment based on their current status.
            CompletableFuture<List<Response>> responses =
                    CFUtils.sequence(segments.stream().map(segment -> {
                SegmentState segmentState = statusMap.get(segment);
                return prepareMsg(segment, segmentState, runtime);
            }).collect(Collectors.toList()));


            runtime.invalidateLayout();
            layout = runtime.getLayoutView().getLayout();
        }

    }
}
