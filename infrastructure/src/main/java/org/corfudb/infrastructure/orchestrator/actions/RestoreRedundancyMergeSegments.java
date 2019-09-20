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
import org.corfudb.protocols.wireprotocol.statetransfer.StateTransferRequestMsg;
import org.corfudb.protocols.wireprotocol.statetransfer.StateTransferResponseMsg;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.Layout.LayoutSegment;
import org.corfudb.runtime.view.Layout.LayoutStripe;
import org.corfudb.util.CFUtils;

import static org.corfudb.infrastructure.orchestrator.actions.RestoreRedundancyMergeSegments.SegmentState.*;

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
            Map<Segment, SegmentState> map = missingStripesMap.entrySet().stream().map(entry -> {
                LayoutSegment entryKey = entry.getKey();
                Segment segment = new Segment(entryKey.getStart(), entryKey.getEnd());
                SegmentState state = NOT_TRANSFERRED;
                return new AbstractMap.SimpleEntry<>(segment, state);
            }).collect(Collectors.toMap(entry -> entry.getKey(), entry -> entry.getValue()));
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

    /**
     * Returns set of nodes which are present in the next index but not in the specified segment. These
     * nodes have reduced redundancy and state needs to be transferred only to these before these segments can be
     * merged.
     *
     * @param layout             Current layout.
     * @param layoutSegmentIndex Segment to compare to get nodes with reduced redundancy.
     * @return Set of nodes with reduced redundancy.
     */
    private Set<String> getNodesWithReducedRedundancy(Layout layout, int layoutSegmentIndex) {
        // Get the set of servers present in the next segment but not in the this
        // segment.
        return Sets.difference(
                layout.getSegments().get(layoutSegmentIndex + 1).getAllLogServers(),
                layout.getSegments().get(layoutSegmentIndex).getAllLogServers());
    }

    private CompletableFuture<Response> prepareMsg(Segment segment,
                                                   SegmentState state,
                                                   CorfuRuntime runtime){
        return runtime
                .getLayoutView()
                .getRuntimeLayout()
                .getLogUnitClient(currentNode)
                .initializeStateTransfer(segment.segmentStart, segment.segmentEnd);
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

        Layout layout = runtime.getLayoutView().getLayout();
        RedundancyCalculator redundancyCalculator = RedundancyCalculator
                .builder().layout(layout).build();

        Map<LayoutSegment, Set<LayoutStripe>> missingStripesForAllSegments
                = redundancyCalculator.getMissingStripesForAllSegments();

        ImmutableMap<Segment, SegmentState> statusMap =
                redundancyCalculator.createStatusMap(missingStripesForAllSegments);

        while(!redundancyCalculator.redundancyIsRestored(statusMap)){
            List<Segment> segments = Ordering.natural().sortedCopy(statusMap.keySet().asList());
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
