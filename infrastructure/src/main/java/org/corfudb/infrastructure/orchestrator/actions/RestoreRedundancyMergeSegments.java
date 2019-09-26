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
import edu.umd.cs.findbugs.ba.ComparableField;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.log.StreamLog;
import org.corfudb.infrastructure.log.statetransfer.StateTransferManager;
import org.corfudb.infrastructure.log.statetransfer.StateTransferWriter;
import org.corfudb.infrastructure.log.statetransfer.batch.GCAwareTransferBatchProcessor;
import org.corfudb.infrastructure.orchestrator.RestoreAction;
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
import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.SegmentState.FAILED;
import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.SegmentState.NOT_TRANSFERRED;
import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.SegmentState.RESTORED;
import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.SegmentState.TRANSFERRED;
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

        public ImmutableMap<CurrentTransferSegment, CompletableFuture<CurrentTransferSegmentStatus>>
        createStateMap(Layout layout) {
            Map<CurrentTransferSegment, CompletableFuture<CurrentTransferSegmentStatus>> map = layout.getSegments().stream().map(segment -> {
                CurrentTransferSegment statusSegment = new CurrentTransferSegment(segment.getStart(), segment.getEnd());

                if (segmentContainsServer(segment)) {
                    return new SimpleEntry<>(statusSegment,
                            CompletableFuture
                                    .completedFuture(new
                                            CurrentTransferSegmentStatus(RESTORED,
                                            segment.getEnd())));
                } else {
                    return new SimpleEntry<>(statusSegment,
                            CompletableFuture.completedFuture(new
                                    CurrentTransferSegmentStatus(NOT_TRANSFERRED,
                                    segment.getStart())));
                }
            }).collect(Collectors.toMap(SimpleEntry::getKey, SimpleEntry::getValue));
            return ImmutableMap.copyOf(map);
        }


        private boolean segmentContainsServer(LayoutSegment segment) {
            // Because we restore the node to the first stripe.
            return segment.getFirstStripe().getLogServers().contains(server);
        }

        private Layout restoreRedundancyForSegment(CurrentTransferSegment mapSegment, Layout layout) {

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

        public Layout updateLayoutAfterRedundancyRestoration(List<CurrentTransferSegment> segments, Layout initLayout) {
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
        public boolean redundancyIsRestored(Map<CurrentTransferSegment, CompletableFuture<CurrentTransferSegmentStatus>> map) {
            return map.values().stream().allMatch(state -> {

                if (!state.isDone()) {
                    return false;
                } else {
                    CurrentTransferSegmentStatus currentTransferSegmentStatus = state.join();
                    return currentTransferSegmentStatus.getSegmentStateTransferState()
                            .equals(RESTORED);
                }
            });
        }

        public static boolean canMergeSegments(Layout layout) {
            if (layout.getSegments().size() == 1) {
                return false;
            } else {
                return Sets.difference(
                        layout.getSegments().get(1).getAllLogServers(),
                        layout.getSegments().get(0).getAllLogServers()).isEmpty();
            }
        }

        public ImmutableMap<CurrentTransferSegment, CompletableFuture<CurrentTransferSegmentStatus>>
        mergeMaps(ImmutableMap<CurrentTransferSegment, CompletableFuture<CurrentTransferSegmentStatus>> oldMap,
                  ImmutableMap<CurrentTransferSegment, CompletableFuture<CurrentTransferSegmentStatus>> newMap) {
            Map<CurrentTransferSegment, CompletableFuture<CurrentTransferSegmentStatus>> resultMap =
                    newMap.keySet().stream().map(newMapKey -> {
                        if (oldMap.containsKey(newMapKey)) {
                            return new SimpleEntry<>(newMapKey, oldMap.get(newMapKey));
                        } else {
                            return new SimpleEntry<>(newMapKey, newMap.get(newMapKey));
                        }
                    }).collect(Collectors.toMap(SimpleEntry::getKey, SimpleEntry::getValue));

            return ImmutableMap.copyOf(resultMap);
        }


    }

    @Getter
    private final String currentNode;


    // If this function fails, then the workflow is retried.
    private void tryRestoreRedundancyAndMergeSegments(Map<CurrentTransferSegment,
            CompletableFuture<CurrentTransferSegmentStatus>> stateMap,
                                                      CorfuRuntime runtime)
            throws OutrankedException {

        // Need to be synchronous here.
        List<Entry<CurrentTransferSegment, CurrentTransferSegmentStatus>> completedEntries
                = stateMap.entrySet().stream().filter(entry -> {
            CompletableFuture<CurrentTransferSegmentStatus> status = entry.getValue();
            return status.isDone();
        }).map(entry -> new SimpleEntry<>(entry.getKey(), entry.getValue()
                .join()))
                .collect(Collectors.toList());

        // If any failed segments exist -> fail. 
        List<Entry<CurrentTransferSegment, CurrentTransferSegmentStatus>> failedEntries =
                completedEntries
                        .stream()
                        .filter(entry -> entry.getValue().getSegmentStateTransferState()
                                .equals(FAILED))
                        .collect(Collectors.toList());

        if (!failedEntries.isEmpty()) {
            throw new IllegalStateException("Transfer failed for one or more segments.");
        }

        // Filter all the segments that have been transferred.
        List<CurrentTransferSegment> transferredSegments = completedEntries
                .stream()
                .filter(entry -> entry.getValue().getSegmentStateTransferState()
                        .equals(TRANSFERRED)).map(Entry::getKey)
                .collect(Collectors.toList());

        if (!transferredSegments.isEmpty()) {
            runtime.invalidateLayout();

            Layout oldLayout = runtime.getLayoutView().getLayout();

            RedundancyCalculator calculator = new RedundancyCalculator(currentNode);

            Layout newLayout =
                    calculator.updateLayoutAfterRedundancyRestoration(
                            transferredSegments, oldLayout);

            if (RedundancyCalculator.canMergeSegments(newLayout)) {
                runtime.getLayoutManagementView().mergeSegments(newLayout);
            } else {
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

    public ImmutableMap<CurrentTransferSegment, CompletableFuture<CurrentTransferSegmentStatus>>
    initTransferAndUpdateMap
            (Map<CurrentTransferSegment, CompletableFuture<CurrentTransferSegmentStatus>> stateMap,
             StateTransferManager manager) {

        Map<CurrentTransferSegment, CompletableFuture<CurrentTransferSegmentStatus>> newStateMap =
                manager.handleTransfer(stateMap).stream()
                        .collect(Collectors.toMap(SimpleEntry::getKey, SimpleEntry::getValue));

        return ImmutableMap.copyOf(newStateMap);
    }

    @Override
    public void impl(@Nonnull CorfuRuntime runtime, @NonNull StreamLog streamLog) throws Exception {

        // Refresh layout.
        runtime.invalidateLayout();

        // Get layout.
        Layout layout = runtime.getLayoutView().getLayout();

        // Create a helper class to perform state calculations.
        RedundancyCalculator redundancyCalculator = new RedundancyCalculator(currentNode);

        // Create a state transfer manager -> should be configurable
        GCAwareTransferBatchProcessor batchProcessor =
                GCAwareTransferBatchProcessor.builder().streamLog(streamLog).build();

        StateTransferWriter transferWriter = StateTransferWriter.builder()
                .batchProcessor(batchProcessor).build();

        StateTransferManager transferManager =
                builder().stateTransferWriter(transferWriter).streamLog(streamLog).build();

        // Create an initial state map.
        ImmutableMap<CurrentTransferSegment, CompletableFuture<CurrentTransferSegmentStatus>> stateMap =
                redundancyCalculator.createStateMap(layout);

        // While redundancy is not restored for all the segments for this node.
        while (!redundancyCalculator.redundancyIsRestored(stateMap)) {

            // Initialize a transfer for each segment and update the map.
            stateMap = initTransferAndUpdateMap(stateMap, transferManager);

            // Try restore redundancy for the segments that are restored.
            // If possible, also merge segments.
            tryRestoreRedundancyAndMergeSegments(stateMap, runtime);

            // Invalidate the layout.
            runtime.invalidateLayout();

            // Get new layout after the consensus.
            layout = runtime.getLayoutView().getLayout();

            // Get the new map from the layout.
            ImmutableMap<CurrentTransferSegment, CompletableFuture<CurrentTransferSegmentStatus>> newLayoutStateMap =
                    redundancyCalculator.createStateMap(layout);

            // Merge the new and the old map into the current map.
            stateMap = redundancyCalculator.mergeMaps(stateMap, newLayoutStateMap);
        }

    }
}
