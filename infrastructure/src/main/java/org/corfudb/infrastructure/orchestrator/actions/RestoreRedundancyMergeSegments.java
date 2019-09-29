package org.corfudb.infrastructure.orchestrator.actions;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.BatchProcessor;
import org.corfudb.infrastructure.log.StreamLog;
import org.corfudb.infrastructure.log.statetransfer.StateTransferManager;
import org.corfudb.infrastructure.log.statetransfer.StateTransferWriter;
import org.corfudb.infrastructure.log.statetransfer.batch.CompactionAwareTransferBatchProcessor;
import org.corfudb.infrastructure.log.statetransfer.batch.GCAwareTransferBatchProcessor;
import org.corfudb.infrastructure.log.statetransfer.batch.TransferBatchProcessor;
import org.corfudb.infrastructure.orchestrator.RestoreAction;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.OutrankedException;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.Layout.LayoutSegment;
import org.corfudb.runtime.view.Layout.LayoutStripe;

import static java.util.AbstractMap.*;
import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.*;
import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.SegmentState.FAILED;
import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.SegmentState.NOT_TRANSFERRED;
import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.SegmentState.RESTORED;
import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.SegmentState.TRANSFERRED;

/**
 * This action attempts to restore redundancy for all servers across all segments
 * starting from the oldest segment. It then collapses the segments once the set of
 * servers in the 2 oldest subsequent segments are equal.
 * Created by Zeeshan on 2019-02-06.
 */
@Slf4j
public class RestoreRedundancyMergeSegments extends RestoreAction {

    @Getter
    private final String currentNode;


    // If this function fails, then the workflow is retried.
    private void tryRestoreRedundancyAndMergeSegments(Map<CurrentTransferSegment,
            CompletableFuture<CurrentTransferSegmentStatus>> stateMap,
                                                      CorfuRuntime runtime)
            throws OutrankedException {

        // Filter all the transfers that has been completed.
        List<Entry<CurrentTransferSegment, CurrentTransferSegmentStatus>> completedEntries
                = stateMap.entrySet().stream().filter(entry -> {
            CompletableFuture<CurrentTransferSegmentStatus> status = entry.getValue();
            return status.isDone();
        }).map(entry -> new SimpleEntry<>(entry.getKey(), entry.getValue()
                .join()))
                .collect(Collectors.toList());

        // If any failed transfers exist -> fail.
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
    public void impl(@Nonnull CorfuRuntime runtime,
                     @NonNull StreamLog streamLog,
                     boolean gcCompatible) throws Exception {
        // Refresh layout.
        runtime.invalidateLayout();

        // Get layout.
        Layout layout = runtime.getLayoutView().getLayout();

        // Create a helper class to perform state calculations.
        RedundancyCalculator redundancyCalculator;

        // Create a batch processor depending on the gc configuration.
        TransferBatchProcessor batchProcessor;

        if(!gcCompatible){
            redundancyCalculator = new PrefixTrimRedundancyCalculator(currentNode, runtime);
            batchProcessor = new CompactionAwareTransferBatchProcessor(streamLog);
        }
        else{
            redundancyCalculator = new RedundancyCalculator(currentNode);
            batchProcessor = new GCAwareTransferBatchProcessor(streamLog);
        }

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
