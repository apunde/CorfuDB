package org.corfudb.infrastructure.orchestrator.actions;

import com.google.common.collect.Sets;

import java.util.Set;

import javax.annotation.Nonnull;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.orchestrator.Action;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.Layout;

/**
 * This action attempts to restore redundancy for all servers across all segments
 * starting from the oldest segment. It then collapses the segments once the set of
 * servers in the 2 oldest subsequent segments are equal.
 * Created by Zeeshan on 2019-02-06.
 */
@Slf4j
public class RestoreRedundancyMergeSegments extends Action {

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

    @Getter
    @Setter
    private String localEndpoint;

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
        // Each segment is compared with the first segment. The data is restored in any new LogUnit nodes and then
        // merged to this segment. This is done for all the segments.
        final int layoutSegmentToMergeTo = 0;

        // Catchup all servers across all segments.

        long startTransferTime = System.currentTimeMillis();
        while (layout.getSegments().size() > 1) {

            Set<String> lowRedundancyServers = getNodesWithReducedRedundancy(layout, layoutSegmentToMergeTo);

            // Currently the state is transferred for the complete segment.
            // TODO: Add stripe specific transfer granularity for optimization.
            // Transfer the replicated segment to the difference set calculated above.
            for (String lowRedundancyServer : lowRedundancyServers) {
                // StateTransfer.transfer(layout, lowRedundancyServer, runtime, layout.getFirstSegment());
                ZeroCopyStateTransfer.transfer(layout, localEndpoint, lowRedundancyServer, runtime, layout.getFirstSegment());
            }

            // Merge the 2 segments.
            runtime.getLayoutManagementView().mergeSegments(new Layout(layout));

            // Refresh layout
            runtime.invalidateLayout();
            layout = runtime.getLayoutView().getLayout();
        }
        log.info("End transfer time: {}", System.currentTimeMillis() - startTransferTime);
    }
}
