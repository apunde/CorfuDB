package org.corfudb.infrastructure.orchestrator.actions;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.orchestrator.Action;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.Layout.LayoutSegment;
import org.corfudb.runtime.view.Layout.LayoutStripe;

/**
 * This action attempts to restore redundancy for all servers across all segments
 * starting from the oldest segment. It then collapses the segments once the set of
 * servers in the 2 oldest subsequent segments are equal.
 * Created by Zeeshan on 2019-02-06.
 */
@Slf4j
public class RestoreRedundancyMergeSegments extends Action {

    @Builder
    public static class RedundancyCalculator {

        @NonNull
        private final Layout layout;

        /**
         * Get the stripes of the last segment for which the node is present.
         *
         * @param server The address of a node.
         * @return The stripes of the last segment.
         */
        @VisibleForTesting
        Set<LayoutStripe> getLastSegmentStripes(String server) {
            List<LayoutStripe> stripesOfLastSegment = layout.getLatestSegment().getStripes();
            return stripesOfLastSegment
                    .stream()
                    .filter(stripe -> stripe.getLogServers().contains(server))
                    .collect(Collectors.toSet());
        }

        /**
         *  Get the map from the segments to the set of stripes that do not contain this node.
         *
         * @param server The address of a node.
         * @return The map for all segments to the set of stripes.
         */
        public Map<LayoutSegment, Set<LayoutStripe>> getMissingStripesForAllSegments(String server) {
            Set<LayoutStripe> stripesToBeRestored = getLastSegmentStripes(server);

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

        /**
         * Check that this node is present for all the stripes in all the segments.
         *
         * @param server The address of a node.
         * @return True if redundancy is completely restored and false otherwise.
         */
        public boolean redundancyIsRestored(String server){
            Map<LayoutSegment, Set<LayoutStripe>> missingStripesForAllSegments
                    = getMissingStripesForAllSegments(server);
            return missingStripesForAllSegments
                    .entrySet()
                    .stream()
                    .allMatch(entry -> entry.getValue().isEmpty());
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
        RedundancyCalculator redundancyCalculator = RedundancyCalculator.builder().layout(layout).build();

        while(!redundancyCalculator.redundancyIsRestored(currentNode)){
            LayoutSegment segment = layout.getFirstSegment();
            Map<LayoutSegment, Set<LayoutStripe>> missingStripesForAllSegments
                    = redundancyCalculator.getMissingStripesForAllSegments(currentNode);
            Set<LayoutStripe> layoutStripes = missingStripesForAllSegments.get(segment);
            for(LayoutStripe stripe: layoutStripes){
                StateTransfer.transferGarbage(layout, currentNode, runtime, segment, layoutStripes);
            }
        }

        // Each segment is compared with the first segment. The data is restored in any new LogUnit nodes and then
        // merged to this segment. This is done for all the segments.
        final int layoutSegmentToMergeTo = 0;

        // Catchup all servers across all segments.
        while (layout.getSegments().size() > 1) {

            Set<String> lowRedundancyServers = getNodesWithReducedRedundancy(layout, layoutSegmentToMergeTo);

            // Currently the state is transferred for the complete segment.
            // TODO: Add stripe specific transfer granularity for optimization.
            // Transfer the replicated segment to the difference set calculated above.
            for (String lowRedundancyServer : lowRedundancyServers) {
                StateTransfer.transfer(layout, lowRedundancyServer, runtime, layout.getFirstSegment());
            }

            // Merge the 2 segments.
            runtime.getLayoutManagementView().mergeSegments(new Layout(layout));

            // Refresh layout
            runtime.invalidateLayout();
            layout = runtime.getLayoutView().getLayout();
        }


    }
}
