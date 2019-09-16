package org.corfudb.infrastructure.orchestrator.actions;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.corfudb.infrastructure.orchestrator.actions.RestoreRedundancyMergeSegments.RedundancyCalculator;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.Layout.LayoutSegment;
import org.corfudb.runtime.view.Layout.LayoutStripe;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static org.corfudb.runtime.view.Layout.ReplicationMode.CHAIN_REPLICATION;
import static org.junit.Assert.assertEquals;

class RestoreRedundancyMergeSegmentsTest {


    @Test
    public void testGetStripesNodeIsPresent(){

        String ourNode = "a";
        List<String> layoutServers = Arrays.asList("a", "b", "c");
        List<String> sequencers = new ArrayList<>(layoutServers);
        LayoutStripe stripe1 = new LayoutStripe(Arrays.asList("c", "d"));
        LayoutStripe stripe2 = new LayoutStripe(Arrays.asList("a", "b"));
        LayoutStripe stripe3 = new LayoutStripe(Arrays.asList("e", "f"));

        Set<LayoutStripe> expectedSet = ImmutableSet.of(stripe2);

        LayoutSegment segment1 =
                new LayoutSegment(CHAIN_REPLICATION, 0, 100, ImmutableList.of(stripe1));
        LayoutSegment segment2 =
                new LayoutSegment(CHAIN_REPLICATION, 100, 200, ImmutableList.of(stripe1));
        LayoutSegment segment3 =
                new LayoutSegment(CHAIN_REPLICATION,
                        200, 300, Arrays.asList(stripe1 , stripe2, stripe3));

        Layout layout = new Layout(layoutServers,
                sequencers, Arrays.asList(segment1, segment2, segment3),
                new ArrayList<>(), 0L, UUID.randomUUID());

        Set<LayoutStripe> lastSegmentStripes = RedundancyCalculator.builder()
                .layout(layout)
                .build()
                .getLastSegmentStripes(ourNode);

        assertEquals(expectedSet, lastSegmentStripes);
    }

    @Test
    public void testGetMissingStripesForAllSegments(){

        String ourNode = "a";
        List<String> layoutServers = Arrays.asList("a", "b", "c");
        List<String> sequencers = new ArrayList<>(layoutServers);
        LayoutStripe stripe1 = new LayoutStripe(Arrays.asList("c", "d"));
        LayoutStripe stripe2 = new LayoutStripe(Arrays.asList("a", "b"));
        LayoutStripe stripe3 = new LayoutStripe(Arrays.asList("e", "f"));
        LayoutStripe stripe4 = new LayoutStripe(Arrays.asList("a", "b", "c", "d"));

        LayoutSegment segment1 =
                new LayoutSegment(CHAIN_REPLICATION, 0, 100, ImmutableList.of(stripe2));
        LayoutSegment segment2 =
                new LayoutSegment(CHAIN_REPLICATION, 100, 200, ImmutableList.of(stripe1, stripe3));
        LayoutSegment segment3 =
                new LayoutSegment(CHAIN_REPLICATION,
                        200, 300, Arrays.asList(stripe2, stripe3));
        LayoutSegment segment4 =
                new LayoutSegment(CHAIN_REPLICATION,
                        300, 400, Arrays.asList(stripe1, stripe2, stripe3, stripe4));

        Map<LayoutSegment, Set<LayoutStripe>> expectedResult = ImmutableMap.of(
                segment1, ImmutableSet.of(stripe4),
                segment2, ImmutableSet.of(stripe2, stripe4),
                segment3, ImmutableSet.of(stripe4),
                segment4, ImmutableSet.of());

        Layout layout = new Layout(layoutServers,
                sequencers, Arrays.asList(segment1, segment2, segment3, segment4),
                new ArrayList<>(), 0L, UUID.randomUUID());

        Map<LayoutSegment, Set<LayoutStripe>> missingStripesForAllSegments = RedundancyCalculator
                .builder()
                .layout(layout)
                .build()
                .getMissingStripesForAllSegments(ourNode);

        assertEquals(expectedResult, missingStripesForAllSegments);
    }


    @Test
    public void testGetDonorServers() {
        String ourNode = "a";
        LayoutStripe stripe = new LayoutStripe(Arrays.asList("c", "d"));
        LayoutStripe emptyStripe = new LayoutStripe(new ArrayList<>());
        LayoutStripe stripeWithNode = new LayoutStripe(Collections.singletonList(ourNode));
        Layout layout = new Layout(
                Arrays.asList("a", "b", "c"),
                Arrays.asList("a", "b", "c"),
                Collections.singletonList(new LayoutSegment(CHAIN_REPLICATION, 0, 100,
                        ImmutableList.of(stripe))),
                0L, UUID.randomUUID());

        assertEquals(RedundancyCalculator.getDonorServers(ourNode, emptyStripe), Optional.empty());
        assertEquals(RedundancyCalculator.getDonorServers(ourNode, stripeWithNode), Optional.empty());
        assertEquals(RedundancyCalculator.getDonorServers(ourNode, stripe),
                Optional.of(Arrays.asList("c", "d")));
    }

    @Test
    public void testRedundancyIsRestored() {

    }

}