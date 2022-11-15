package org.ihtsdo.otf.traceabilityservice.service;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class BranchUtilTest {
    @Test
    public void getNodes_ShouldReturnExpected_WhenInternationalWithNoTrailing() {
        // given
        String branchPath = "MAIN/PROJECT/TASK-123";

        // when
        List<String> nodes = BranchUtil.getNodes(branchPath);

        // then
        assertEquals(3, nodes.size());
        assertEquals("MAIN", nodes.get(0));
        assertEquals("MAIN/PROJECT", nodes.get(1));
        assertEquals("MAIN/PROJECT/TASK-123", nodes.get(2));
    }

    @Test
    public void getNodes_ShouldReturnExpected_WhenInternationalWithTrailing() {
        // given
        String branchPath = "MAIN/PROJECT/TASK-123/";

        // when
        List<String> nodes = BranchUtil.getNodes(branchPath);

        // then
        assertEquals(3, nodes.size());
        assertEquals("MAIN", nodes.get(0));
        assertEquals("MAIN/PROJECT", nodes.get(1));
        assertEquals("MAIN/PROJECT/TASK-123", nodes.get(2));
    }

    @Test
    public void getNodes_ShouldReturnExpected_WhenExtensionWithNoTrailing() {
        // given
        String branchPath = "MAIN/SNOMEDCT-XX/PROJECT/TASK-123";

        // when
        List<String> nodes = BranchUtil.getNodes(branchPath);

        // then
        assertEquals(3, nodes.size());
        assertEquals("MAIN/SNOMEDCT-XX", nodes.get(0));
        assertEquals("MAIN/SNOMEDCT-XX/PROJECT", nodes.get(1));
        assertEquals("MAIN/SNOMEDCT-XX/PROJECT/TASK-123", nodes.get(2));
    }

    @Test
    public void getNodes_ShouldReturnExpected_WhenExtensionWithTrailing() {
        // given
        String branchPath = "MAIN/SNOMEDCT-XX/PROJECT/TASK-123/";

        // when
        List<String> nodes = BranchUtil.getNodes(branchPath);

        // then
        assertEquals(3, nodes.size());
        assertEquals("MAIN/SNOMEDCT-XX", nodes.get(0));
        assertEquals("MAIN/SNOMEDCT-XX/PROJECT", nodes.get(1));
        assertEquals("MAIN/SNOMEDCT-XX/PROJECT/TASK-123", nodes.get(2));
    }

    @Test
    public void getNodes_ShouldReturnExpected_WhenAscending() {
        // given
        String branchPath = "MAIN/SNOMEDCT-XX/PROJECT/TASK-123";

        // when
        List<String> nodes = BranchUtil.getNodes(branchPath, false);

        // then
        assertEquals(3, nodes.size());
        assertEquals("MAIN/SNOMEDCT-XX/PROJECT/TASK-123", nodes.get(0));
        assertEquals("MAIN/SNOMEDCT-XX/PROJECT", nodes.get(1));
        assertEquals("MAIN/SNOMEDCT-XX", nodes.get(2));
    }

    @Test
    public void getNodes_ShouldNotReturnSame_WhenDifferentSortsGiven() {
        // given
        String branchPath = "MAIN/SNOMEDCT-XX/PROJECT/TASK-123";

        // when
        List<String> ascending = BranchUtil.getNodes(branchPath, false);
        List<String> descending = BranchUtil.getNodes(branchPath, true);

        // then
        assertNotEquals(ascending, descending);
        Collections.reverse(descending);
        assertEquals(ascending, descending);
    }
}
