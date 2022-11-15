package org.ihtsdo.otf.traceabilityservice.service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class BranchUtil {

	private BranchUtil() {
	}

	public static boolean isCodeSystemBranch(String branch) {
		return branch.equals("MAIN") || branch.startsWith("SNOMEDCT-", branch.lastIndexOf("/") + 1);
	}

	/**
	 * Return the full branchPath, including the full branchPath of each ancestor in descending order.
	 *
	 * @param branchPath Path of branch to process.
	 * @return The full branchPath, including the full branchPath of each ancestor in descending order.
	 */
	public static List<String> getNodes(String branchPath) {
		return getNodes(branchPath, true);
	}

	/**
	 * Return the full branchPath, including the full branchPath of each ancestor, in either descending or ascending order.
	 *
	 * @param branchPath Path of branch to process.
	 * @param descending Control whether response is in descending or ascending order.
	 * @return The full branchPath, including the full branchPath of each ancestor, in either descending or ascending order.
	 */
	public static List<String> getNodes(String branchPath, boolean descending) {
		if (branchPath == null || branchPath.isEmpty()) {
			return Collections.emptyList();
		}

		List<String> branchPathHierarchy = getBranchPathHierarchyAsc(branchPath);
		int hierarchySize = branchPathHierarchy.size();
		List<String> nodes = new ArrayList<>();
		boolean rootNode = false;
		for (int x = 0; x < hierarchySize; x++) {
			String node = branchPathHierarchy.get(x);

			if (isRootNode(node)) {
				rootNode = true;
			}

			if (!isLastIteration(x, hierarchySize)) {
				node = prependParentNodes(x, hierarchySize, branchPathHierarchy, node);
			}

			nodes.add(node);

			// Do not process any more ancestor nodes, e.g.
			// MAIN/SNOMEDCT-XX will be the root for the given branchPath.
			if (rootNode) {
				break;
			}
		}

		if (descending) {
			Collections.reverse(nodes);
		}

		return nodes;
	}

	private static List<String> getBranchPathHierarchyAsc(String branchPath) {
		List<String> hierarchy = Arrays.asList(branchPath.split("/"));
		Collections.reverse(hierarchy);

		// [0] => Task, [1] => Project, [2] => CodeSystem
		return hierarchy;
	}

	private static boolean isRootNode(String node) {
		return node.contains("SNOMEDCT-");
	}

	private static boolean isLastIteration(int currentIteration, int totalIterations) {
		return currentIteration++ == totalIterations - 1;
	}

	private static String prependParentNodes(int x, int hierarchySize, List<String> branchPathHierarchy, String node) {
		for (int y = x + 1; y < hierarchySize; y++) {
			String parentNode = branchPathHierarchy.get(y);
			node = parentNode + "/" + node;
		}

		return node;
	}
}
