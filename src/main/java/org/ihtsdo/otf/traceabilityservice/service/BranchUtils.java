package org.ihtsdo.otf.traceabilityservice.service;

import java.util.HashSet;
import java.util.Set;

public class BranchUtils {

	private BranchUtils() {
	}

	public static boolean isCodeSystemBranch(String branch) {
		return branch.equals("MAIN") || branch.startsWith("SNOMEDCT-", branch.lastIndexOf("/") + 1);
	}
	
	public static Set<String> getAncestorBranches(String branchPath) {
		Set<String> ancestorBranches = new HashSet<>();
		//Where is first slash? Work forward from there collecting more paths until the last one is found.
		//This will not include the full path
		int thisSlash = branchPath.indexOf('/');
		while (thisSlash != -1) {
			ancestorBranches.add(branchPath.substring(0,thisSlash));
			thisSlash = branchPath.indexOf('/', thisSlash + 1);
		}
		return ancestorBranches;
	}
}
