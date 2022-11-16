package org.ihtsdo.otf.traceabilityservice.service;


public class BranchUtil {

	private BranchUtil() {
	}

	public static boolean isCodeSystemBranch(String branch) {
		return branch.equals("MAIN") || branch.startsWith("SNOMEDCT-", branch.lastIndexOf("/") + 1);
	}
}
