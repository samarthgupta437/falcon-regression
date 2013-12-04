package com.inmobi.qa.falcon.supportClasses;

import com.inmobi.qa.falcon.util.Util;

public enum GetBundle {

	BillingFeedReplicationBundle("LocalDC_feedReplicaltion_BillingRC"), RegularBundle("src/test/resources/ELbundle");

	private final String value;

	private GetBundle(String value) {
		Util.print("GetBundle of enum bundle path is: " + value);
		this.value = value;
	}

	public String getValue() {
		Util.print("getValue of enum bundle path is: "+value);
		return value;
	}

}
