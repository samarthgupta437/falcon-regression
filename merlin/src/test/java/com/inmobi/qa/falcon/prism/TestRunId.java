package com.inmobi.qa.falcon.prism;

import java.util.List;

import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.client.WorkflowJob;

public class TestRunId {

	public static void main(String[] args) throws OozieClientException {
		OozieClient client = new OozieClient("http://mk-qa-63:11000/oozie/");
		CoordinatorJob coordInfo = client.getCoordJobInfo("0000248-120618130937367-testuser-C");
		List<CoordinatorAction> actions = coordInfo.getActions();
		for(CoordinatorAction action:actions){
			WorkflowJob job = client.getJobInfo(action.getExternalId());
			System.out.println(job.getRun());
		}
	}
}
