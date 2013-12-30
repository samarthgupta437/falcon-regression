package org.apache.falcon.regression.prism;

import org.apache.falcon.regression.testHelper.BaseSingleClusterTests;
import org.testng.annotations.Test;

public class RetryTest extends BaseSingleClusterTests {

   @Test(timeOut = 120000,groups = "multiCluster",enabled = false)
    public void FailedFeedReplicationRetry() {

   }

    @Test(timeOut = 120000,groups = "multiCluster",enabled = false)
    public void FailedFeedRetentionRetry() {

    }

    @Test(timeOut = 120000,groups = "singleCluster",enabled = false)
    public void FailedProcessRetry() {

    }

    @Test(timeOut = 120000,groups = "multiCluster",enabled = false)
    public void ReplicationRetry_lateData() {

    }

    @Test(timeOut = 120000,groups = "multiCluster",enabled = false)
    public void ReplicationRetry_lateData_expectedPartition() {

    }

    @Test(timeOut = 120000,groups = "multiCluster",enabled = false)
    public void ReplicationRetry_lateData_otherPartition() {

    }

    @Test(timeOut = 120000,groups = "singleCluster",enabled = false)
    public void ProcessRetry_lateData_compulsoryInput() {

    }

    @Test(timeOut = 120000,groups = "singleCluster",enabled = false)
    public void ProcessRetry_lateData_optionalInput() {

    }

    @Test(timeOut = 120000,groups = "singleCluster",enabled = false)
    public void ProcessRetry_lateData_otherPartition() {

    }
}
