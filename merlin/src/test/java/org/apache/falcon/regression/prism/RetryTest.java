
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.falcon.regression.prism;

import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.testng.annotations.Test;

public class RetryTest extends BaseTestClass {

    public RetryTest(){
        super();
    }

    @Test(timeOut = 120000, groups = "multiCluster", enabled = false)
    public void FailedFeedReplicationRetry() {

    }

    @Test(timeOut = 120000, groups = "multiCluster", enabled = false)
    public void FailedFeedRetentionRetry() {

    }

    @Test(timeOut = 120000, groups = "singleCluster", enabled = false)
    public void FailedProcessRetry() {

    }

    @Test(timeOut = 120000, groups = "multiCluster", enabled = false)
    public void ReplicationRetry_lateData() {

    }

    @Test(timeOut = 120000, groups = "multiCluster", enabled = false)
    public void ReplicationRetry_lateData_expectedPartition() {

    }

    @Test(timeOut = 120000, groups = "multiCluster", enabled = false)
    public void ReplicationRetry_lateData_otherPartition() {

    }

    @Test(timeOut = 120000, groups = "singleCluster", enabled = false)
    public void ProcessRetry_lateData_compulsoryInput() {

    }

    @Test(timeOut = 120000, groups = "singleCluster", enabled = false)
    public void ProcessRetry_lateData_optionalInput() {

    }

    @Test(timeOut = 120000, groups = "singleCluster", enabled = false)
    public void ProcessRetry_lateData_otherPartition() {

    }
}
