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

import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.generated.feed.ActionType;
import org.apache.falcon.regression.core.generated.feed.ClusterType;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.core.util.Util.URLS;
import org.apache.falcon.regression.core.util.XmlUtil;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import java.util.Properties;

@Test(groups = "embedded")
public class FeedReplicationS4 extends BaseTestClass {

    ColoHelper cluster1 = servers.get(0);
    ColoHelper cluster2 = servers.get(1);
    String baseTestHDFSDir = baseHDFSDir + "/FeedReplicationS4/${YEAR}/${MONTH}/${DAY}/${HOUR}";
    String aggregateWorkflowDir = baseHDFSDir + "/FeedReplicationS4/aggregator";
    String s4location;

    public FeedReplicationS4(){
        Properties merlinProp = Util.getPropertiesObj(MERLIN_PROPERTIES);
        s4location = merlinProp.getProperty("base_s4_location");
    }

    @BeforeClass
    public void uploadWorkflow() throws Exception {
        uploadDirToClusters(aggregateWorkflowDir, OSUtil.RESOURCES_OOZIE);
    }

    @BeforeMethod(alwaysRun = true)
    public void setupTest() throws Exception {
        Bundle bundle = (Bundle) Bundle.readBundle("S4Replication")[0][0];
        bundles[0] = new Bundle(bundle, cluster1);
        bundles[1] = new Bundle(bundle, cluster2);
        bundles[0].generateUniqueBundle();
        bundles[1].generateUniqueBundle();
        bundles[0].setProcessWorkflow(aggregateWorkflowDir);
        bundles[1].setProcessWorkflow(aggregateWorkflowDir);
    }

    @AfterMethod
    public void tearDown() throws Exception {
        removeBundles();
    }

    @Test(enabled = true, timeOut = 1200000)
    public void ReplicationFromS4() throws Exception {
        //Bundle.submitCluster(bundles[0],bundles[1]);
        String feedOutput01 = bundles[0].getDataSets().get(0);
        feedOutput01 = InstanceUtil
                .setFeedCluster(feedOutput01,
                        XmlUtil.createValidity("2010-10-01T12:00Z", "2099-01-01T00:00Z"),
                        XmlUtil.createRtention("days(10000)", ActionType.DELETE), null,
                        ClusterType.SOURCE, null);
        feedOutput01 = InstanceUtil
                .setFeedCluster(feedOutput01,
                        XmlUtil.createValidity("2012-12-06T05:00Z", "2099-10-01T12:10Z"),
                        XmlUtil.createRtention("minutes(5)", ActionType.DELETE),
                        Util.readClusterName(bundles[1].getClusters().get(0)), ClusterType.SOURCE, "",
                        s4location);
        feedOutput01 = InstanceUtil
                .setFeedCluster(feedOutput01,
                        XmlUtil.createValidity("2012-12-06T05:00Z", "2099-10-01T12:25Z"),
                        XmlUtil.createRtention("minutes(5)", ActionType.DELETE),
                        Util.readClusterName(bundles[0].getClusters().get(0)), ClusterType.TARGET, "",
                        baseTestHDFSDir);
        System.out.println(feedOutput01);
        Bundle.submitCluster(bundles[0], bundles[1]);
        prism.getFeedHelper()
                .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, feedOutput01);
    }
}
