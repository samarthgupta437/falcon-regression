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
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.core.util.Util.URLS;
import org.apache.falcon.regression.core.util.XmlUtil;
import org.apache.falcon.regression.testHelper.BaseMultiClusterTests;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.concurrent.TimeUnit;

public class FeedRetentionTest extends BaseMultiClusterTests {

    private Bundle bundle1, bundle2;

    @BeforeMethod(alwaysRun = true)
    public void setUp(Method method) throws Exception {
        Util.print("test name: " + method.getName());
        //getImpressionRC bundle
        bundle1 = (Bundle) Bundle.readBundle("impressionRC")[0][0];
        bundle1.generateUniqueBundle();
        bundle1 = new Bundle(bundle1, server1.getEnvFileName(), server1.getPrefix());

        bundle2 = (Bundle) Bundle.readBundle("impressionRC")[0][0];
        bundle2.generateUniqueBundle();
        bundle2 = new Bundle(bundle2, server2.getEnvFileName(), server2.getPrefix());
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws Exception {
        prism.getProcessHelper().delete(URLS.DELETE_URL, bundle1.getProcessData());
        prism.getFeedHelper().delete(URLS.DELETE_URL, bundle1.getDataSets().get(0));
        prism.getFeedHelper().delete(URLS.DELETE_URL, bundle1.getDataSets().get(1));
        Bundle.deleteCluster(bundle1, bundle2);
    }

    /** submit 2 clusters
     *  submit and schedule feed on above 2 clusters, both having different locations
     *  submit and schedule process having the above feed as output feed and running on 2
     *  clusters */
    @Test(enabled = true)
    public void testRetentionClickRC_2Colo() throws Exception {
        String inputPath = baseHDFSDir + "/testInput/";
        String inputData = inputPath + "${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}";
        String outputPathTemplate = baseHDFSDir + "/testOutput/op%d/ivoryRetention0%d/%s/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}";

        InstanceUtil.putFileInFolders(server1,
                InstanceUtil.createEmptyDirWithinDatesAndPrefix(server1,
                        InstanceUtil.oozieDateToDate(InstanceUtil.getTimeWrtSystemTime(-5)),
                        InstanceUtil.oozieDateToDate(InstanceUtil.getTimeWrtSystemTime(10)),
                        inputPath, 1), "thriftRRMar0602.gz");
        InstanceUtil.putFileInFolders(server2,
                InstanceUtil.createEmptyDirWithinDatesAndPrefix(server2,
                        InstanceUtil.oozieDateToDate(InstanceUtil.getTimeWrtSystemTime(-5)),
                        InstanceUtil.oozieDateToDate(InstanceUtil.getTimeWrtSystemTime(10)),
                        inputPath, 1), "thriftRRMar0602.gz");

        prism.getClusterHelper().submitEntity(URLS.SUBMIT_URL, bundle1.getClusters().get(0));
        prism.getClusterHelper().submitEntity(URLS.SUBMIT_URL, bundle2.getClusters().get(0));

        String feedOutput01 = bundle1.getFeed("FETL-RequestRC");

        feedOutput01 = InstanceUtil.setFeedCluster(feedOutput01,
                XmlUtil.createValidity("2010-10-01T12:00Z", "2099-01-01T00:00Z"),
                XmlUtil.createRtention("days(10000)", ActionType.DELETE), null,
                ClusterType.SOURCE, null, null);

        feedOutput01 = InstanceUtil.setFeedCluster(feedOutput01,
                XmlUtil.createValidity("2010-10-01T12:00Z", "2099-10-01T12:10Z"),
                XmlUtil.createRtention("minutes(5)", ActionType.DELETE),
                Util.readClusterName(bundle1.getClusters().get(0)), ClusterType.SOURCE,
                "${cluster.colo}",
                String.format(outputPathTemplate, 1, 1, "data"),
                String.format(outputPathTemplate, 1, 1, "stats"),
                String.format(outputPathTemplate, 1, 1, "meta"),
                String.format(outputPathTemplate, 1, 1, "tmp"));

        feedOutput01 = InstanceUtil.setFeedCluster(feedOutput01,
                XmlUtil.createValidity("2010-10-01T12:00Z", "2099-10-01T12:25Z"),
                XmlUtil.createRtention("minutes(5)", ActionType.DELETE),
                Util.readClusterName(bundle2.getClusters().get(0)), ClusterType.SOURCE,
                "${cluster.colo}",
                String.format(outputPathTemplate, 1, 2, "data"),
                String.format(outputPathTemplate, 1, 2, "stats"),
                String.format(outputPathTemplate, 1, 2, "meta"),
                String.format(outputPathTemplate, 1, 2, "tmp"));

        //submit the new output feed
        AssertUtil.assertSucceeded(prism.getFeedHelper().submitEntity(URLS.SUBMIT_URL, feedOutput01));
        TimeUnit.SECONDS.sleep(10);

        String feedOutput02 = bundle1.getFeed("FETL-ImpressionRC");
        feedOutput02 = InstanceUtil.setFeedCluster(feedOutput02,
                XmlUtil.createValidity("2010-10-01T12:00Z", "2099-01-01T00:00Z"),
                XmlUtil.createRtention("days(10000)", ActionType.DELETE), null,
                ClusterType.SOURCE, null, null);

        feedOutput02 = InstanceUtil.setFeedCluster(feedOutput02,
                XmlUtil.createValidity("2010-10-01T12:00Z", "2099-10-01T12:10Z"),
                XmlUtil.createRtention("minutes(5)", ActionType.DELETE),
                Util.readClusterName(bundle1.getClusters().get(0)), ClusterType.SOURCE,
                "${cluster.colo}",
                String.format(outputPathTemplate, 2, 1, "data"),
                String.format(outputPathTemplate, 2, 1, "stats"),
                String.format(outputPathTemplate, 2, 1, "meta"),
                String.format(outputPathTemplate, 2, 1, "tmp"));

        feedOutput02 = InstanceUtil.setFeedCluster(feedOutput02,
                XmlUtil.createValidity("2010-10-01T12:00Z", "2099-10-01T12:25Z"),
                XmlUtil.createRtention("minutes(5)", ActionType.DELETE),
                Util.readClusterName(bundle2.getClusters().get(0)), ClusterType.SOURCE,
                "${cluster.colo}",
                String.format(outputPathTemplate, 2, 2, "data"),
                String.format(outputPathTemplate, 2, 2, "stats"),
                String.format(outputPathTemplate, 2, 2, "meta"),
                String.format(outputPathTemplate, 2, 2, "tmp"));

        //submit the new output feed
        AssertUtil.assertSucceeded(prism.getFeedHelper().submitEntity(URLS.SUBMIT_URL, feedOutput02));
        TimeUnit.SECONDS.sleep(10);

        String feedInput = bundle1.getFeed("FETL2-RRLog");
        feedInput = InstanceUtil
                .setFeedCluster(feedInput,
                        XmlUtil.createValidity("2010-10-01T12:00Z", "2099-01-01T00:00Z"),
                        XmlUtil.createRtention("days(10000)", ActionType.DELETE), null,
                        ClusterType.SOURCE, null, null);

        feedInput = InstanceUtil.setFeedCluster(feedInput,
                XmlUtil.createValidity("2010-10-01T12:00Z", "2099-10-01T12:10Z"),
                XmlUtil.createRtention("minutes(5)", ActionType.DELETE),
                Util.readClusterName(bundle1.getClusters().get(0)), ClusterType.SOURCE,
                "${cluster.colo}", inputData);

        feedInput = InstanceUtil.setFeedCluster(feedInput,
                XmlUtil.createValidity("2010-10-01T12:00Z", "2099-10-01T12:25Z"),
                XmlUtil.createRtention("minutes(5)", ActionType.DELETE),
                Util.readClusterName(bundle2.getClusters().get(0)), ClusterType.SOURCE,
                "${cluster.colo}", inputData);

        AssertUtil.assertSucceeded(prism.getFeedHelper().submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, feedInput));
        TimeUnit.SECONDS.sleep(10);

        String process = bundle1.getProcessData();
        process = InstanceUtil.setProcessCluster(process, null,
                XmlUtil.createProcessValidity("2012-10-01T12:00Z", "2012-10-01T12:10Z"));

        process = InstanceUtil.setProcessCluster(process, Util.readClusterName(bundle1.getClusters().get(0)),
                XmlUtil.createProcessValidity(InstanceUtil.getTimeWrtSystemTime(-2), InstanceUtil.getTimeWrtSystemTime(5)));
        process = InstanceUtil.setProcessCluster(process, Util.readClusterName(bundle2.getClusters().get(0)),
                XmlUtil.createProcessValidity(InstanceUtil.getTimeWrtSystemTime(-2), InstanceUtil.getTimeWrtSystemTime(5)));

        Util.print("process: " + process);

        AssertUtil.assertSucceeded(prism.getProcessHelper().submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, process));

        AssertUtil.assertSucceeded(prism.getFeedHelper().schedule(URLS.SCHEDULE_URL, feedOutput01));
        AssertUtil.assertSucceeded(prism.getFeedHelper().schedule(URLS.SCHEDULE_URL, feedOutput02));
    }

}
