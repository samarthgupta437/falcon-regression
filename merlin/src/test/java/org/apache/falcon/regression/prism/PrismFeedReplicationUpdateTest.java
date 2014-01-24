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
import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.core.util.Util.URLS;
import org.apache.falcon.regression.core.util.XmlUtil;
import org.apache.falcon.regression.testHelper.BaseMultiClusterTests;
import org.apache.oozie.client.CoordinatorAction.Status;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;

public class PrismFeedReplicationUpdateTest extends BaseMultiClusterTests {

    private final String inputPath = baseHDFSDir + "/input-data/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}";
    private String alternativeInputPath = baseHDFSDir + "/newFeedPath/input-data/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}";
    private Bundle bundle1, bundle2, bundle3;

    private String server1Colo = server1.getClusterHelper().getColo().split("=")[1];
    private String server2Colo = server2.getClusterHelper().getColo().split("=")[1];
    private String server3Colo = server3.getClusterHelper().getColo().split("=")[1];

    @BeforeMethod(alwaysRun = true)
    public void setUp(Method method) throws Exception {
        Util.print("test name: " + method.getName());
        Bundle bundle = Util.readELBundles()[0][0];

        bundle1 = new Bundle(bundle, server1.getEnvFileName(), server1.getPrefix());
        bundle2 = new Bundle(bundle, server2.getEnvFileName(), server2.getPrefix());
        bundle3 = new Bundle(bundle, server3.getEnvFileName(), server3.getPrefix());

        bundle1.generateUniqueBundle();
        bundle2.generateUniqueBundle();
        bundle3.generateUniqueBundle();
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws Exception {
        bundle1.deleteBundle(prism);
        bundle2.deleteBundle(prism);
        bundle3.deleteBundle(prism);
    }

    @Test(enabled = true, timeOut = 1200000)
    public void multipleSourceOneTarget() throws Exception {

        bundle1.setInputFeedDataPath(inputPath);
        Bundle.submitCluster(bundle1, bundle2, bundle3);

        String feed = bundle1.getDataSets().get(0);
        feed = InstanceUtil.setFeedCluster(feed,
                XmlUtil.createValidity("2009-02-01T00:00Z", "2012-01-01T00:00Z"),
                XmlUtil.createRtention("hours(10)", ActionType.DELETE), null,
                ClusterType.SOURCE, null, null);

        String postFix = "/US/ua2";
        String prefix = bundle1.getFeedDataPathPrefix();
        HadoopUtil.deleteDirIfExists(prefix.substring(1), server2FS);
        Util.lateDataReplenish(server2, 5, 80, prefix, postFix);


        postFix = "/UK/ua3";
        prefix = bundle1.getFeedDataPathPrefix();
        HadoopUtil.deleteDirIfExists(prefix.substring(1), server3FS);
        Util.lateDataReplenish(server3, 5, 80, prefix, postFix);

        String startTime = InstanceUtil.getTimeWrtSystemTime(-30);


        feed = InstanceUtil.setFeedCluster(feed, XmlUtil.createValidity(startTime,
                        InstanceUtil.addMinsToTime(startTime, 85)),
                        XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                        Util.readClusterName(bundle2.getClusters().get(0)), ClusterType.SOURCE,
                        "US/${cluster.colo}", null);

        feed = InstanceUtil.setFeedCluster(feed,
                XmlUtil.createValidity(InstanceUtil.addMinsToTime(startTime, 20),
                        InstanceUtil.addMinsToTime(startTime, 105)),
                XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                Util.readClusterName(bundle1.getClusters().get(0)), ClusterType.TARGET, null, null);

        feed = InstanceUtil.setFeedCluster(feed,
                XmlUtil.createValidity(InstanceUtil.addMinsToTime(startTime, 40),
                        InstanceUtil.addMinsToTime(startTime, 130)),
                XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                Util.readClusterName(bundle3.getClusters().get(0)), ClusterType.SOURCE,
                "UK/${cluster.colo}", null);

        Util.print("feed: " + feed);

        prism.getFeedHelper().submitEntity(URLS.SUBMIT_URL, feed);
        Thread.sleep(10000);

        prism.getFeedHelper().schedule(URLS.SCHEDULE_URL, feed);
        Thread.sleep(30000);

        //change feed location path
        feed = InstanceUtil.setFeedFilePath(feed, alternativeInputPath);

        Util.print("updated feed: " + feed);

        //update feed
        prism.getFeedHelper().update(feed, feed);
        Thread.sleep(30000);

        Assert.assertEquals(InstanceUtil.checkIfFeedCoordExist(server2.getFeedHelper(), Util.readDatasetName(feed),
                                "REPLICATION"), 0);
        Assert.assertEquals(InstanceUtil.checkIfFeedCoordExist(server2.getFeedHelper(), Util.readDatasetName(feed),
                                "RETENTION"), 2);
        Assert.assertEquals(InstanceUtil.checkIfFeedCoordExist(server3.getFeedHelper(), Util.readDatasetName(feed),
                                "REPLICATION"), 0);
        Assert.assertEquals(InstanceUtil.checkIfFeedCoordExist(server3.getFeedHelper(), Util.readDatasetName(feed),
                                "RETENTION"), 2);
        Assert.assertEquals(InstanceUtil.checkIfFeedCoordExist(server1.getFeedHelper(), Util.readDatasetName(feed),
                                "REPLICATION"), 4);
        Assert.assertEquals(InstanceUtil.checkIfFeedCoordExist(server1.getFeedHelper(), Util.readDatasetName(feed),
                                "RETENTION"), 2);
    }

    @Test(enabled = true, timeOut = 1200000)
    public void updateFeed_dependentProcessTest() throws Exception {
        //set cluster colos
        bundle1.setCLusterColo(server1Colo);
        Util.print("cluster bundle1: " + bundle1.getClusters().get(0));
        bundle2.setCLusterColo(server2Colo);
        Util.print("cluster bundle2: " + bundle2.getClusters().get(0));
        bundle3.setCLusterColo(server3Colo);
        Util.print("cluster bundle3: " + bundle3.getClusters().get(0));

        //submit 3 clusters
        Bundle.submitCluster(bundle1, bundle2, bundle3);

        //get 2 unique feeds
        String feed01 = Util.getInputFeedFromBundle(bundle1);
        String feed02 = Util.getInputFeedFromBundle(bundle2);
        String outputFeed = Util.getOutputFeedFromBundle(bundle1);

        //set clusters to null;
        feed01 = InstanceUtil.setFeedCluster(feed01,
                        XmlUtil.createValidity("2009-02-01T00:00Z", "2012-01-01T00:00Z"),
                        XmlUtil.createRtention("hours(10)", ActionType.DELETE), null,
                        ClusterType.SOURCE, null, null);

        feed02 = InstanceUtil.setFeedCluster(feed02,
                        XmlUtil.createValidity("2009-02-01T00:00Z", "2012-01-01T00:00Z"),
                        XmlUtil.createRtention("hours(10)", ActionType.DELETE), null,
                        ClusterType.SOURCE, null, null);

        outputFeed = InstanceUtil.setFeedCluster(outputFeed,
                        XmlUtil.createValidity("2009-02-01T00:00Z", "2012-01-01T00:00Z"),
                        XmlUtil.createRtention("hours(10)", ActionType.DELETE), null,
                        ClusterType.SOURCE, null, null);

        //set new feed input data
        feed01 = Util.setFeedPathValue(feed01, baseHDFSDir + "/feed01/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}/");
        feed02 = Util.setFeedPathValue(feed02, baseHDFSDir + "/feed02/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}/");

        //generate data in both the colos ua1 and ua3
        String prefix = InstanceUtil.getFeedPrefix(feed01);
        HadoopUtil.deleteDirIfExists(prefix.substring(1), server1FS);
        Util.lateDataReplenish(server1, 70, 1, prefix);

        prefix = InstanceUtil.getFeedPrefix(feed02);
        HadoopUtil.deleteDirIfExists(prefix.substring(1), server3FS);
        Util.lateDataReplenish(server3, 70, 1, prefix);

        String startTime = InstanceUtil.getTimeWrtSystemTime(-50);

        //set clusters for feed01
        feed01 = InstanceUtil.setFeedCluster(feed01, XmlUtil.createValidity(startTime, "2099-01-01T00:00Z"),
                        XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                        Util.readClusterName(bundle1.getClusters().get(0)), ClusterType.SOURCE,
                        null, null);

        feed01 = InstanceUtil.setFeedCluster(feed01, XmlUtil.createValidity(startTime, "2099-01-01T00:00Z"),
                        XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                        Util.readClusterName(bundle3.getClusters().get(0)), ClusterType.TARGET,
                        null, null);

        //set clusters for feed02
        feed02 = InstanceUtil.setFeedCluster(feed02, XmlUtil.createValidity(startTime, "2099-01-01T00:00Z"),
                        XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                        Util.readClusterName(bundle1.getClusters().get(0)), ClusterType.TARGET,
                        null, null);

        feed02 = InstanceUtil.setFeedCluster(feed02, XmlUtil.createValidity(startTime, "2099-01-01T00:00Z"),
                        XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                        Util.readClusterName(bundle3.getClusters().get(0)), ClusterType.SOURCE,
                        null, null);

        //set clusters for output feed
        outputFeed = InstanceUtil.setFeedCluster(outputFeed,
                XmlUtil.createValidity(startTime, "2099-01-01T00:00Z"),
                XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                Util.readClusterName(bundle1.getClusters().get(0)), ClusterType.SOURCE, null, null);

        outputFeed = InstanceUtil.setFeedCluster(outputFeed,
                XmlUtil.createValidity(startTime, "2099-01-01T00:00Z"),
                XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                Util.readClusterName(bundle3.getClusters().get(0)), ClusterType.TARGET, null, null);


        //submit and schedule feeds
        Util.print("feed01: " + feed01);
        Util.print("feed02: " + feed02);
        Util.print("outputFeed: " + outputFeed);

        prism.getFeedHelper().submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, feed01);
        prism.getFeedHelper().submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, feed02);
        prism.getFeedHelper().submitAndSchedule(URLS.SUBMIT_URL, outputFeed);


        //create a process with 2 clusters

        //get a process
        String process = bundle1.getProcessData();

        //add clusters to process
        String processStartTime = InstanceUtil.getTimeWrtSystemTime(-11);
        String processEndTime = InstanceUtil.getTimeWrtSystemTime(70);

        process = InstanceUtil.setProcessCluster(process, null,
                XmlUtil.createProcessValidity(startTime, "2099-01-01T00:00Z"));

        process = InstanceUtil.setProcessCluster(process, Util.readClusterName(bundle1.getClusters().get(0)),
                XmlUtil.createProcessValidity(processStartTime, processEndTime));

        process = InstanceUtil.setProcessCluster(process, Util.readClusterName(bundle3.getClusters().get(0)),
                XmlUtil.createProcessValidity(processStartTime, processEndTime));
        process = InstanceUtil.addProcessInputFeed(process, Util.readDatasetName(feed02), Util.readDatasetName(feed02));

        //submit and schedule process
        Util.print("process: " + process);

        prism.getProcessHelper().submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, process);

        Util.print("Wait till process goes into running ");

        for (int i = 0; i < 30; i++) {
            Status status1 = InstanceUtil.getInstanceStatus(server1, Util.getProcessName(process), 0, 0);
            Status status2 = InstanceUtil.getInstanceStatus(server3, Util.getProcessName(process), 0, 0);
            if ((status1 == Status.RUNNING || status1 == Status.SUCCEEDED)
                    && (status2 == Status.RUNNING || status2 == Status.SUCCEEDED)) {
                break;
            }
            Thread.sleep(20000);
        }

        feed01 = InstanceUtil.setFeedFilePath(feed01, alternativeInputPath);
        Util.print("updated feed: " + feed01);
        prism.getFeedHelper().update(feed01, feed01);
    }
}
