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
import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.core.util.Util.URLS;
import org.apache.falcon.regression.core.util.XmlUtil;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.hadoop.fs.FileSystem;
import org.apache.oozie.client.CoordinatorAction.Status;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;

public class PrismFeedReplicationUpdateTest extends BaseTestClass {

    ColoHelper cluster1;
    ColoHelper cluster2;
    ColoHelper cluster3;
    FileSystem cluster1FS;
    FileSystem cluster2FS;
    FileSystem cluster3FS;
    private String cluster1Colo;
    private String cluster2Colo;
    private String cluster3Colo;
    private Bundle bundle1, bundle2, bundle3;
    private final String inputPath = baseHDFSDir + "/input-data/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}";
    private String alternativeInputPath = baseHDFSDir + "/newFeedPath/input-data/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}";

    public PrismFeedReplicationUpdateTest(){
        super();
        cluster1 = servers.get(0);
        cluster2 = servers.get(1);
        cluster3 = servers.get(2);
        cluster1FS = serverFS.get(0);
        cluster2FS = serverFS.get(1);
        cluster3FS = serverFS.get(2);
        cluster1Colo = cluster1.getClusterHelper().getColo().split("=")[1];
        cluster2Colo = cluster2.getClusterHelper().getColo().split("=")[1];
        cluster3Colo = cluster3.getClusterHelper().getColo().split("=")[1];
    }

    @BeforeMethod(alwaysRun = true)
    public void setUp(Method method) throws Exception {
        Util.print("test name: " + method.getName());
        Bundle bundle = Util.readELBundles()[0][0];

        bundle1 = new Bundle(bundle, cluster1.getEnvFileName(), cluster1.getPrefix());
        bundle2 = new Bundle(bundle, cluster2.getEnvFileName(), cluster2.getPrefix());
        bundle3 = new Bundle(bundle, cluster3.getEnvFileName(), cluster3.getPrefix());

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
        HadoopUtil.deleteDirIfExists(prefix.substring(1), cluster2FS);
        Util.lateDataReplenish(cluster2, 5, 80, prefix, postFix);


        postFix = "/UK/ua3";
        prefix = bundle1.getFeedDataPathPrefix();
        HadoopUtil.deleteDirIfExists(prefix.substring(1), cluster3FS);
        Util.lateDataReplenish(cluster3, 5, 80, prefix, postFix);

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

        Assert.assertEquals(InstanceUtil.checkIfFeedCoordExist(cluster2.getFeedHelper(),
                Util.readDatasetName(feed),
                                "REPLICATION"), 0);
        Assert.assertEquals(InstanceUtil.checkIfFeedCoordExist(cluster2.getFeedHelper(),
                Util.readDatasetName(feed),
                                "RETENTION"), 2);
        Assert.assertEquals(InstanceUtil.checkIfFeedCoordExist(cluster3.getFeedHelper(),
                Util.readDatasetName(feed),
                                "REPLICATION"), 0);
        Assert.assertEquals(InstanceUtil.checkIfFeedCoordExist(cluster3.getFeedHelper(),
                Util.readDatasetName(feed),
                                "RETENTION"), 2);
        Assert.assertEquals(InstanceUtil.checkIfFeedCoordExist(cluster1.getFeedHelper(), Util.readDatasetName(feed),
                                "REPLICATION"), 4);
        Assert.assertEquals(InstanceUtil.checkIfFeedCoordExist(cluster1.getFeedHelper(), Util.readDatasetName(feed),
                                "RETENTION"), 2);
    }

    @Test(enabled = true, timeOut = 1200000)
    public void updateFeed_dependentProcessTest() throws Exception {
        //set cluster colos
        bundle1.setCLusterColo(cluster1Colo);
        Util.print("cluster bundle1: " + bundle1.getClusters().get(0));
        bundle2.setCLusterColo(cluster2Colo);
        Util.print("cluster bundle2: " + bundle2.getClusters().get(0));
        bundle3.setCLusterColo(cluster3Colo);
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
        HadoopUtil.deleteDirIfExists(prefix.substring(1), cluster1FS);
        Util.lateDataReplenish(cluster1, 70, 1, prefix, null);

        prefix = InstanceUtil.getFeedPrefix(feed02);
        HadoopUtil.deleteDirIfExists(prefix.substring(1), cluster3FS);
        Util.lateDataReplenish(cluster3, 70, 1, prefix, null);

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
            Status status1 = InstanceUtil.getInstanceStatus(cluster1, Util.getProcessName(process), 0, 0);
            Status status2 = InstanceUtil.getInstanceStatus(cluster3,
                    Util.getProcessName(process), 0, 0);
            if (status1 != null && status2 != null &&
                    (status1 == Status.RUNNING || status1 == Status.SUCCEEDED)
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
