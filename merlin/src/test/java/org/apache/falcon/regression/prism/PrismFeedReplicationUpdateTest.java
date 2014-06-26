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
import org.apache.falcon.entity.v0.feed.ActionType;
import org.apache.falcon.entity.v0.feed.ClusterType;
import org.apache.falcon.regression.core.enumsAndConstants.ENTITY_TYPE;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.core.util.TimeUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.core.util.Util.URLS;
import org.apache.falcon.regression.core.util.XmlUtil;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.apache.oozie.client.CoordinatorAction.Status;
import org.apache.oozie.client.Job;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.lang.reflect.Method;

@Test(groups = "embedded")
public class PrismFeedReplicationUpdateTest extends BaseTestClass {

    ColoHelper cluster1 = servers.get(0);
    ColoHelper cluster2 = servers.get(1);
    ColoHelper cluster3 = servers.get(2);
    FileSystem cluster1FS = serverFS.get(0);
    FileSystem cluster2FS = serverFS.get(1);
    FileSystem cluster3FS = serverFS.get(2);
    String cluster1Colo = cluster1.getClusterHelper().getColoName();
    String cluster2Colo = cluster2.getClusterHelper().getColoName();
    String cluster3Colo = cluster3.getClusterHelper().getColoName();
    private final String baseTestDir = baseHDFSDir + "/PrismFeedReplicationUpdateTest";
    private final String inputPath =
        baseTestDir + "/input-data/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}";
    private String alternativeInputPath =
        baseTestDir + "/newFeedPath/input-data/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}";
    private String aggregateWorkflowDir = baseTestDir + "/aggregator";
    private static final Logger logger = Logger.getLogger(PrismFeedReplicationUpdateTest.class);

    @BeforeClass(alwaysRun = true)
    public void prepareCluster() throws IOException {
        // upload workflow to hdfs
        uploadDirToClusters(aggregateWorkflowDir, OSUtil.RESOURCES_OOZIE);
    }

    @BeforeMethod(alwaysRun = true)
    public void setUp(Method method) throws Exception {
        logger.info("test name: " + method.getName());
        Bundle bundle = BundleUtil.readELBundles()[0][0];

        for (int i = 0; i < 3; i++) {
            bundles[i] = new Bundle(bundle, servers.get(i));
            bundles[i].generateUniqueBundle();
            bundles[i].setProcessWorkflow(aggregateWorkflowDir);
        }
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        removeBundles();
    }

    @Test(enabled = true, timeOut = 1200000)
    public void multipleSourceOneTarget() throws Exception {

        bundles[0].setInputFeedDataPath(inputPath);
        Bundle.submitCluster(bundles[0], bundles[1], bundles[2]);

        String feed = bundles[0].getDataSets().get(0);
        feed = InstanceUtil.setFeedCluster(feed,
            XmlUtil.createValidity("2009-02-01T00:00Z", "2012-01-01T00:00Z"),
            XmlUtil.createRtention("hours(10)", ActionType.DELETE), null,
            ClusterType.SOURCE, null);

        // use the colo string here so that the test works in embedded and distributed mode.
        String postFix = "/US/" + cluster2Colo;
        String prefix = bundles[0].getFeedDataPathPrefix();
        HadoopUtil.deleteDirIfExists(prefix.substring(1), cluster2FS);
        Util.lateDataReplenish(cluster2, 5, 80, prefix, postFix);

        // use the colo string here so that the test works in embedded and distributed mode.
        postFix = "/UK/" + cluster3Colo;
        prefix = bundles[0].getFeedDataPathPrefix();
        HadoopUtil.deleteDirIfExists(prefix.substring(1), cluster3FS);
        Util.lateDataReplenish(cluster3, 5, 80, prefix, postFix);

        String startTime = TimeUtil.getTimeWrtSystemTime(-30);

        feed = InstanceUtil.setFeedCluster(feed, XmlUtil.createValidity(startTime,
            TimeUtil.addMinsToTime(startTime, 85)),
            XmlUtil.createRtention("hours(10)", ActionType.DELETE),
            Util.readClusterName(bundles[1].getClusters().get(0)), ClusterType.SOURCE,
            "US/${cluster.colo}");

        feed = InstanceUtil.setFeedCluster(feed,
            XmlUtil.createValidity(TimeUtil.addMinsToTime(startTime, 20),
                TimeUtil.addMinsToTime(startTime, 105)),
            XmlUtil.createRtention("hours(10)", ActionType.DELETE),
            Util.readClusterName(bundles[0].getClusters().get(0)), ClusterType.TARGET, null);

        feed = InstanceUtil.setFeedCluster(feed,
            XmlUtil.createValidity(TimeUtil.addMinsToTime(startTime, 40),
                TimeUtil.addMinsToTime(startTime, 130)),
            XmlUtil.createRtention("hours(10)", ActionType.DELETE),
            Util.readClusterName(bundles[2].getClusters().get(0)), ClusterType.SOURCE,
            "UK/${cluster.colo}");

        logger.info("feed: " + Util.prettyPrintXml(feed));

        prism.getFeedHelper().submitEntity(URLS.SUBMIT_URL, feed);
        prism.getFeedHelper().schedule(URLS.SCHEDULE_URL, feed);
        AssertUtil.checkStatus(serverOC.get(0), ENTITY_TYPE.FEED, feed, Job.Status.RUNNING);

        //change feed location path
        feed = InstanceUtil.setFeedFilePath(feed, alternativeInputPath);

        logger.info("updated feed: " + Util.prettyPrintXml(feed));

        //update feed
        AssertUtil.assertSucceeded(prism.getFeedHelper().update(feed, feed));

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
        Assert.assertEquals(
            InstanceUtil.checkIfFeedCoordExist(cluster1.getFeedHelper(), Util.readDatasetName(feed),
                "REPLICATION"), 4);
        Assert.assertEquals(
            InstanceUtil.checkIfFeedCoordExist(cluster1.getFeedHelper(), Util.readDatasetName(feed),
                "RETENTION"), 2);
    }

    @Test(enabled = true, timeOut = 1200000)
    public void updateFeed_dependentProcessTest() throws Exception {
        //set cluster colos
        bundles[0].setCLusterColo(cluster1Colo);
        logger.info("cluster bundles[0]: " + Util.prettyPrintXml(bundles[0].getClusters().get(0)));
        bundles[1].setCLusterColo(cluster2Colo);
        logger.info("cluster bundles[1]: " + Util.prettyPrintXml(bundles[1].getClusters().get(0)));
        bundles[2].setCLusterColo(cluster3Colo);
        logger.info("cluster bundles[2]: " + Util.prettyPrintXml(bundles[2].getClusters().get(0)));

        //submit 3 clusters
        Bundle.submitCluster(bundles[0], bundles[1], bundles[2]);

        //get 2 unique feeds
        String feed01 = BundleUtil.getInputFeedFromBundle(bundles[0]);
        String feed02 = BundleUtil.getInputFeedFromBundle(bundles[1]);
        String outputFeed = BundleUtil.getOutputFeedFromBundle(bundles[0]);

        //set clusters to null;
        feed01 = InstanceUtil.setFeedCluster(feed01,
            XmlUtil.createValidity("2009-02-01T00:00Z", "2012-01-01T00:00Z"),
            XmlUtil.createRtention("hours(10)", ActionType.DELETE), null,
            ClusterType.SOURCE, null);

        feed02 = InstanceUtil.setFeedCluster(feed02,
            XmlUtil.createValidity("2009-02-01T00:00Z", "2012-01-01T00:00Z"),
            XmlUtil.createRtention("hours(10)", ActionType.DELETE), null,
            ClusterType.SOURCE, null);

        outputFeed = InstanceUtil.setFeedCluster(outputFeed,
            XmlUtil.createValidity("2009-02-01T00:00Z", "2012-01-01T00:00Z"),
            XmlUtil.createRtention("hours(10)", ActionType.DELETE), null,
            ClusterType.SOURCE, null);

        //set new feed input data
        feed01 = Util.setFeedPathValue(feed01,
            baseHDFSDir + "/feed01/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}/");
        feed02 = Util.setFeedPathValue(feed02,
            baseHDFSDir + "/feed02/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}/");

        //generate data in both the colos ua1 and ua3
        String prefix = InstanceUtil.getFeedPrefix(feed01);
        HadoopUtil.deleteDirIfExists(prefix.substring(1), cluster1FS);
        Util.lateDataReplenish(cluster1, 70, 1, prefix, null);

        prefix = InstanceUtil.getFeedPrefix(feed02);
        HadoopUtil.deleteDirIfExists(prefix.substring(1), cluster3FS);
        Util.lateDataReplenish(cluster3, 70, 1, prefix, null);

        String startTime = TimeUtil.getTimeWrtSystemTime(-50);

        //set clusters for feed01
        feed01 = InstanceUtil
            .setFeedCluster(feed01, XmlUtil.createValidity(startTime, "2099-01-01T00:00Z"),
                XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                Util.readClusterName(bundles[0].getClusters().get(0)), ClusterType.SOURCE,
                null);

        feed01 = InstanceUtil
            .setFeedCluster(feed01, XmlUtil.createValidity(startTime, "2099-01-01T00:00Z"),
                XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                Util.readClusterName(bundles[2].getClusters().get(0)), ClusterType.TARGET,
                null);

        //set clusters for feed02
        feed02 = InstanceUtil
            .setFeedCluster(feed02, XmlUtil.createValidity(startTime, "2099-01-01T00:00Z"),
                XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                Util.readClusterName(bundles[0].getClusters().get(0)), ClusterType.TARGET,
                null);

        feed02 = InstanceUtil
            .setFeedCluster(feed02, XmlUtil.createValidity(startTime, "2099-01-01T00:00Z"),
                XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                Util.readClusterName(bundles[2].getClusters().get(0)), ClusterType.SOURCE,
                null);

        //set clusters for output feed
        outputFeed = InstanceUtil.setFeedCluster(outputFeed,
            XmlUtil.createValidity(startTime, "2099-01-01T00:00Z"),
            XmlUtil.createRtention("hours(10)", ActionType.DELETE),
            Util.readClusterName(bundles[0].getClusters().get(0)), ClusterType.SOURCE, null);

        outputFeed = InstanceUtil.setFeedCluster(outputFeed,
            XmlUtil.createValidity(startTime, "2099-01-01T00:00Z"),
            XmlUtil.createRtention("hours(10)", ActionType.DELETE),
            Util.readClusterName(bundles[2].getClusters().get(0)), ClusterType.TARGET, null);

        //submit and schedule feeds
        logger.info("feed01: " + Util.prettyPrintXml(feed01));
        logger.info("feed02: " + Util.prettyPrintXml(feed02));
        logger.info("outputFeed: " + Util.prettyPrintXml(outputFeed));

        prism.getFeedHelper().submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, feed01);
        prism.getFeedHelper().submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, feed02);
        prism.getFeedHelper().submitAndSchedule(URLS.SUBMIT_URL, outputFeed);

        //create a process with 2 clusters

        //get a process
        String process = bundles[0].getProcessData();

        //add clusters to process
        String processStartTime = TimeUtil.getTimeWrtSystemTime(-11);
        String processEndTime = TimeUtil.getTimeWrtSystemTime(70);

        process = InstanceUtil.setProcessCluster(process, null,
            XmlUtil.createProcessValidity(startTime, "2099-01-01T00:00Z"));

        process = InstanceUtil
            .setProcessCluster(process, Util.readClusterName(bundles[0].getClusters().get(0)),
                XmlUtil.createProcessValidity(processStartTime, processEndTime));

        process = InstanceUtil
            .setProcessCluster(process, Util.readClusterName(bundles[2].getClusters().get(0)),
                XmlUtil.createProcessValidity(processStartTime, processEndTime));
        process = InstanceUtil.addProcessInputFeed(process, Util.readDatasetName(feed02),
            Util.readDatasetName(feed02));

        //submit and schedule process
        logger.info("process: " + Util.prettyPrintXml(process));

        prism.getProcessHelper().submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, process);

        logger.info("Wait till process goes into running ");

        InstanceUtil.waitTillInstanceReachState(serverOC.get(0), Util.getProcessName(process), 1,
            Status.RUNNING, 10, ENTITY_TYPE.PROCESS);
        InstanceUtil.waitTillInstanceReachState(serverOC.get(2), Util.getProcessName(process), 1,
            Status.RUNNING, 10, ENTITY_TYPE.PROCESS);

        feed01 = InstanceUtil.setFeedFilePath(feed01, alternativeInputPath);
        logger.info("updated feed: " + Util.prettyPrintXml(feed01));
        prism.getFeedHelper().update(feed01, feed01);
    }
}
