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

package org.apache.falcon.regression;

import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.generated.feed.ActionType;
import org.apache.falcon.regression.core.generated.feed.ClusterType;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.response.ServiceResponse;
import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.core.util.Util.URLS;
import org.apache.falcon.regression.core.util.XmlUtil;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.falcon.regression.testHelper.BaseTestUtil;
import org.apache.hadoop.fs.FileSystem;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;

/**
 * Feed cluster update tests.
 */
public class FeedClusterUpdateTest extends BaseTestClass {

    String testDir = "/FeedClusterUpdateTest";
    String baseTestDir = baseHDFSDir + testDir;
    ColoHelper cluster1;
    ColoHelper cluster2;
    ColoHelper cluster3;
    FileSystem cluster2FS;
    FileSystem cluster3FS;
    private Bundle b1 = new Bundle();
    private Bundle b2 = new Bundle();
    private Bundle b3 = new Bundle();
    private String feed;
    String startTime;
    String feedOriginalSubmit;
    String feedUpdated;

    public FeedClusterUpdateTest(){
         super();
        cluster1 = servers.get(0);
        cluster2 = servers.get(1);
        cluster3 = servers.get(2);
        cluster2FS = serverFS.get(1);
        cluster3FS = serverFS.get(2);
    }

    @BeforeClass(alwaysRun = true)
    public void createTestData() throws Exception {
        b1 = (Bundle) Util.readELBundles()[0][0];
        b1.generateUniqueBundle();
        b2 = (Bundle) Util.readELBundles()[0][0];
        b2.generateUniqueBundle();
        b3 = (Bundle) Util.readELBundles()[0][0];
        b3.generateUniqueBundle();
        try {
            b1 = new Bundle(b1, cluster1.getEnvFileName(), cluster1.getPrefix());
            b2 = new Bundle(b2, cluster2.getEnvFileName(), cluster2.getPrefix());
            b3 = new Bundle(b3, cluster3.getEnvFileName(), cluster3.getPrefix());
            String postFix = "/US/ua2";
            HadoopUtil.deleteDirIfExists(baseTestDir, cluster2FS);
            Util.lateDataReplenish(cluster2, 80, 1, baseTestDir, postFix);
            postFix = "/UK/ua3";
            HadoopUtil.deleteDirIfExists(baseTestDir, cluster3FS);
            Util.lateDataReplenish(cluster3, 80, 1, baseTestDir, postFix);
        } finally {
            b1.deleteBundle(prism);
            b2.deleteBundle(prism);
            b3.deleteBundle(prism);
        }
    }

    @BeforeMethod(alwaysRun = true)
    public void setup(Method method) throws Exception {
        Util.print("test name: " + method.getName());
        //restart server as precaution
        Util.restartService(cluster1.getClusterHelper());
        Util.restartService(cluster2.getClusterHelper());
        Util.restartService(cluster3.getClusterHelper());

        b1 = (Bundle) Util.readELBundles()[0][0];
        b1.generateUniqueBundle();
        b2 = (Bundle) Util.readELBundles()[0][0];
        b2.generateUniqueBundle();
        b3 = (Bundle) Util.readELBundles()[0][0];
        b3.generateUniqueBundle();

        b1 = new Bundle(b1, cluster1.getEnvFileName(), cluster1.getPrefix());
        b2 = new Bundle(b2, cluster2.getEnvFileName(), cluster2.getPrefix());
        b3 = new Bundle(b3, cluster3.getEnvFileName(), cluster3.getPrefix());
        Util.submitAllClusters(b1, b2, b3);
        feed = b1.getDataSets().get(0);
        feed = InstanceUtil.setFeedCluster(feed,
                XmlUtil.createValidity("2009-02-01T00:00Z", "2012-01-01T00:00Z"),
                XmlUtil.createRtention("hours(10)", ActionType.DELETE), null,
                ClusterType.SOURCE, null, null);
        startTime = InstanceUtil.getTimeWrtSystemTime(-50);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws Exception {
        BaseTestUtil.removeBundles(prism, b1, b2, b3);
    }

    @Test(enabled = false, groups = {"multiCluster"})
    public void addSourceCluster() throws Exception {
        //add one source and one target , schedule only on source
        feedOriginalSubmit = InstanceUtil.setFeedCluster(feed, XmlUtil.createValidity(startTime,
                InstanceUtil.addMinsToTime(startTime, 65)),
                XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                Util.readClusterName(b2.getClusters().get(0)), ClusterType.SOURCE,
                null, null);
        feedOriginalSubmit = InstanceUtil.setFeedCluster(feedOriginalSubmit,
                XmlUtil.createValidity(InstanceUtil.addMinsToTime(startTime, 20),
                        InstanceUtil.addMinsToTime(startTime, 85)),
                XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                Util.readClusterName(b1.getClusters().get(0)), ClusterType.TARGET, null, null);

        Util.print("Feed: " + feedOriginalSubmit);

        ServiceResponse response =
                prism.getFeedHelper().submitEntity(URLS.SUBMIT_URL, feedOriginalSubmit);
        Thread.sleep(10000);
        Util.assertSucceeded(response);

        //schedule on source
        response = cluster2.getFeedHelper().schedule(URLS.SCHEDULE_URL, feedOriginalSubmit);
        Thread.sleep(20000);
        Util.assertSucceeded(response);

        Assert.assertEquals(InstanceUtil
                .checkIfFeedCoordExist(cluster2.getFeedHelper(),
                        Util.readDatasetName(feedOriginalSubmit),
                        "REPLICATION"), 0);
        Assert.assertEquals(InstanceUtil
                .checkIfFeedCoordExist(cluster2.getFeedHelper(),
                        Util.readDatasetName(feedOriginalSubmit), "RETENTION" +
                        ""), 1);
        Assert.assertEquals(InstanceUtil
                .checkIfFeedCoordExist(cluster3.getFeedHelper(),
                        Util.readDatasetName(feedOriginalSubmit),
                        "REPLICATION"), 0);
        Assert.assertEquals(InstanceUtil
                .checkIfFeedCoordExist(cluster3.getFeedHelper(),
                        Util.readDatasetName(feedOriginalSubmit), "RETENTION"),
                0);
        Assert.assertEquals(InstanceUtil
                .checkIfFeedCoordExist(cluster1.getFeedHelper(),
                        Util.readDatasetName(feedOriginalSubmit),
                        "REPLICATION"), 0);
        Assert.assertEquals(InstanceUtil
                .checkIfFeedCoordExist(cluster1.getFeedHelper(),
                        Util.readDatasetName(feedOriginalSubmit),
                        "RETENTION"), 0);

        //prepare updated Feed
        feedUpdated = InstanceUtil.setFeedCluster(
                feed, XmlUtil.createValidity(startTime,
                InstanceUtil.addMinsToTime(startTime, 65)),
                XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                Util.readClusterName(b2.getClusters().get(0)), ClusterType.SOURCE,
                "US/${cluster.colo}", null);
        feedUpdated = InstanceUtil.setFeedCluster(feedUpdated,
                XmlUtil.createValidity(InstanceUtil.addMinsToTime(startTime, 20),
                        InstanceUtil.addMinsToTime(startTime, 85)),
                XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                Util.readClusterName(b1.getClusters().get(0)), ClusterType.TARGET, null, null);
        feedUpdated = InstanceUtil.setFeedCluster(feedUpdated,
                XmlUtil.createValidity(InstanceUtil.addMinsToTime(startTime, 40),
                        InstanceUtil.addMinsToTime(startTime, 110)),
                XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                Util.readClusterName(b3.getClusters().get(0)), ClusterType.SOURCE,
                "UK/${cluster.colo}", null);

        response = prism.getFeedHelper().update(feedUpdated, feedUpdated);
        Thread.sleep(20000);
        Util.assertSucceeded(response);

        prism.getFeedHelper()
                .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, feedUpdated);
        Assert.assertEquals(InstanceUtil
                .checkIfFeedCoordExist(cluster2.getFeedHelper(), Util.readDatasetName(feedUpdated),
                        "REPLICATION"), 0);
        Assert.assertEquals(InstanceUtil
                .checkIfFeedCoordExist(cluster2.getFeedHelper(), Util.readDatasetName(feedUpdated),
                        "RETENTION"), 2);
        Assert.assertEquals(InstanceUtil
                .checkIfFeedCoordExist(cluster3.getFeedHelper(), Util.readDatasetName(feedUpdated),
                        "REPLICATION"), 0);
        Assert.assertEquals(InstanceUtil
                .checkIfFeedCoordExist(cluster3.getFeedHelper(), Util.readDatasetName(feedUpdated),
                        "RETENTION"), 1);
        Assert.assertEquals(InstanceUtil
                .checkIfFeedCoordExist(cluster1.getFeedHelper(), Util.readDatasetName(feedUpdated),
                        "REPLICATION"), 1);
        Assert.assertEquals(InstanceUtil
                .checkIfFeedCoordExist(cluster1.getFeedHelper(), Util.readDatasetName(feedUpdated),
                        "RETENTION"), 2);
    }

    @Test(enabled = false, groups = {"multiCluster"})
    public void addTargetCluster() throws Exception {
        //add one source and one target , schedule only on source
        feedOriginalSubmit = InstanceUtil
                .setFeedCluster(feed, XmlUtil.createValidity(startTime,
                        InstanceUtil.addMinsToTime(startTime, 65)),
                        XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                        Util.readClusterName(b2.getClusters().get(0)), ClusterType.SOURCE,
                        null, null);
        feedOriginalSubmit = InstanceUtil.setFeedCluster(feedOriginalSubmit,
                XmlUtil.createValidity(InstanceUtil.addMinsToTime(startTime, 40),
                        InstanceUtil.addMinsToTime(startTime, 110)),
                XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                Util.readClusterName(b3.getClusters().get(0)), ClusterType.SOURCE,
                "UK/${cluster.colo}", null);

        Util.print("Feed: " + feedOriginalSubmit);

        ServiceResponse response =
                prism.getFeedHelper().submitEntity(URLS.SUBMIT_URL, feedOriginalSubmit);
        Thread.sleep(10000);
        Util.assertSucceeded(response);

        //schedule on source
        response = cluster2.getFeedHelper().schedule(URLS.SCHEDULE_URL, feedOriginalSubmit);
        Thread.sleep(20000);
        Util.assertSucceeded(response);

        Assert.assertEquals(InstanceUtil
                .checkIfFeedCoordExist(cluster2.getFeedHelper(),
                        Util.readDatasetName(feedOriginalSubmit),
                        "REPLICATION"), 0);
        Assert.assertEquals(InstanceUtil
                .checkIfFeedCoordExist(cluster2.getFeedHelper(),
                        Util.readDatasetName(feedOriginalSubmit),
                        "RETENTION"), 1);
        Assert.assertEquals(InstanceUtil
                .checkIfFeedCoordExist(cluster3.getFeedHelper(),
                        Util.readDatasetName(feedOriginalSubmit),
                        "REPLICATION"), 0);
        Assert.assertEquals(InstanceUtil
                .checkIfFeedCoordExist(cluster3.getFeedHelper(),
                        Util.readDatasetName(feedOriginalSubmit),
                        "RETENTION"), 0);
        Assert.assertEquals(InstanceUtil
                .checkIfFeedCoordExist(cluster1.getFeedHelper(),
                        Util.readDatasetName(feedOriginalSubmit),
                        "REPLICATION"), 0);
        Assert.assertEquals(InstanceUtil
                .checkIfFeedCoordExist(cluster1.getFeedHelper(),
                        Util.readDatasetName(feedOriginalSubmit), "RETENTION"),
                0);

        //prepare updated Feed
        feedUpdated = InstanceUtil
                .setFeedCluster(feed, XmlUtil.createValidity(startTime,
                        InstanceUtil.addMinsToTime(startTime, 65)),
                        XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                        Util.readClusterName(b2.getClusters().get(0)), ClusterType.SOURCE,
                        "US/${cluster.colo}", null);
        feedUpdated = InstanceUtil.setFeedCluster(feedUpdated,
                XmlUtil.createValidity(InstanceUtil.addMinsToTime(startTime, 20),
                        InstanceUtil.addMinsToTime(startTime, 85)),
                XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                Util.readClusterName(b1.getClusters().get(0)), ClusterType.TARGET, null, null);
        feedUpdated = InstanceUtil.setFeedCluster(feedUpdated,
                XmlUtil.createValidity(InstanceUtil.addMinsToTime(startTime, 40),
                        InstanceUtil.addMinsToTime(startTime, 110)),
                XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                Util.readClusterName(b3.getClusters().get(0)), ClusterType.SOURCE,
                "UK/${cluster.colo}", null);

        Util.print("Updated Feed: " + feedUpdated);

        response = prism.getFeedHelper().update(feedUpdated, feedUpdated);
        Thread.sleep(20000);
        Util.assertSucceeded(response);

        prism.getFeedHelper()
                .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, feedUpdated);

        Assert.assertEquals(InstanceUtil
                .checkIfFeedCoordExist(cluster2.getFeedHelper(), Util.readDatasetName(feedUpdated),
                        "REPLICATION"), 0);
        Assert.assertEquals(InstanceUtil
                .checkIfFeedCoordExist(cluster2.getFeedHelper(), Util.readDatasetName(feedUpdated),
                        "RETENTION"), 2);
        Assert.assertEquals(InstanceUtil
                .checkIfFeedCoordExist(cluster3.getFeedHelper(), Util.readDatasetName(feedUpdated),
                        "REPLICATION"), 0);
        Assert.assertEquals(InstanceUtil
                .checkIfFeedCoordExist(cluster3.getFeedHelper(), Util.readDatasetName(feedUpdated),
                        "RETENTION"), 1);
        Assert.assertEquals(InstanceUtil
                .checkIfFeedCoordExist(cluster1.getFeedHelper(), Util.readDatasetName(feedUpdated),
                        "REPLICATION"), 2);
        Assert.assertEquals(InstanceUtil
                .checkIfFeedCoordExist(cluster1.getFeedHelper(), Util.readDatasetName(feedUpdated),
                        "RETENTION"), 1);
    }

    @Test(enabled = false, groups = {"multiCluster"})
    public void add2SourceCluster() throws Exception {
        //add one source and one target , schedule only on source
        feedOriginalSubmit = InstanceUtil
                .setFeedCluster(feed, XmlUtil.createValidity(startTime,
                        InstanceUtil.addMinsToTime(startTime, 65)),
                        XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                        Util.readClusterName(b2.getClusters().get(0)), ClusterType.SOURCE,
                        null, null);

        Util.print("Feed: " + feedOriginalSubmit);

        ServiceResponse response = prism.getFeedHelper().submitEntity(URLS.SUBMIT_URL, feedOriginalSubmit);
        Thread.sleep(10000);
        Util.assertSucceeded(response);

        //schedule on source
        response = cluster2.getFeedHelper().schedule(URLS.SCHEDULE_URL, feedOriginalSubmit);
        Thread.sleep(20000);
        Util.assertSucceeded(response);

        Assert.assertEquals(InstanceUtil
                .checkIfFeedCoordExist(cluster2.getFeedHelper(),
                        Util.readDatasetName(feedOriginalSubmit),
                        "REPLICATION"), 0);
        Assert.assertEquals(InstanceUtil
                .checkIfFeedCoordExist(cluster2.getFeedHelper(),
                        Util.readDatasetName(feedOriginalSubmit), "RETENTION"),
                1);
        Assert.assertEquals(InstanceUtil
                .checkIfFeedCoordExist(cluster3.getFeedHelper(),
                        Util.readDatasetName(feedOriginalSubmit),
                        "REPLICATION"), 0);
        Assert.assertEquals(InstanceUtil
                .checkIfFeedCoordExist(cluster3.getFeedHelper(),
                        Util.readDatasetName(feedOriginalSubmit), "RETENTION"),
                0);
        Assert.assertEquals(InstanceUtil
                .checkIfFeedCoordExist(cluster1.getFeedHelper(),
                        Util.readDatasetName(feedOriginalSubmit),
                        "REPLICATION"), 0);
        Assert.assertEquals(InstanceUtil
                .checkIfFeedCoordExist(cluster1.getFeedHelper(),
                        Util.readDatasetName(feedOriginalSubmit), "RETENTION"),
                0);

        //prepare updated Feed
        feedUpdated = InstanceUtil
                .setFeedCluster(feed, XmlUtil.createValidity(startTime,
                        InstanceUtil.addMinsToTime(startTime, 65)),
                        XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                        Util.readClusterName(b2.getClusters().get(0)), ClusterType.SOURCE,
                        "US/${cluster.colo}", null);
        feedUpdated = InstanceUtil.setFeedCluster(feedUpdated,
                XmlUtil.createValidity(InstanceUtil.addMinsToTime(startTime, 20),
                        InstanceUtil.addMinsToTime(startTime, 85)),
                XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                Util.readClusterName(b1.getClusters().get(0)), ClusterType.SOURCE, null, null);
        feedUpdated = InstanceUtil.setFeedCluster(feedUpdated,
                XmlUtil.createValidity(InstanceUtil.addMinsToTime(startTime, 40),
                        InstanceUtil.addMinsToTime(startTime, 110)),
                XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                Util.readClusterName(b3.getClusters().get(0)), ClusterType.SOURCE,
                "UK/${cluster.colo}", null);

        Util.print("Updated Feed: " + feedUpdated);

        response = prism.getFeedHelper().update(feedUpdated, feedUpdated);
        Thread.sleep(20000);
        Util.assertSucceeded(response);

        prism.getFeedHelper()
                .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, feedUpdated);

        Assert.assertEquals(InstanceUtil
                .checkIfFeedCoordExist(cluster2.getFeedHelper(), Util.readDatasetName(feedUpdated),
                        "REPLICATION"), 0);
        Assert.assertEquals(InstanceUtil
                .checkIfFeedCoordExist(cluster2.getFeedHelper(), Util.readDatasetName(feedUpdated),
                        "RETENTION"), 2);
        Assert.assertEquals(InstanceUtil
                .checkIfFeedCoordExist(cluster3.getFeedHelper(), Util.readDatasetName(feedUpdated),
                        "REPLICATION"), 0);
        Assert.assertEquals(InstanceUtil
                .checkIfFeedCoordExist(cluster3.getFeedHelper(), Util.readDatasetName(feedUpdated),
                        "RETENTION"), 1);
        Assert.assertEquals(InstanceUtil
                .checkIfFeedCoordExist(cluster1.getFeedHelper(), Util.readDatasetName(feedUpdated),
                        "REPLICATION"), 0);
        Assert.assertEquals(InstanceUtil
                .checkIfFeedCoordExist(cluster1.getFeedHelper(), Util.readDatasetName(feedUpdated),
                        "RETENTION"), 1);
    }

    @Test(enabled = false, groups = {"multiCluster"})
    public void add2TargetCluster() throws Exception {
        //add one source and one target , schedule only on source
        feedOriginalSubmit = InstanceUtil
                .setFeedCluster(feed, XmlUtil.createValidity(startTime,
                        InstanceUtil.addMinsToTime(startTime, 65)),
                        XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                        Util.readClusterName(b2.getClusters().get(0)), ClusterType.SOURCE,
                        null, null);

        Util.print("Feed: " + feedOriginalSubmit);

        ServiceResponse response =
                prism.getFeedHelper().submitEntity(URLS.SUBMIT_URL, feedOriginalSubmit);
        Thread.sleep(10000);
        Util.assertSucceeded(response);

        //schedule on source

        response = cluster2.getFeedHelper().schedule(URLS.SCHEDULE_URL, feedOriginalSubmit);
        Thread.sleep(20000);
        Util.assertSucceeded(response);

        Assert.assertEquals(InstanceUtil
                .checkIfFeedCoordExist(cluster2.getFeedHelper(),
                        Util.readDatasetName(feedOriginalSubmit),
                        "REPLICATION"), 0);
        Assert.assertEquals(InstanceUtil
                .checkIfFeedCoordExist(cluster2.getFeedHelper(),
                        Util.readDatasetName(feedOriginalSubmit),
                        "RETENTION"), 1);
        Assert.assertEquals(InstanceUtil
                .checkIfFeedCoordExist(cluster3.getFeedHelper(),
                        Util.readDatasetName(feedOriginalSubmit),
                        "REPLICATION"), 0);
        Assert.assertEquals(InstanceUtil
                .checkIfFeedCoordExist(cluster3.getFeedHelper(),
                        Util.readDatasetName(feedOriginalSubmit), "RETENTION"),
                0);
        Assert.assertEquals(InstanceUtil
                .checkIfFeedCoordExist(cluster1.getFeedHelper(),
                        Util.readDatasetName(feedOriginalSubmit),
                        "REPLICATION"), 0);
        Assert.assertEquals(InstanceUtil
                .checkIfFeedCoordExist(cluster1.getFeedHelper(),
                        Util.readDatasetName(feedOriginalSubmit), "RETENTION"),
                0);

        //prepare updated Feed
        feedUpdated = InstanceUtil.setFeedCluster(feed, XmlUtil.createValidity(startTime,
                InstanceUtil.addMinsToTime(startTime, 65)),
                XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                Util.readClusterName(b2.getClusters().get(0)), ClusterType.SOURCE,
                null, null);
        feedUpdated = InstanceUtil.setFeedCluster(feedUpdated,
                XmlUtil.createValidity(InstanceUtil.addMinsToTime(startTime, 20),
                        InstanceUtil.addMinsToTime(startTime, 85)),
                XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                Util.readClusterName(b1.getClusters().get(0)), ClusterType.TARGET, null, null);
        feedUpdated = InstanceUtil.setFeedCluster(feedUpdated,
                XmlUtil.createValidity(InstanceUtil.addMinsToTime(startTime, 40),
                        InstanceUtil.addMinsToTime(startTime, 110)),
                XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                Util.readClusterName(b3.getClusters().get(0)), ClusterType.TARGET, null, null);

        Util.print("Updated Feed: " + feedUpdated);

        response = prism.getFeedHelper().update(feedUpdated, feedUpdated);
        Thread.sleep(20000);
        Util.assertSucceeded(response);

        prism.getFeedHelper()
                .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, feedUpdated);

        Assert.assertEquals(InstanceUtil
                .checkIfFeedCoordExist(cluster2.getFeedHelper(), Util.readDatasetName(feedUpdated),
                        "REPLICATION"), 0);
        Assert.assertEquals(InstanceUtil
                .checkIfFeedCoordExist(cluster2.getFeedHelper(), Util.readDatasetName(feedUpdated),
                        "RETENTION"), 1);
        Assert.assertEquals(InstanceUtil
                .checkIfFeedCoordExist(cluster3.getFeedHelper(), Util.readDatasetName(feedUpdated),
                        "REPLICATION"), 1);
        Assert.assertEquals(InstanceUtil
                .checkIfFeedCoordExist(cluster3.getFeedHelper(), Util.readDatasetName(feedUpdated),
                        "RETENTION"), 1);
        Assert.assertEquals(InstanceUtil
                .checkIfFeedCoordExist(cluster1.getFeedHelper(), Util.readDatasetName(feedUpdated),
                        "REPLICATION"), 1);
        Assert.assertEquals(InstanceUtil
                .checkIfFeedCoordExist(cluster1.getFeedHelper(), Util.readDatasetName(feedUpdated),
                        "RETENTION"), 1);
    }

    @Test(enabled = false, groups = {"multiCluster"})
    public void add1Source1TargetCluster() throws Exception {
        //add one source and one target , schedule only on source
        feedOriginalSubmit = InstanceUtil
                .setFeedCluster(feed, XmlUtil.createValidity(startTime,
                        InstanceUtil.addMinsToTime(startTime, 65)),
                        XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                        Util.readClusterName(b2.getClusters().get(0)), ClusterType.SOURCE,
                        null, null);

        Util.print("Feed: " + feedOriginalSubmit);

        ServiceResponse response =
                prism.getFeedHelper().submitEntity(URLS.SUBMIT_URL, feedOriginalSubmit);
        Thread.sleep(10000);
        Util.assertSucceeded(response);

        //schedule on source
        response = cluster2.getFeedHelper().schedule(URLS.SCHEDULE_URL, feedOriginalSubmit);
        Thread.sleep(20000);
        Util.assertSucceeded(response);

        Assert.assertEquals(InstanceUtil
                .checkIfFeedCoordExist(cluster2.getFeedHelper(),
                        Util.readDatasetName(feedOriginalSubmit),
                        "REPLICATION"), 0);
        Assert.assertEquals(InstanceUtil
                .checkIfFeedCoordExist(cluster2.getFeedHelper(),
                        Util.readDatasetName(feedOriginalSubmit), "RETENTION"),
                1);
        Assert.assertEquals(InstanceUtil
                .checkIfFeedCoordExist(cluster3.getFeedHelper(),
                        Util.readDatasetName(feedOriginalSubmit),
                        "REPLICATION"), 0);
        Assert.assertEquals(InstanceUtil
                .checkIfFeedCoordExist(cluster3.getFeedHelper(),
                        Util.readDatasetName(feedOriginalSubmit), "RETENTION"),
                0);
        Assert.assertEquals(InstanceUtil
                .checkIfFeedCoordExist(cluster1.getFeedHelper(),
                        Util.readDatasetName(feedOriginalSubmit),
                        "REPLICATION"), 0);
        Assert.assertEquals(InstanceUtil
                .checkIfFeedCoordExist(cluster1.getFeedHelper(),
                        Util.readDatasetName(feedOriginalSubmit),
                        "RETENTION"), 0);

        //prepare updated Feed
        feedUpdated = InstanceUtil
                .setFeedCluster(feed, XmlUtil.createValidity(startTime,
                        InstanceUtil.addMinsToTime(startTime, 65)),
                        XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                        Util.readClusterName(b2.getClusters().get(0)), ClusterType.SOURCE,
                        "US/${cluster.colo}", null);
        feedUpdated = InstanceUtil.setFeedCluster(feedUpdated,
                XmlUtil.createValidity(InstanceUtil.addMinsToTime(startTime, 20),
                        InstanceUtil.addMinsToTime(startTime, 85)),
                XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                Util.readClusterName(b1.getClusters().get(0)), ClusterType.TARGET, null, null);
        feedUpdated = InstanceUtil.setFeedCluster(feedUpdated,
                XmlUtil.createValidity(InstanceUtil.addMinsToTime(startTime, 40),
                        InstanceUtil.addMinsToTime(startTime, 110)),
                XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                Util.readClusterName(b3.getClusters().get(0)), ClusterType.SOURCE,
                "UK/${cluster.colo}", null);

        Util.print("Updated Feed: " + feedUpdated);

        response = prism.getFeedHelper().update(feedUpdated, feedUpdated);
        Thread.sleep(20000);
        Util.assertSucceeded(response);

        prism.getFeedHelper()
                .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, feedUpdated);

        Assert.assertEquals(InstanceUtil
                .checkIfFeedCoordExist(cluster2.getFeedHelper(), Util.readDatasetName(feedUpdated),
                        "REPLICATION"), 0);
        Assert.assertEquals(InstanceUtil
                .checkIfFeedCoordExist(cluster2.getFeedHelper(), Util.readDatasetName(feedUpdated),
                        "RETENTION"), 2);
        Assert.assertEquals(InstanceUtil
                .checkIfFeedCoordExist(cluster3.getFeedHelper(), Util.readDatasetName(feedUpdated),
                        "REPLICATION"), 0);
        Assert.assertEquals(InstanceUtil
                .checkIfFeedCoordExist(cluster3.getFeedHelper(), Util.readDatasetName(feedUpdated),
                        "RETENTION"), 1);
        Assert.assertEquals(InstanceUtil
                .checkIfFeedCoordExist(cluster1.getFeedHelper(), Util.readDatasetName(feedUpdated),
                        "REPLICATION"), 2);
        Assert.assertEquals(InstanceUtil
                .checkIfFeedCoordExist(cluster1.getFeedHelper(), Util.readDatasetName(feedUpdated),
                        "RETENTION"), 1);
    }

    @Test(enabled = false, groups = {"multiCluster"})
    public void deleteSourceCluster() throws Exception {
        //add one source and one target , schedule only on source
        feedOriginalSubmit = InstanceUtil
                .setFeedCluster(feed, XmlUtil.createValidity(startTime,
                        InstanceUtil.addMinsToTime(startTime, 65)),
                        XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                        Util.readClusterName(b2.getClusters().get(0)), ClusterType.SOURCE,
                        "US/${cluster.colo}");
        feedOriginalSubmit = InstanceUtil.setFeedCluster(feedOriginalSubmit,
                XmlUtil.createValidity(InstanceUtil.addMinsToTime(startTime, 20),
                        InstanceUtil.addMinsToTime(startTime, 85)),
                XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                Util.readClusterName(b1.getClusters().get(0)), ClusterType.TARGET, null, null);
        feedOriginalSubmit = InstanceUtil.setFeedCluster(feedOriginalSubmit,
                XmlUtil.createValidity(InstanceUtil.addMinsToTime(startTime, 40),
                        InstanceUtil.addMinsToTime(startTime, 110)),
                XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                Util.readClusterName(b3.getClusters().get(0)), ClusterType.SOURCE,
                "UK/${cluster.colo}", null);

        Util.print("Feed: " + feedOriginalSubmit);

        ServiceResponse response =
                prism.getFeedHelper().submitEntity(URLS.SUBMIT_URL, feedOriginalSubmit);
        Thread.sleep(10000);
        Util.assertSucceeded(response);

        //schedule on source

        response = prism.getFeedHelper().schedule(URLS.SCHEDULE_URL, feedOriginalSubmit);
        Thread.sleep(20000);
        Util.assertSucceeded(response);

        Assert.assertEquals(InstanceUtil
                .checkIfFeedCoordExist(cluster2.getFeedHelper(),
                        Util.readDatasetName(feedOriginalSubmit),
                        "REPLICATION"), 0);
        Assert.assertEquals(InstanceUtil
                .checkIfFeedCoordExist(cluster2.getFeedHelper(),
                        Util.readDatasetName(feedOriginalSubmit), "RETENTION" +
                        ""), 1);
        Assert.assertEquals(InstanceUtil
                .checkIfFeedCoordExist(cluster3.getFeedHelper(),
                        Util.readDatasetName(feedOriginalSubmit),
                        "REPLICATION"), 0);
        Assert.assertEquals(InstanceUtil
                .checkIfFeedCoordExist(cluster3.getFeedHelper(),
                        Util.readDatasetName(feedOriginalSubmit),
                        "RETENTION"), 1);
        Assert.assertEquals(InstanceUtil
                .checkIfFeedCoordExist(cluster1.getFeedHelper(),
                        Util.readDatasetName(feedOriginalSubmit),
                        "REPLICATION"), 2);
        Assert.assertEquals(InstanceUtil
                .checkIfFeedCoordExist(cluster1.getFeedHelper(),
                        Util.readDatasetName(feedOriginalSubmit),
                        "RETENTION"), 1);

        //prepare updated Feed
        feedUpdated = InstanceUtil
                .setFeedCluster(feed, XmlUtil.createValidity(startTime,
                        InstanceUtil.addMinsToTime(startTime, 65)),
                        XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                        Util.readClusterName(b2.getClusters().get(0)), ClusterType.SOURCE,
                        null, null);
        feedUpdated = InstanceUtil.setFeedCluster(feedUpdated,
                XmlUtil.createValidity(InstanceUtil.addMinsToTime(startTime, 20),
                        InstanceUtil.addMinsToTime(startTime, 85)),
                XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                Util.readClusterName(b1.getClusters().get(0)), ClusterType.TARGET, null, null);

        response = prism.getFeedHelper().update(feedUpdated, feedUpdated);
        Thread.sleep(20000);
        Util.assertSucceeded(response);

        response = cluster3.getFeedHelper().getEntityDefinition(URLS.GET_ENTITY_DEFINITION, feedUpdated);
        Util.assertFailed(response);

        prism.getFeedHelper()
                .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, feedUpdated);
        Assert.assertEquals(InstanceUtil
                .checkIfFeedCoordExist(cluster2.getFeedHelper(), Util.readDatasetName(feedUpdated),
                        "REPLICATION"), 0);
        Assert.assertEquals(InstanceUtil
                .checkIfFeedCoordExist(cluster2.getFeedHelper(), Util.readDatasetName(feedUpdated),
                        "RETENTION"), 2);
        Assert.assertEquals(InstanceUtil
                .checkIfFeedCoordExist(cluster3.getFeedHelper(), Util.readDatasetName(feedUpdated),
                        "REPLICATION"), 0);
        Assert.assertEquals(InstanceUtil
                .checkIfFeedCoordExist(cluster3.getFeedHelper(), Util.readDatasetName(feedUpdated),
                        "RETENTION"), 1);
        Assert.assertEquals(InstanceUtil
                .checkIfFeedCoordExist(cluster1.getFeedHelper(), Util.readDatasetName(feedUpdated),
                        "REPLICATION"), 0);
        Assert.assertEquals(InstanceUtil
                .checkIfFeedCoordExist(cluster1.getFeedHelper(), Util.readDatasetName(feedUpdated),
                        "RETENTION"), 0);
    }

    @Test(enabled = true, groups = {"multiCluster"})
    public void deleteTargetCluster() throws Exception {
        //add one source and one target , schedule only on source
        feedOriginalSubmit = InstanceUtil
                .setFeedCluster(feed, XmlUtil.createValidity(startTime,
                        InstanceUtil.addMinsToTime(startTime, 65)),
                        XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                        Util.readClusterName(b2.getClusters().get(0)), ClusterType.SOURCE,
                        "US/${cluster.colo}", null);
        feedOriginalSubmit = InstanceUtil.setFeedCluster(feedOriginalSubmit,
                XmlUtil.createValidity(InstanceUtil.addMinsToTime(startTime, 20),
                        InstanceUtil.addMinsToTime(startTime, 85)),
                XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                Util.readClusterName(b1.getClusters().get(0)), ClusterType.TARGET, null, null);
        feedOriginalSubmit = InstanceUtil.setFeedCluster(feedOriginalSubmit,
                XmlUtil.createValidity(InstanceUtil.addMinsToTime(startTime, 40),
                        InstanceUtil.addMinsToTime(startTime, 110)),
                XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                Util.readClusterName(b3.getClusters().get(0)), ClusterType.SOURCE,
                "UK/${cluster.colo}", null);

        Util.print("Feed: " + feedOriginalSubmit);

        ServiceResponse response =
                prism.getFeedHelper().submitEntity(URLS.SUBMIT_URL, feedOriginalSubmit);
        Thread.sleep(10000);
        Util.assertSucceeded(response);

        //schedule on source
        response = prism.getFeedHelper().schedule(URLS.SCHEDULE_URL, feedOriginalSubmit);
        Thread.sleep(20000);
        Util.assertSucceeded(response);

        Assert.assertEquals(InstanceUtil
                .checkIfFeedCoordExist(cluster2.getFeedHelper(),
                        Util.readDatasetName(feedOriginalSubmit),
                        "REPLICATION"), 0);
        Assert.assertEquals(InstanceUtil
                .checkIfFeedCoordExist(cluster2.getFeedHelper(),
                        Util.readDatasetName(feedOriginalSubmit), "RETENTION"),
                1);
        Assert.assertEquals(InstanceUtil
                .checkIfFeedCoordExist(cluster3.getFeedHelper(),
                        Util.readDatasetName(feedOriginalSubmit),
                        "REPLICATION"), 0);
        Assert.assertEquals(InstanceUtil
                .checkIfFeedCoordExist(cluster3.getFeedHelper(),
                        Util.readDatasetName(feedOriginalSubmit), "RETENTION"),
                1);
        Assert.assertEquals(InstanceUtil
                .checkIfFeedCoordExist(cluster1.getFeedHelper(),
                        Util.readDatasetName(feedOriginalSubmit),
                        "REPLICATION"), 2);
        Assert.assertEquals(InstanceUtil
                .checkIfFeedCoordExist(cluster1.getFeedHelper(),
                        Util.readDatasetName(feedOriginalSubmit), "RETENTION"),
                1);

        //prepare updated Feed
        feedUpdated = InstanceUtil
                .setFeedCluster(feed, XmlUtil.createValidity(startTime,
                        InstanceUtil.addMinsToTime(startTime, 65)),
                        XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                        Util.readClusterName(b2.getClusters().get(0)), ClusterType.SOURCE,
                        null, null);
        feedUpdated = InstanceUtil.setFeedCluster(feedUpdated,
                XmlUtil.createValidity(InstanceUtil.addMinsToTime(startTime, 40),
                        InstanceUtil.addMinsToTime(startTime, 110)),
                XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                Util.readClusterName(b3.getClusters().get(0)), ClusterType.SOURCE, null, null);

        Util.print("Feed: " + feedUpdated);

        response = prism.getFeedHelper().update(feedUpdated, feedUpdated);
        Thread.sleep(20000);
        Util.assertSucceeded(response);

        response = cluster1.getFeedHelper().getEntityDefinition(URLS.GET_ENTITY_DEFINITION, feedUpdated);
        Util.assertFailed(response);

        prism.getFeedHelper()
                .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, feedUpdated);
        Assert.assertEquals(InstanceUtil
                .checkIfFeedCoordExist(cluster2.getFeedHelper(), Util.readDatasetName(feedUpdated),
                        "REPLICATION"), 0);
        Assert.assertEquals(InstanceUtil
                .checkIfFeedCoordExist(cluster2.getFeedHelper(), Util.readDatasetName(feedUpdated),
                        "RETENTION"), 2);
        Assert.assertEquals(InstanceUtil
                .checkIfFeedCoordExist(cluster3.getFeedHelper(), Util.readDatasetName(feedUpdated),
                        "REPLICATION"), 0);
        Assert.assertEquals(InstanceUtil
                .checkIfFeedCoordExist(cluster3.getFeedHelper(), Util.readDatasetName(feedUpdated),
                        "RETENTION"), 2);
        Assert.assertEquals(InstanceUtil
                .checkIfFeedCoordExist(cluster1.getFeedHelper(), Util.readDatasetName(feedUpdated),
                        "REPLICATION"), 0);
        Assert.assertEquals(InstanceUtil
                .checkIfFeedCoordExist(cluster1.getFeedHelper(), Util.readDatasetName(feedUpdated),
                        "RETENTION"), 0);
    }

    /*
    @Test(enabled = false)
    public void delete2SourceCluster() {

    }

    @Test(enabled = false)
    public void delete2TargetCluster() {

    }

    @Test(enabled = false)
    public void delete1Source1TargetCluster() {

    }
    */
}
