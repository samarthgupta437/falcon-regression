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
import org.apache.falcon.regression.core.helpers.PrismHelper;
import org.apache.falcon.regression.core.response.ServiceResponse;
import org.apache.falcon.regression.core.supportClasses.Consumer;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.core.util.Util.URLS;
import org.apache.falcon.regression.core.util.XmlUtil;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileWriter;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class PrismFeedReplicationTest {


    @BeforeMethod(alwaysRun = true)
    public void testName(Method method) throws Exception {
        Util.print("test name: " + method.getName());
        //restart server as precaution
        Util.restartService(ua1.getClusterHelper());
        Util.restartService(ua2.getClusterHelper());
        Util.restartService(ua3.getClusterHelper());


    }

    public PrismFeedReplicationTest() throws Exception {

    }

    PrismHelper prismHelper = new PrismHelper("prism.properties");

    ColoHelper ua1 = new ColoHelper("mk-qa.config.properties");

    ColoHelper ua2 = new ColoHelper("ivoryqa-1.config.properties");

    ColoHelper ua3 = new ColoHelper("gs1001.config.properties");

    @Test(enabled = true, timeOut = 1200000)
    public void multipleSourceOneTarget_oneSourceWithPartition() throws Exception {

        Bundle b1 = (Bundle) Util.readELBundles()[0][0];
        b1.generateUniqueBundle();
        Bundle b2 = (Bundle) Util.readELBundles()[0][0];
        b2.generateUniqueBundle();
        Bundle b3 = (Bundle) Util.readELBundles()[0][0];
        b3.generateUniqueBundle();

        try {
            b1 = new Bundle(b1, ua1.getEnvFileName());
            b2 = new Bundle(b2, ua2.getEnvFileName());
            b3 = new Bundle(b3, ua3.getEnvFileName());


            b1.setInputFeedDataPath(
                    "/samarthRetention/input-data/rawLogs/oozieExample/${YEAR}/${MONTH}/${DAY}/$" +
                            "{HOUR}/${MINUTE}/");

            Bundle.submitCluster(b1, b2, b3);


            String feed = b1.getDataSets().get(0);
            feed = InstanceUtil.setFeedCluster(feed,
                    XmlUtil.createValidity("2009-02-01T00:00Z", "2012-01-01T00:00Z"),
                    XmlUtil.createRtention("hours(10)", ActionType.DELETE), null,
                    ClusterType.SOURCE, null);


            String startTime = InstanceUtil.getTimeWrtSystemTime(-100);

            feed = InstanceUtil
                    .setFeedCluster(feed, XmlUtil.createValidity(startTime, "2099-01-01T00:00Z"),
                            XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                            Util.readClusterName(b2.getClusters().get(0)), ClusterType.SOURCE,
                            "US/${cluster.colo}");
            feed = InstanceUtil
                    .setFeedCluster(feed, XmlUtil.createValidity(startTime, "2099-01-01T00:00Z"),
                            XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                            Util.readClusterName(b1.getClusters().get(0)), ClusterType.TARGET,
                            null);
            feed = InstanceUtil
                    .setFeedCluster(feed, XmlUtil.createValidity(startTime, "2099-01-01T00:00Z"),
                            XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                            Util.readClusterName(b3.getClusters().get(0)), ClusterType.SOURCE,
                            null);


            Util.print("feed: " + feed);

            ServiceResponse r = prismHelper.getFeedHelper()
                    .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, feed);
            Thread.sleep(10000);
            Assert.assertTrue(r.getMessage().contains("FAILED"));

            Assert.assertTrue(
                    r.getMessage().contains("as there are more than one source clusters"));

        } finally {
            b1.deleteBundle(prismHelper);
            b2.deleteBundle(prismHelper);
            b3.deleteBundle(prismHelper);

        }
    }


    @Test(enabled = true, timeOut = 1200000)
    public void multipleSourceOneTargetTest() throws Exception {

        Bundle b1 = (Bundle) Util.readELBundles()[0][0];
        b1.generateUniqueBundle();
        Bundle b2 = (Bundle) Util.readELBundles()[0][0];
        b2.generateUniqueBundle();
        Bundle b3 = (Bundle) Util.readELBundles()[0][0];
        b3.generateUniqueBundle();

        try {
            b1 = new Bundle(b1, ua1.getEnvFileName());
            b2 = new Bundle(b2, ua2.getEnvFileName());
            b3 = new Bundle(b3, ua3.getEnvFileName());


            b1.setInputFeedDataPath(
                    "/samarthRetention/input-data/rawLogs/oozieExample/${YEAR}/${MONTH}/${DAY}/$" +
                            "{HOUR}/${MINUTE}/");

            b1.setCLusterColo("ua1");
            Util.print("cluster b1: " + b1.getClusters().get(0));

            ServiceResponse r = prismHelper.getClusterHelper()
                    .submitEntity(URLS.SUBMIT_URL, b1.getClusters().get(0));
            Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));


            b2.setCLusterColo("ua2");
            Util.print("cluster b2: " + b2.getClusters().get(0));
            r = prismHelper.getClusterHelper()
                    .submitEntity(URLS.SUBMIT_URL, b2.getClusters().get(0));
            Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));


            b3.setCLusterColo("ua3");
            Util.print("cluster b3: " + b3.getClusters().get(0));
            r = prismHelper.getClusterHelper()
                    .submitEntity(URLS.SUBMIT_URL, b3.getClusters().get(0));
            Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));


            String feed = b1.getDataSets().get(0);
            feed = InstanceUtil.setFeedCluster(feed,
                    XmlUtil.createValidity("2009-02-01T00:00Z", "2012-01-01T00:00Z"),
                    XmlUtil.createRtention("hours(10)", ActionType.DELETE), null,
                    ClusterType.SOURCE, null);

            String postFix = "/US/ua2";
            String prefix = b1.getFeedDataPathPrefix();
            Util.HDFSCleanup(ua2, prefix.substring(1));
            Util.lateDataReplenish(ua2, 120, 0, 1, prefix, postFix);


            postFix = "/UK/ua3";
            prefix = b1.getFeedDataPathPrefix();
            Util.HDFSCleanup(ua3, prefix.substring(1));
            Util.lateDataReplenish(ua3, 120, 0, 1, prefix, postFix);

            String startTime = InstanceUtil.getTimeWrtSystemTime(-100);

            feed = InstanceUtil
                    .setFeedCluster(feed, XmlUtil.createValidity(startTime, "2099-01-01T00:00Z"),
                            XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                            Util.readClusterName(b2.getClusters().get(0)), ClusterType.SOURCE,
                            "US/${cluster.colo}");
            feed = InstanceUtil
                    .setFeedCluster(feed, XmlUtil.createValidity(startTime, "2099-01-01T00:00Z"),
                            XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                            Util.readClusterName(b1.getClusters().get(0)), ClusterType.TARGET,
                            null);
            feed = InstanceUtil
                    .setFeedCluster(feed, XmlUtil.createValidity(startTime, "2099-01-01T00:00Z"),
                            XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                            Util.readClusterName(b3.getClusters().get(0)), ClusterType.SOURCE,
                            "UK/${cluster.colo}");


            Util.print("feed: " + feed);

            r = prismHelper.getFeedHelper().submitEntity(URLS.SUBMIT_URL, feed);
            Thread.sleep(10000);

            r = prismHelper.getFeedHelper().schedule(URLS.SCHEDULE_URL, feed);
            Thread.sleep(15000);

            Consumer consumer =
                    new Consumer("IVORY." + Util.readDatasetName(Util.getInputFeedFromBundle(b1)),
                            Util.readQueueLocationFromCluster(b1.getClusters().get(0)));
            consumer.start();

            String TargetBundleID = InstanceUtil
                    .getLatestBundleID(Util.readDatasetName(feed), "FEED", ua1.getFeedHelper());

            ArrayList<String> replicationCoordNameTarget = InstanceUtil
                    .getReplicationCoordName(TargetBundleID, ua1.getFeedHelper());
            String retentionCoordNameTarget =
                    InstanceUtil.getRetentionCoordName(TargetBundleID, ua1.getFeedHelper());
            ArrayList<String> replicationCoordIDTarget = InstanceUtil
                    .getReplicationCoordID(TargetBundleID, ua1.getFeedHelper());
            String retentionCoordIDTarget =
                    InstanceUtil.getRetentionCoordID(TargetBundleID, ua1.getFeedHelper());

            String SourceBundleID_01 = InstanceUtil
                    .getLatestBundleID(Util.readDatasetName(feed), "FEED", ua2.getFeedHelper());
            String retentionCoordNameSource_01 = InstanceUtil
                    .getRetentionCoordName(SourceBundleID_01, ua2.getFeedHelper());
            String retentionCoordIDSource_01 =
                    InstanceUtil.getRetentionCoordID(SourceBundleID_01, ua2.getFeedHelper());


            String SourceBundleID_02 = InstanceUtil
                    .getLatestBundleID(Util.readDatasetName(feed), "FEED", ua2.getFeedHelper());
            String retentionCoordNameSource_02 = InstanceUtil
                    .getRetentionCoordName(SourceBundleID_02, ua2.getFeedHelper());
            String retentionCoordIDSource_02 =
                    InstanceUtil.getRetentionCoordID(SourceBundleID_02, ua2.getFeedHelper());


            if (replicationCoordNameTarget.size() == 0 || retentionCoordNameTarget == null ||
                    replicationCoordIDTarget.size() == 0 || retentionCoordIDTarget == null ||
                    retentionCoordNameSource_01 == null || retentionCoordIDSource_01 == null ||
                    retentionCoordNameSource_02 == null || retentionCoordIDSource_02 == null)
                Assert.assertFalse(true,
                        "correct retention and replication coords were not created on source on " +
                                "target machines");

            consumer.stop();
            List<HashMap<String, String>> data = consumer.getMessageData();

            InstanceUtil.verifyDataInTarget(ua1.getFeedHelper(), feed);

            //	Util.print("counsumerData: "+ data.get(0).toString());


        } finally {
            b1.deleteBundle(prismHelper);
            b2.deleteBundle(prismHelper);
            b3.deleteBundle(prismHelper);

        }
    }


    @Test(enabled = true, timeOut = 1200000)
    public void onlyOneTarget() throws Exception {
        Bundle b1 = (Bundle) Util.readELBundles()[0][0];
        b1.generateUniqueBundle();
        Bundle b2 = (Bundle) Util.readELBundles()[0][0];
        b2.generateUniqueBundle();
        Bundle b3 = (Bundle) Util.readELBundles()[0][0];
        b3.generateUniqueBundle();

        try {
            b1 = new Bundle(b1, ua1.getEnvFileName());
            b2 = new Bundle(b2, ua2.getEnvFileName());
            b3 = new Bundle(b3, ua3.getEnvFileName());

            String feed = b1.getDataSets().get(0);
            feed = InstanceUtil.setFeedCluster(feed,
                    XmlUtil.createValidity("2009-02-01T00:00Z", "2012-01-01T00:00Z"),
                    XmlUtil.createRtention("hours(10)", ActionType.DELETE), null,
                    ClusterType.SOURCE, null);


            b1.setCLusterColo("ua1");
            Util.print("cluster b1: " + b1.getClusters().get(0));

            ServiceResponse r = prismHelper.getClusterHelper()
                    .submitEntity(URLS.SUBMIT_URL, b1.getClusters().get(0));
            Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));


            String startTime = InstanceUtil.getTimeWrtSystemTime(-100);

            feed = InstanceUtil.removeFeedPartitionsTag(feed);
            feed = InstanceUtil
                    .setFeedCluster(feed, XmlUtil.createValidity(startTime, "2099-01-01T00:00Z"),
                            XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                            Util.readClusterName(b1.getClusters().get(0)), ClusterType.TARGET,
                            null);

            Util.print("feed: " + feed);
            r = prismHelper.getFeedHelper().submitEntity(URLS.SUBMIT_URL, feed);
            Assert.assertTrue(r.getMessage().contains("FAILED"));
            Assert.assertTrue(
                    r.getMessage().contains("should have atleast one source cluster defined"));

        } finally {
            b1.deleteBundle(prismHelper);
            b2.deleteBundle(prismHelper);
            b3.deleteBundle(prismHelper);
        }
    }


    @Test(enabled = true, timeOut = 1200000)
    public void onlyOneSource() throws Exception {
        Bundle b1 = (Bundle) Util.readELBundles()[0][0];
        b1.generateUniqueBundle();
        Bundle b2 = (Bundle) Util.readELBundles()[0][0];
        b2.generateUniqueBundle();
        Bundle b3 = (Bundle) Util.readELBundles()[0][0];
        b3.generateUniqueBundle();

        try {
            b1 = new Bundle(b1, ua1.getEnvFileName());
            b2 = new Bundle(b2, ua2.getEnvFileName());
            b3 = new Bundle(b3, ua3.getEnvFileName());

            String feed = b1.getDataSets().get(0);
            feed = InstanceUtil.setFeedCluster(feed,
                    XmlUtil.createValidity("2009-02-01T00:00Z", "2012-01-01T00:00Z"),
                    XmlUtil.createRtention("hours(10)", ActionType.DELETE), null,
                    ClusterType.SOURCE, null);


            b1.setCLusterColo("ua1");
            Util.print("cluster b1: " + b1.getClusters().get(0));

            ServiceResponse r = prismHelper.getClusterHelper()
                    .submitEntity(URLS.SUBMIT_URL, b1.getClusters().get(0));
            Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));


            String startTime = InstanceUtil.getTimeWrtSystemTime(-100);

            feed = InstanceUtil.removeFeedPartitionsTag(feed);
            feed = InstanceUtil
                    .setFeedCluster(feed, XmlUtil.createValidity(startTime, "2099-01-01T00:00Z"),
                            XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                            Util.readClusterName(b1.getClusters().get(0)), ClusterType.SOURCE,
                            null);
            Util.print("feed: " + feed);
            r = prismHelper.getFeedHelper().submitEntity(URLS.SUBMIT_URL, feed);
            Thread.sleep(10000);
            Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));

            r = prismHelper.getFeedHelper().schedule(URLS.SCHEDULE_URL, feed);
            Thread.sleep(15000);

            Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));
            Thread.sleep(20000);


        } finally {
            b1.deleteBundle(prismHelper);
            b2.deleteBundle(prismHelper);
            b3.deleteBundle(prismHelper);
        }
    }

    @Test(enabled = true, timeOut = 1200000)
    public void muultipeSourceNoTarget_noPartitionAtTop() throws Exception {
        Bundle b1 = (Bundle) Util.readELBundles()[0][0];
        b1.generateUniqueBundle();
        Bundle b2 = (Bundle) Util.readELBundles()[0][0];
        b2.generateUniqueBundle();
        Bundle b3 = (Bundle) Util.readELBundles()[0][0];
        b3.generateUniqueBundle();

        try {
            b1 = new Bundle(b1, ua1.getEnvFileName());
            b2 = new Bundle(b2, ua2.getEnvFileName());
            b3 = new Bundle(b3, ua3.getEnvFileName());

            String feed = b1.getDataSets().get(0);
            feed = InstanceUtil.setFeedCluster(feed,
                    XmlUtil.createValidity("2009-02-01T00:00Z", "2012-01-01T00:00Z"),
                    XmlUtil.createRtention("hours(10)", ActionType.DELETE), null,
                    ClusterType.SOURCE, null);


            b1.setCLusterColo("ua1");
            Util.print("cluster b1: " + b1.getClusters().get(0));

            ServiceResponse r = prismHelper.getClusterHelper()
                    .submitEntity(URLS.SUBMIT_URL, b1.getClusters().get(0));
            Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));


            b2.setCLusterColo("ua2");
            Util.print("cluster b2: " + b2.getClusters().get(0));
            r = prismHelper.getClusterHelper()
                    .submitEntity(URLS.SUBMIT_URL, b2.getClusters().get(0));
            Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));


            b3.setCLusterColo("ua3");
            Util.print("cluster b3: " + b3.getClusters().get(0));
            r = prismHelper.getClusterHelper()
                    .submitEntity(URLS.SUBMIT_URL, b3.getClusters().get(0));
            Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));

            String startTime = InstanceUtil.getTimeWrtSystemTime(-100);

            feed = InstanceUtil.removeFeedPartitionsTag(feed);
            feed = InstanceUtil
                    .setFeedCluster(feed, XmlUtil.createValidity(startTime, "2099-01-01T00:00Z"),
                            XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                            Util.readClusterName(b1.getClusters().get(0)), ClusterType.SOURCE,
                            null);
            feed = InstanceUtil
                    .setFeedCluster(feed, XmlUtil.createValidity(startTime, "2099-01-01T00:00Z"),
                            XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                            Util.readClusterName(b2.getClusters().get(0)), ClusterType.SOURCE,
                            null);
            feed = InstanceUtil
                    .setFeedCluster(feed, XmlUtil.createValidity(startTime, "2099-01-01T00:00Z"),
                            XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                            Util.readClusterName(b3.getClusters().get(0)), ClusterType.SOURCE,
                            null);

            Util.print("feed: " + feed);
            r = prismHelper.getFeedHelper().submitEntity(URLS.SUBMIT_URL, feed);
            Thread.sleep(10000);

            r = prismHelper.getFeedHelper().schedule(URLS.SCHEDULE_URL, feed);
            Thread.sleep(15000);

            Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));
            Thread.sleep(20000);

            Assert.assertEquals(
                    InstanceUtil
                            .checkIfFeedCoordExist(ua2.getFeedHelper(), Util.readDatasetName(feed),
                                    "RETENTION"),
                    1);
            Assert.assertEquals(
                    InstanceUtil
                            .checkIfFeedCoordExist(ua3.getFeedHelper(), Util.readDatasetName(feed),
                                    "RETENTION"),
                    1);
            Assert.assertEquals(
                    InstanceUtil
                            .checkIfFeedCoordExist(ua1.getFeedHelper(), Util.readDatasetName(feed),
                                    "RETENTION"),
                    1);

        } finally {
            b1.deleteBundle(prismHelper);
            b2.deleteBundle(prismHelper);
            b3.deleteBundle(prismHelper);
        }
    }


    @Test(enabled = true, timeOut = 1200000)
    public void muultipeSourceNoTarget_noPartition() throws Exception {
        Bundle b1 = (Bundle) Util.readELBundles()[0][0];
        b1.generateUniqueBundle();
        Bundle b2 = (Bundle) Util.readELBundles()[0][0];
        b2.generateUniqueBundle();
        Bundle b3 = (Bundle) Util.readELBundles()[0][0];
        b3.generateUniqueBundle();

        try {
            b1 = new Bundle(b1, ua1.getEnvFileName());
            b2 = new Bundle(b2, ua2.getEnvFileName());
            b3 = new Bundle(b3, ua3.getEnvFileName());

            String feed = b1.getDataSets().get(0);
            feed = InstanceUtil.setFeedCluster(feed,
                    XmlUtil.createValidity("2009-02-01T00:00Z", "2012-01-01T00:00Z"),
                    XmlUtil.createRtention("hours(10)", ActionType.DELETE), null,
                    ClusterType.SOURCE, null);


            b1.setCLusterColo("ua1");
            Util.print("cluster b1: " + b1.getClusters().get(0));

            ServiceResponse r = prismHelper.getClusterHelper()
                    .submitEntity(URLS.SUBMIT_URL, b1.getClusters().get(0));
            Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));


            b2.setCLusterColo("ua2");
            Util.print("cluster b2: " + b2.getClusters().get(0));
            r = prismHelper.getClusterHelper()
                    .submitEntity(URLS.SUBMIT_URL, b2.getClusters().get(0));
            Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));


            b3.setCLusterColo("ua3");
            Util.print("cluster b3: " + b3.getClusters().get(0));
            r = prismHelper.getClusterHelper()
                    .submitEntity(URLS.SUBMIT_URL, b3.getClusters().get(0));
            Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));

            String startTime = InstanceUtil.getTimeWrtSystemTime(-100);

            feed = InstanceUtil
                    .setFeedCluster(feed, XmlUtil.createValidity(startTime, "2099-01-01T00:00Z"),
                            XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                            Util.readClusterName(b1.getClusters().get(0)), ClusterType.SOURCE,
                            null);
            feed = InstanceUtil
                    .setFeedCluster(feed, XmlUtil.createValidity(startTime, "2099-01-01T00:00Z"),
                            XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                            Util.readClusterName(b2.getClusters().get(0)), ClusterType.SOURCE,
                            null);
            feed = InstanceUtil
                    .setFeedCluster(feed, XmlUtil.createValidity(startTime, "2099-01-01T00:00Z"),
                            XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                            Util.readClusterName(b3.getClusters().get(0)), ClusterType.SOURCE,
                            null);

            Util.print("feed: " + feed);
            r = prismHelper.getFeedHelper().submitEntity(URLS.SUBMIT_URL, feed);
            Thread.sleep(10000);

            r = prismHelper.getFeedHelper().schedule(URLS.SCHEDULE_URL, feed);
            Thread.sleep(15000);

            Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));
            Thread.sleep(20000);

            Assert.assertEquals(
                    InstanceUtil
                            .checkIfFeedCoordExist(ua2.getFeedHelper(), Util.readDatasetName(feed),
                                    "RETENTION"),
                    1);
            Assert.assertEquals(
                    InstanceUtil
                            .checkIfFeedCoordExist(ua3.getFeedHelper(), Util.readDatasetName(feed),
                                    "RETENTION"),
                    1);
            Assert.assertEquals(
                    InstanceUtil
                            .checkIfFeedCoordExist(ua1.getFeedHelper(), Util.readDatasetName(feed),
                                    "RETENTION"),
                    1);

        } finally {
            b1.deleteBundle(prismHelper);
            b2.deleteBundle(prismHelper);
            b3.deleteBundle(prismHelper);
        }
    }


    @SuppressWarnings("deprecation")

    @Test(enabled = true, timeOut = 1200000)
    public void oneSourceOneTarget_FeedResume() throws Exception {

        Bundle b1 = (Bundle) Util.readELBundles()[0][0];
        b1.generateUniqueBundle();
        Bundle b2 = (Bundle) Util.readELBundles()[0][0];
        b2.generateUniqueBundle();

        try {
            b1 = new Bundle(b1, ua1.getEnvFileName());

            b2 = new Bundle(b2, ua2.getEnvFileName());

            b1.setInputFeedDataPath(
                    "/samarthRetention/input-data/rawLogs/oozieExample/${YEAR}/${MONTH}/${DAY}/$" +
                            "{HOUR}/${MINUTE}");

            b1.setCLusterColo("ua1");
            Util.print("cluster b1: " + b1.getClusters().get(0));
            ServiceResponse r = prismHelper.getClusterHelper()
                    .submitEntity(URLS.SUBMIT_URL, b1.getClusters().get(0));
            Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));


            b2.setCLusterColo("ua2");
            Util.print("cluster b2: " + b2.getClusters().get(0));
            r = prismHelper.getClusterHelper()
                    .submitEntity(URLS.SUBMIT_URL, b2.getClusters().get(0));
            Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));


            String feed = b1.getDataSets().get(0);
            feed = InstanceUtil.setFeedCluster(feed,
                    XmlUtil.createValidity("2009-02-01T00:00Z", "2012-01-01T00:00Z"),
                    XmlUtil.createRtention("hours(10)", ActionType.DELETE), null,
                    ClusterType.SOURCE, null);

            String prefix = b1.getFeedDataPathPrefix();
            Util.HDFSCleanup(prefix.substring(1));
            Util.lateDataReplenish(200, 0, 1, prefix);

            String startTime = InstanceUtil.getTimeWrtSystemTime(-100);

            Util.print("Source: ua1   Target: ua2");
            feed = InstanceUtil
                    .setFeedCluster(feed, XmlUtil.createValidity(startTime, "2099-01-01T00:00Z"),
                            XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                            Util.readClusterName(b2.getClusters().get(0)), ClusterType.TARGET,
                            null);
            feed = InstanceUtil
                    .setFeedCluster(feed, XmlUtil.createValidity(startTime, "2099-01-01T00:00Z"),
                            XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                            Util.readClusterName(b1.getClusters().get(0)), ClusterType.SOURCE,
                            null);


            Util.print("feed: " + feed);

            r = prismHelper.getFeedHelper().submitEntity(URLS.SUBMIT_URL, feed);


            r = prismHelper.getFeedHelper().schedule(URLS.SCHEDULE_URL, feed);
            Thread.sleep(30000);

            r = prismHelper.getFeedHelper().suspend(URLS.SUSPEND_URL, feed);
            Thread.sleep(30000);

            r = prismHelper.getFeedHelper().resume(URLS.RESUME_URL, feed);


        } finally {
            b1.deleteBundle(prismHelper);
            b2.deleteBundle(prismHelper);

        }
    }

    @Test(enabled = true, timeOut = 1200000)
    public void oneSourceOneTarget_AlalibilityFlag() throws Exception {

        String dependency = "depends.txt";
        Bundle b1 = (Bundle) Util.readAvailabilityBUndle()[0][0];
        b1.setInputFeedAvailabilityFlag(dependency);

        b1.generateUniqueBundle();
        Bundle b2 = (Bundle) Util.readAvailabilityBUndle()[0][0];
        b2.generateUniqueBundle();

        try {
            b1 = new Bundle(b1, ua1.getEnvFileName());

            b2 = new Bundle(b2, ua2.getEnvFileName());

            b1.setInputFeedDataPath(
                    "/samarthRetention/input-data/rawLogs/oozieExample/${YEAR}/${MONTH}/${DAY}/$" +
                            "{HOUR" +
                            "}/${MINUTE}");

            b1.setCLusterColo("ua1");
            Util.print("cluster b1: " + b1.getClusters().get(0));
            ServiceResponse r = prismHelper.getClusterHelper()
                    .submitEntity(URLS.SUBMIT_URL, b1.getClusters().get(0));
            Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));


            b2.setCLusterColo("ua2");
            Util.print("cluster b2: " + b2.getClusters().get(0));
            r = prismHelper.getClusterHelper()
                    .submitEntity(URLS.SUBMIT_URL, b2.getClusters().get(0));
            Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));


            String feed = b1.getDataSets().get(0);
            feed = InstanceUtil.setFeedCluster(feed,
                    XmlUtil.createValidity("2009-02-01T00:00Z", "2012-01-01T00:00Z"),
                    XmlUtil.createRtention("hours(10)", ActionType.DELETE), null,
                    ClusterType.SOURCE, null);

            String prefix = b1.getFeedDataPathPrefix();
            Util.HDFSCleanup(ua1, prefix.substring(1));
            Util.lateDataReplenish(ua1, 200, 0, 1, prefix);

            String startTime = InstanceUtil.getTimeWrtSystemTime(-100);

            Util.print("Source: ua1   Target: ua2");
            feed = InstanceUtil
                    .setFeedCluster(feed, XmlUtil.createValidity(startTime, "2099-01-01T00:00Z"),
                            XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                            Util.readClusterName(b2.getClusters().get(0)), ClusterType.TARGET,
                            null);
            feed = InstanceUtil
                    .setFeedCluster(feed, XmlUtil.createValidity(startTime, "2099-01-01T00:00Z"),
                            XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                            Util.readClusterName(b1.getClusters().get(0)), ClusterType.SOURCE,
                            null);


            Util.print("feed: " + feed);

            r = prismHelper.getFeedHelper().submitEntity(URLS.SUBMIT_URL, feed);


            r = prismHelper.getFeedHelper().schedule(URLS.SCHEDULE_URL, feed);

        } finally {
            b1.deleteBundle(prismHelper);
            b2.deleteBundle(prismHelper);
        }
    }


    @Test(enabled = true, timeOut = 1200000)
    public void oneSourceOneTarget_PrismTarget() throws Exception {

        Bundle b1 = (Bundle) Util.readELBundles()[0][0];
        b1.generateUniqueBundle();
        Bundle b2 = (Bundle) Util.readELBundles()[0][0];
        b2.generateUniqueBundle();

        try {
            b1 = new Bundle(b1, ua1.getEnvFileName());

            b2 = new Bundle(b2, ua2.getEnvFileName());

            b1.setInputFeedDataPath(
                    "/samarthRetention/input-data/rawLogs/oozieExample/${YEAR}/${MONTH}/${DAY}/$" +
                            "{HOUR}/${MINUTE}");

            b1.setCLusterColo("ua1");
            Util.print("cluster b1: " + b1.getClusters().get(0));
            ServiceResponse r = prismHelper.getClusterHelper()
                    .submitEntity(URLS.SUBMIT_URL, b1.getClusters().get(0));
            Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));


            b2.setCLusterColo("ua2");
            Util.print("cluster b2: " + b2.getClusters().get(0));
            r = prismHelper.getClusterHelper()
                    .submitEntity(URLS.SUBMIT_URL, b2.getClusters().get(0));
            Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));


            String feed = b1.getDataSets().get(0);
            feed = InstanceUtil.setFeedCluster(feed,
                    XmlUtil.createValidity("2009-02-01T00:00Z", "2012-01-01T00:00Z"),
                    XmlUtil.createRtention("hours(10)", ActionType.DELETE), null,
                    ClusterType.SOURCE, null);

            String prefix = b1.getFeedDataPathPrefix();
            Util.HDFSCleanup(ua1, prefix.substring(1));
            Util.lateDataReplenish(ua1, 120, 0, 1, prefix);

            //clear target cluster
            Util.HDFSCleanup(ua2, prefix.substring(1));


            String startTime = InstanceUtil.getTimeWrtSystemTime(-100);

            Util.print("Source: ua1   Target: ua2");
            feed = InstanceUtil
                    .setFeedCluster(feed, XmlUtil.createValidity(startTime, "2099-01-01T00:00Z"),
                            XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                            Util.readClusterName(b2.getClusters().get(0)), ClusterType.TARGET,
                            null);
            feed = InstanceUtil
                    .setFeedCluster(feed, XmlUtil.createValidity(startTime, "2099-01-01T00:00Z"),
                            XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                            Util.readClusterName(b1.getClusters().get(0)), ClusterType.SOURCE,
                            null);


            Util.print("feed: " + feed);

            r = prismHelper.getFeedHelper().submitEntity(URLS.SUBMIT_URL, feed);


            Consumer consumer_mkqa =
                    new Consumer("IVORY." + Util.readDatasetName(Util.getInputFeedFromBundle(b1)),
                            ua1.getClusterHelper().getActiveMQ());
            consumer_mkqa.start();


            Consumer consumer_ivoryqa1 =
                    new Consumer("IVORY." + Util.readDatasetName(Util.getInputFeedFromBundle(b1)),
                            ua2.getClusterHelper().getActiveMQ());
            consumer_ivoryqa1.start();

            r = prismHelper.getFeedHelper().schedule(URLS.SCHEDULE_URL, feed);

            consumer_mkqa.stop();
            List<HashMap<String, String>> data_mkqa = consumer_mkqa.getMessageData();

            consumer_ivoryqa1.stop();
            List<HashMap<String, String>> data_ivoryqa1 = consumer_ivoryqa1.getMessageData();


            //mkqaJMS file
            for (int i = 0; i < data_mkqa.size(); i++)
                Util.print("counsumerData from data_mkqa: " + data_mkqa.get(i).toString());

            File mkqaJMS = new File("src/test/resources/mkqaJMS.txt");


            mkqaJMS.createNewFile();


            for (int i = 0; i < data_mkqa.size(); i++) {
                Util.print("counsumerData from data_mkqa: " + data_mkqa.get(i).toString());


                FileWriter fr = new FileWriter(mkqaJMS);
                fr.append(data_mkqa.get(i).toString());
                fr.close();

            }

            File ivoryJMS = new File("src/test/resources/ivoryJMS.txt");


            ivoryJMS.createNewFile();


            for (int i = 0; i < data_ivoryqa1.size(); i++) {
                Util.print("counsumerData from data_ivoryqa1: " + data_ivoryqa1.get(i).toString());


                FileWriter fr = new FileWriter(ivoryJMS);
                fr.append(data_ivoryqa1.get(i).toString());
                fr.close();

            }

        } finally {
            b1.deleteBundle(prismHelper);
            b2.deleteBundle(prismHelper);

        }
    }

    @Test(enabled = true, timeOut = 1200000)
    public void oneSourceOneTarget_extraDataTargetDelete() throws Exception {

        Bundle b1 = (Bundle) Util.readELBundles()[0][0];
        b1.generateUniqueBundle();
        Bundle b2 = (Bundle) Util.readELBundles()[0][0];
        b2.generateUniqueBundle();

        try {
            b1 = new Bundle(b1, ua1.getEnvFileName());

            b2 = new Bundle(b2, ua3.getEnvFileName());

            b1.setInputFeedDataPath(
                    "/replicationDeleteExtraTargetTest/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");

            b1.setCLusterColo("ua1");
            b2.setCLusterColo("ua3");

            Bundle.submitCluster(b1, b2);

            String feed = b1.getDataSets().get(0);
            feed = InstanceUtil.setFeedCluster(feed,
                    XmlUtil.createValidity("2009-02-01T00:00Z", "2012-01-01T00:00Z"),
                    XmlUtil.createRtention("hours(10)", ActionType.DELETE), null,
                    ClusterType.SOURCE, null);

            String prefix = b1.getFeedDataPathPrefix();
            Util.HDFSCleanup(ua1, prefix.substring(1));
            Util.HDFSCleanup(ua3, prefix.substring(1));

            InstanceUtil.putFileInFolders(ua1, InstanceUtil.createEmptyDirWithinDatesAndPrefix(ua1,
                    InstanceUtil.oozieDateToDate(InstanceUtil.getTimeWrtSystemTime(-20)),
                    InstanceUtil.oozieDateToDate(InstanceUtil.getTimeWrtSystemTime(10)),
                    "/replicationDeleteExtraTargetTest/", 1), "log_01.txt", "xmlFileName.xml",
                    "_SUCCESS");
            InstanceUtil.putFileInFolders(ua3, InstanceUtil.createEmptyDirWithinDatesAndPrefix(ua3,
                    InstanceUtil.oozieDateToDate(InstanceUtil.getTimeWrtSystemTime(-20)),
                    InstanceUtil.oozieDateToDate(InstanceUtil.getTimeWrtSystemTime(10)),
                    "/replicationDeleteExtraTargetTest/", 1), "QABackLog.txt", "xmlFileName.xml");


            String startTime = InstanceUtil.getTimeWrtSystemTime(-10);

            feed = InstanceUtil
                    .setFeedCluster(feed, XmlUtil.createValidity(startTime, "2099-01-01T00:00Z"),
                            XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                            Util.readClusterName(b1.getClusters().get(0)), ClusterType.SOURCE,
                            null);
            feed = InstanceUtil
                    .setFeedCluster(feed, XmlUtil.createValidity(startTime, "2099-01-01T00:00Z"),
                            XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                            Util.readClusterName(b2.getClusters().get(0)), ClusterType.TARGET,
                            null);


            Util.print("feed: " + feed);

            ServiceResponse r = prismHelper.getFeedHelper().submitEntity(URLS.SUBMIT_URL, feed);
            r = prismHelper.getFeedHelper().schedule(URLS.SCHEDULE_URL, feed);

		

	/*		String bundleID = instanceUtil.getLatestBundleID(Util.readDatasetName(feed),"FEED",ua1.getClusterHelper());

			if(bundleID!=null)
			{
				String retentionCoord = instanceUtil.getRetentionCoordID(bundleID, ua1.getClusterHelper());
				if(retentionCoord== null)
					Assert.assertTrue(false);
			}
*/


        } finally {
            b1.deleteBundle(prismHelper);
            b2.deleteBundle(prismHelper);

        }
    }


}
