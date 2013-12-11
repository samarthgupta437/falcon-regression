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
import org.apache.falcon.regression.core.helpers.PrismHelper;
import org.apache.falcon.regression.core.response.ServiceResponse;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.core.util.Util.URLS;
import org.apache.falcon.regression.core.util.XmlUtil;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;


/**
 * Feed cluster update tests.
 */
@SuppressWarnings("deprecation")
public class FeedClusterUpdateTest {

    private final PrismHelper prismHelper = new PrismHelper("prism.properties");

    private final ColoHelper ua1 = new ColoHelper("mk-qa.config.properties");
    private final ColoHelper ua2 = new ColoHelper("ivoryqa-1.config.properties");
    private final ColoHelper ua3 = new ColoHelper("gs1001.config.properties");

    @BeforeMethod(alwaysRun = true)
    public void testName(Method method) throws Exception {
        Util.print("test name: " + method.getName());
        //restart server as precaution
        Util.restartService(ua1.getClusterHelper());
        Util.restartService(ua2.getClusterHelper());
        Util.restartService(ua3.getClusterHelper());
    }


    @BeforeClass(alwaysRun = true)
    public void createTestData() throws Exception {
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


            String postFix = "/US/ua2";
            String prefix = b1.getFeedDataPathPrefix();
            Util.HDFSCleanup(ua2, prefix.substring(1));
            Util.lateDataReplenish(ua2, 80, 0, 1, prefix, postFix);


            postFix = "/UK/ua3";
            prefix = b1.getFeedDataPathPrefix();
            Util.HDFSCleanup(ua3, prefix.substring(1));
            Util.lateDataReplenish(ua3, 80, 0, 1, prefix, postFix);

        } finally {

            b1.deleteBundle(prismHelper);
            b2.deleteBundle(prismHelper);
            b3.deleteBundle(prismHelper);
        }
    }


    @Test(enabled = false, groups = {"multiCluster"})
    public void addSourceCluster() throws Exception {
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

            Util.submitAllClusters(b1, b2, b3);


            String feed = b1.getDataSets().get(0);
            feed = InstanceUtil.setFeedCluster(feed,
                    XmlUtil.createValidity("2009-02-01T00:00Z", "2012-01-01T00:00Z"),
                    XmlUtil.createRtention("hours(10)", ActionType.DELETE), null,
                    ClusterType.SOURCE, null);


            String startTime = InstanceUtil.getTimeWrtSystemTime(-50);

            String feedOriginalSubmit;
            String feedUpdated;

            //add one source and one target , schedule only on source
            feedOriginalSubmit = InstanceUtil.setFeedCluster(feed, XmlUtil.createValidity(startTime,
                    InstanceUtil.addMinsToTime(startTime, 65)),
                    XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                    Util.readClusterName(b2.getClusters().get(0)), ClusterType.SOURCE,
                    null);
            feedOriginalSubmit = InstanceUtil.setFeedCluster(feedOriginalSubmit,
                    XmlUtil.createValidity(InstanceUtil.addMinsToTime(startTime, 20),
                            InstanceUtil.addMinsToTime(startTime, 85)),
                    XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                    Util.readClusterName(b1.getClusters().get(0)), ClusterType.TARGET, null);

            Util.print("Feed: " + feedOriginalSubmit);

            ServiceResponse response =
                    prismHelper.getFeedHelper().submitEntity(URLS.SUBMIT_URL, feedOriginalSubmit);
            Thread.sleep(10000);
            Util.assertSucceeded(response);

            //schedule on source

            response = ua2.getFeedHelper().schedule(URLS.SCHEDULE_URL, feedOriginalSubmit);
            Thread.sleep(20000);
            Util.assertSucceeded(response);

            Assert.assertEquals(InstanceUtil
                    .checkIfFeedCoordExist(ua2.getFeedHelper(),
                            Util.readDatasetName(feedOriginalSubmit),
                            "REPLICATION"), 0);
            Assert.assertEquals(InstanceUtil
                    .checkIfFeedCoordExist(ua2.getFeedHelper(),
                            Util.readDatasetName(feedOriginalSubmit), "RETENTION" +
                            ""), 1);
            Assert.assertEquals(InstanceUtil
                    .checkIfFeedCoordExist(ua3.getFeedHelper(),
                            Util.readDatasetName(feedOriginalSubmit),
                            "REPLICATION"), 0);
            Assert.assertEquals(InstanceUtil
                    .checkIfFeedCoordExist(ua3.getFeedHelper(),
                            Util.readDatasetName(feedOriginalSubmit), "RETENTION"),
                    0);
            Assert.assertEquals(InstanceUtil
                    .checkIfFeedCoordExist(ua1.getFeedHelper(),
                            Util.readDatasetName(feedOriginalSubmit),
                            "REPLICATION"), 0);
            Assert.assertEquals(InstanceUtil
                    .checkIfFeedCoordExist(ua1.getFeedHelper(),
                            Util.readDatasetName(feedOriginalSubmit),
                            "RETENTION"), 0);

            //prepare updated Feed
            feedUpdated = InstanceUtil.setFeedCluster(
                    feed, XmlUtil.createValidity(startTime,
                    InstanceUtil.addMinsToTime(startTime, 65)),
                    XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                    Util.readClusterName(b2.getClusters().get(0)), ClusterType.SOURCE,
                    "US/${cluster.colo}");
            feedUpdated = InstanceUtil.setFeedCluster(feedUpdated,
                    XmlUtil.createValidity(InstanceUtil.addMinsToTime(startTime, 20),
                            InstanceUtil.addMinsToTime(startTime, 85)),
                    XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                    Util.readClusterName(b1.getClusters().get(0)), ClusterType.TARGET, null);
            feedUpdated = InstanceUtil.setFeedCluster(feedUpdated,
                    XmlUtil.createValidity(InstanceUtil.addMinsToTime(startTime, 40),
                            InstanceUtil.addMinsToTime(startTime, 110)),
                    XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                    Util.readClusterName(b3.getClusters().get(0)), ClusterType.SOURCE,
                    "UK/${cluster.colo}");


            response = prismHelper.getFeedHelper().update(feedUpdated, feedUpdated);
            Thread.sleep(20000);
            Util.assertSucceeded(response);

            prismHelper.getFeedHelper()
                    .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, feedUpdated);
            Assert.assertEquals(InstanceUtil
                    .checkIfFeedCoordExist(ua2.getFeedHelper(), Util.readDatasetName(feedUpdated),
                            "REPLICATION"), 0);
            Assert.assertEquals(InstanceUtil
                    .checkIfFeedCoordExist(ua2.getFeedHelper(), Util.readDatasetName(feedUpdated),
                            "RETENTION"), 2);
            Assert.assertEquals(InstanceUtil
                    .checkIfFeedCoordExist(ua3.getFeedHelper(), Util.readDatasetName(feedUpdated),
                            "REPLICATION"), 0);
            Assert.assertEquals(InstanceUtil
                    .checkIfFeedCoordExist(ua3.getFeedHelper(), Util.readDatasetName(feedUpdated),
                            "RETENTION"), 1);
            Assert.assertEquals(InstanceUtil
                    .checkIfFeedCoordExist(ua1.getFeedHelper(), Util.readDatasetName(feedUpdated),
                            "REPLICATION"), 1);
            Assert.assertEquals(InstanceUtil
                    .checkIfFeedCoordExist(ua1.getFeedHelper(), Util.readDatasetName(feedUpdated),
                            "RETENTION"), 2);


        } finally {

            b1.deleteBundle(prismHelper);
            b2.deleteBundle(prismHelper);
            b3.deleteBundle(prismHelper);
        }
    }

    @Test(enabled = false, groups = {"multiCluster"})
    public void addTargetCluster() throws Exception {
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

            Util.submitAllClusters(b1, b2, b3);


            String feed = b1.getDataSets().get(0);
            feed = InstanceUtil.setFeedCluster(feed,
                    XmlUtil.createValidity("2009-02-01T00:00Z", "2012-01-01T00:00Z"),
                    XmlUtil.createRtention("hours(10)", ActionType.DELETE), null,
                    ClusterType.SOURCE, null);


            String startTime = InstanceUtil.getTimeWrtSystemTime(-50);


            String feedOriginalSubmit;
            String feedUpdated;

            //add one source and one target , schedule only on source
            feedOriginalSubmit = InstanceUtil
                    .setFeedCluster(feed, XmlUtil.createValidity(startTime,
                            InstanceUtil.addMinsToTime(startTime, 65)),
                            XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                            Util.readClusterName(b2.getClusters().get(0)), ClusterType.SOURCE,
                            null);
            feedOriginalSubmit = InstanceUtil.setFeedCluster(feedOriginalSubmit,
                    XmlUtil.createValidity(InstanceUtil.addMinsToTime(startTime, 40),
                            InstanceUtil.addMinsToTime(startTime, 110)),
                    XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                    Util.readClusterName(b3.getClusters().get(0)), ClusterType.SOURCE,
                    "UK/${cluster.colo}");

            Util.print("Feed: " + feedOriginalSubmit);

            ServiceResponse response =
                    prismHelper.getFeedHelper().submitEntity(URLS.SUBMIT_URL, feedOriginalSubmit);
            Thread.sleep(10000);
            Util.assertSucceeded(response);

            //schedule on source

            response = ua2.getFeedHelper().schedule(URLS.SCHEDULE_URL, feedOriginalSubmit);
            Thread.sleep(20000);
            Util.assertSucceeded(response);

            Assert.assertEquals(InstanceUtil
                    .checkIfFeedCoordExist(ua2.getFeedHelper(),
                            Util.readDatasetName(feedOriginalSubmit),
                            "REPLICATION"), 0);
            Assert.assertEquals(InstanceUtil
                    .checkIfFeedCoordExist(ua2.getFeedHelper(),
                            Util.readDatasetName(feedOriginalSubmit),
                            "RETENTION"), 1);
            Assert.assertEquals(InstanceUtil
                    .checkIfFeedCoordExist(ua3.getFeedHelper(),
                            Util.readDatasetName(feedOriginalSubmit),
                            "REPLICATION"), 0);
            Assert.assertEquals(InstanceUtil
                    .checkIfFeedCoordExist(ua3.getFeedHelper(),
                            Util.readDatasetName(feedOriginalSubmit),
                            "RETENTION"), 0);
            Assert.assertEquals(InstanceUtil
                    .checkIfFeedCoordExist(ua1.getFeedHelper(),
                            Util.readDatasetName(feedOriginalSubmit),
                            "REPLICATION"), 0);
            Assert.assertEquals(InstanceUtil
                    .checkIfFeedCoordExist(ua1.getFeedHelper(),
                            Util.readDatasetName(feedOriginalSubmit), "RETENTION"),
                    0);

            //prepare updated Feed
            feedUpdated = InstanceUtil
                    .setFeedCluster(feed, XmlUtil.createValidity(startTime,
                            InstanceUtil.addMinsToTime(startTime, 65)),
                            XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                            Util.readClusterName(b2.getClusters().get(0)), ClusterType.SOURCE,
                            "US/${cluster.colo}");
            feedUpdated = InstanceUtil.setFeedCluster(feedUpdated,
                    XmlUtil.createValidity(InstanceUtil.addMinsToTime(startTime, 20),
                            InstanceUtil.addMinsToTime(startTime, 85)),
                    XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                    Util.readClusterName(b1.getClusters().get(0)), ClusterType.TARGET, null);
            feedUpdated = InstanceUtil.setFeedCluster(feedUpdated,
                    XmlUtil.createValidity(InstanceUtil.addMinsToTime(startTime, 40),
                            InstanceUtil.addMinsToTime(startTime, 110)),
                    XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                    Util.readClusterName(b3.getClusters().get(0)), ClusterType.SOURCE,
                    "UK/${cluster.colo}");

            Util.print("Updated Feed: " + feedUpdated);

            response = prismHelper.getFeedHelper().update(feedUpdated, feedUpdated);
            Thread.sleep(20000);
            Util.assertSucceeded(response);

            prismHelper.getFeedHelper()
                    .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, feedUpdated);

            Assert.assertEquals(InstanceUtil
                    .checkIfFeedCoordExist(ua2.getFeedHelper(), Util.readDatasetName(feedUpdated),
                            "REPLICATION"), 0);
            Assert.assertEquals(InstanceUtil
                    .checkIfFeedCoordExist(ua2.getFeedHelper(), Util.readDatasetName(feedUpdated),
                            "RETENTION"), 2);
            Assert.assertEquals(InstanceUtil
                    .checkIfFeedCoordExist(ua3.getFeedHelper(), Util.readDatasetName(feedUpdated),
                            "REPLICATION"), 0);
            Assert.assertEquals(InstanceUtil
                    .checkIfFeedCoordExist(ua3.getFeedHelper(), Util.readDatasetName(feedUpdated),
                            "RETENTION"), 1);
            Assert.assertEquals(InstanceUtil
                    .checkIfFeedCoordExist(ua1.getFeedHelper(), Util.readDatasetName(feedUpdated),
                            "REPLICATION"), 2);
            Assert.assertEquals(InstanceUtil
                    .checkIfFeedCoordExist(ua1.getFeedHelper(), Util.readDatasetName(feedUpdated),
                            "RETENTION"), 1);


        } finally {

            b1.deleteBundle(prismHelper);
            b2.deleteBundle(prismHelper);
            b3.deleteBundle(prismHelper);
        }
    }

    @Test(enabled = false, groups = {"multiCluster"})
    public void add2SourceCluster() throws Exception {
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

            Util.submitAllClusters(b1, b2, b3);


            String feed = b1.getDataSets().get(0);
            feed = InstanceUtil.setFeedCluster(feed,
                    XmlUtil.createValidity("2009-02-01T00:00Z", "2012-01-01T00:00Z"),
                    XmlUtil.createRtention("hours(10)", ActionType.DELETE), null,
                    ClusterType.SOURCE, null);


            String startTime = InstanceUtil.getTimeWrtSystemTime(-50);


            String feedOriginalSubmit;
            String feedUpdated;

            //add one source and one target , schedule only on source
            feedOriginalSubmit = InstanceUtil
                    .setFeedCluster(feed, XmlUtil.createValidity(startTime,
                            InstanceUtil.addMinsToTime(startTime, 65)),
                            XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                            Util.readClusterName(b2.getClusters().get(0)), ClusterType.SOURCE,
                            null);

            Util.print("Feed: " + feedOriginalSubmit);

            ServiceResponse response = prismHelper.getFeedHelper().submitEntity(URLS.SUBMIT_URL, feedOriginalSubmit);
            Thread.sleep(10000);
            Util.assertSucceeded(response);

            //schedule on source

            response = ua2.getFeedHelper().schedule(URLS.SCHEDULE_URL, feedOriginalSubmit);
            Thread.sleep(20000);
            Util.assertSucceeded(response);

            Assert.assertEquals(InstanceUtil
                    .checkIfFeedCoordExist(ua2.getFeedHelper(),
                            Util.readDatasetName(feedOriginalSubmit),
                            "REPLICATION"), 0);
            Assert.assertEquals(InstanceUtil
                    .checkIfFeedCoordExist(ua2.getFeedHelper(),
                            Util.readDatasetName(feedOriginalSubmit), "RETENTION"),
                    1);
            Assert.assertEquals(InstanceUtil
                    .checkIfFeedCoordExist(ua3.getFeedHelper(),
                            Util.readDatasetName(feedOriginalSubmit),
                            "REPLICATION"), 0);
            Assert.assertEquals(InstanceUtil
                    .checkIfFeedCoordExist(ua3.getFeedHelper(),
                            Util.readDatasetName(feedOriginalSubmit), "RETENTION"),
                    0);
            Assert.assertEquals(InstanceUtil
                    .checkIfFeedCoordExist(ua1.getFeedHelper(),
                            Util.readDatasetName(feedOriginalSubmit),
                            "REPLICATION"), 0);
            Assert.assertEquals(InstanceUtil
                    .checkIfFeedCoordExist(ua1.getFeedHelper(),
                            Util.readDatasetName(feedOriginalSubmit), "RETENTION"),
                    0);

            //prepare updated Feed
            feedUpdated = InstanceUtil
                    .setFeedCluster(feed, XmlUtil.createValidity(startTime,
                            InstanceUtil.addMinsToTime(startTime, 65)),
                            XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                            Util.readClusterName(b2.getClusters().get(0)), ClusterType.SOURCE,
                            "US/${cluster.colo}");
            feedUpdated = InstanceUtil.setFeedCluster(feedUpdated,
                    XmlUtil.createValidity(InstanceUtil.addMinsToTime(startTime, 20),
                            InstanceUtil.addMinsToTime(startTime, 85)),
                    XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                    Util.readClusterName(b1.getClusters().get(0)), ClusterType.SOURCE, null);
            feedUpdated = InstanceUtil.setFeedCluster(feedUpdated,
                    XmlUtil.createValidity(InstanceUtil.addMinsToTime(startTime, 40),
                            InstanceUtil.addMinsToTime(startTime, 110)),
                    XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                    Util.readClusterName(b3.getClusters().get(0)), ClusterType.SOURCE,
                    "UK/${cluster.colo}");

            Util.print("Updated Feed: " + feedUpdated);

            response = prismHelper.getFeedHelper().update(feedUpdated, feedUpdated);
            Thread.sleep(20000);
            Util.assertSucceeded(response);

            prismHelper.getFeedHelper()
                    .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, feedUpdated);

            Assert.assertEquals(InstanceUtil
                    .checkIfFeedCoordExist(ua2.getFeedHelper(), Util.readDatasetName(feedUpdated),
                            "REPLICATION"), 0);
            Assert.assertEquals(InstanceUtil
                    .checkIfFeedCoordExist(ua2.getFeedHelper(), Util.readDatasetName(feedUpdated),
                            "RETENTION"), 2);
            Assert.assertEquals(InstanceUtil
                    .checkIfFeedCoordExist(ua3.getFeedHelper(), Util.readDatasetName(feedUpdated),
                            "REPLICATION"), 0);
            Assert.assertEquals(InstanceUtil
                    .checkIfFeedCoordExist(ua3.getFeedHelper(), Util.readDatasetName(feedUpdated),
                            "RETENTION"), 1);
            Assert.assertEquals(InstanceUtil
                    .checkIfFeedCoordExist(ua1.getFeedHelper(), Util.readDatasetName(feedUpdated),
                            "REPLICATION"), 0);
            Assert.assertEquals(InstanceUtil
                    .checkIfFeedCoordExist(ua1.getFeedHelper(), Util.readDatasetName(feedUpdated),
                            "RETENTION"), 1);

        } finally {

            b1.deleteBundle(prismHelper);
            b2.deleteBundle(prismHelper);
            b3.deleteBundle(prismHelper);
        }
    }

    @Test(enabled = false, groups = {"multiCluster"})
    public void add2TargetCluster() throws Exception {

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

            Util.submitAllClusters(b1, b2, b3);


            String feed = b1.getDataSets().get(0);
            feed = InstanceUtil.setFeedCluster(feed,
                    XmlUtil.createValidity("2009-02-01T00:00Z", "2012-01-01T00:00Z"),
                    XmlUtil.createRtention("hours(10)", ActionType.DELETE), null,
                    ClusterType.SOURCE, null);


            String startTime = InstanceUtil.getTimeWrtSystemTime(-50);


            String feedOriginalSubmit;
            String feedUpdated;

            //add one source and one target , schedule only on source
            feedOriginalSubmit = InstanceUtil
                    .setFeedCluster(feed, XmlUtil.createValidity(startTime,
                            InstanceUtil.addMinsToTime(startTime, 65)),
                            XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                            Util.readClusterName(b2.getClusters().get(0)), ClusterType.SOURCE,
                            null);

            Util.print("Feed: " + feedOriginalSubmit);

            ServiceResponse response =
                    prismHelper.getFeedHelper().submitEntity(URLS.SUBMIT_URL, feedOriginalSubmit);
            Thread.sleep(10000);
            Util.assertSucceeded(response);

            //schedule on source

            response = ua2.getFeedHelper().schedule(URLS.SCHEDULE_URL, feedOriginalSubmit);
            Thread.sleep(20000);
            Util.assertSucceeded(response);

            Assert.assertEquals(InstanceUtil
                    .checkIfFeedCoordExist(ua2.getFeedHelper(),
                            Util.readDatasetName(feedOriginalSubmit),
                            "REPLICATION"), 0);
            Assert.assertEquals(InstanceUtil
                    .checkIfFeedCoordExist(ua2.getFeedHelper(),
                            Util.readDatasetName(feedOriginalSubmit),
                            "RETENTION"), 1);
            Assert.assertEquals(InstanceUtil
                    .checkIfFeedCoordExist(ua3.getFeedHelper(),
                            Util.readDatasetName(feedOriginalSubmit),
                            "REPLICATION"), 0);
            Assert.assertEquals(InstanceUtil
                    .checkIfFeedCoordExist(ua3.getFeedHelper(),
                            Util.readDatasetName(feedOriginalSubmit), "RETENTION"),
                    0);
            Assert.assertEquals(InstanceUtil
                    .checkIfFeedCoordExist(ua1.getFeedHelper(),
                            Util.readDatasetName(feedOriginalSubmit),
                            "REPLICATION"), 0);
            Assert.assertEquals(InstanceUtil
                    .checkIfFeedCoordExist(ua1.getFeedHelper(),
                            Util.readDatasetName(feedOriginalSubmit), "RETENTION"),
                    0);

            //prepare updated Feed
            feedUpdated = InstanceUtil.setFeedCluster(feed, XmlUtil.createValidity(startTime,
                    InstanceUtil.addMinsToTime(startTime, 65)),
                    XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                    Util.readClusterName(b2.getClusters().get(0)), ClusterType.SOURCE,
                    null);
            feedUpdated = InstanceUtil.setFeedCluster(feedUpdated,
                    XmlUtil.createValidity(InstanceUtil.addMinsToTime(startTime, 20),
                            InstanceUtil.addMinsToTime(startTime, 85)),
                    XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                    Util.readClusterName(b1.getClusters().get(0)), ClusterType.TARGET, null);
            feedUpdated = InstanceUtil.setFeedCluster(feedUpdated,
                    XmlUtil.createValidity(InstanceUtil.addMinsToTime(startTime, 40),
                            InstanceUtil.addMinsToTime(startTime, 110)),
                    XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                    Util.readClusterName(b3.getClusters().get(0)), ClusterType.TARGET, null);

            Util.print("Updated Feed: " + feedUpdated);

            response = prismHelper.getFeedHelper().update(feedUpdated, feedUpdated);
            Thread.sleep(20000);
            Util.assertSucceeded(response);

            prismHelper.getFeedHelper()
                    .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, feedUpdated);

            Assert.assertEquals(InstanceUtil
                    .checkIfFeedCoordExist(ua2.getFeedHelper(), Util.readDatasetName(feedUpdated),
                            "REPLICATION"), 0);
            Assert.assertEquals(InstanceUtil
                    .checkIfFeedCoordExist(ua2.getFeedHelper(), Util.readDatasetName(feedUpdated),
                            "RETENTION"), 1);
            Assert.assertEquals(InstanceUtil
                    .checkIfFeedCoordExist(ua3.getFeedHelper(), Util.readDatasetName(feedUpdated),
                            "REPLICATION"), 1);
            Assert.assertEquals(InstanceUtil
                    .checkIfFeedCoordExist(ua3.getFeedHelper(), Util.readDatasetName(feedUpdated),
                            "RETENTION"), 1);
            Assert.assertEquals(InstanceUtil
                    .checkIfFeedCoordExist(ua1.getFeedHelper(), Util.readDatasetName(feedUpdated),
                            "REPLICATION"), 1);
            Assert.assertEquals(InstanceUtil
                    .checkIfFeedCoordExist(ua1.getFeedHelper(), Util.readDatasetName(feedUpdated),
                            "RETENTION"), 1);


        } finally {

            b1.deleteBundle(prismHelper);
            b2.deleteBundle(prismHelper);
            b3.deleteBundle(prismHelper);
        }

    }

    @Test(enabled = false, groups = {"multiCluster"})
    public void add1Source1TargetCluster() throws Exception {
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

            Util.submitAllClusters(b1, b2, b3);


            String feed = b1.getDataSets().get(0);
            feed = InstanceUtil.setFeedCluster(feed,
                    XmlUtil.createValidity("2009-02-01T00:00Z", "2012-01-01T00:00Z"),
                    XmlUtil.createRtention("hours(10)", ActionType.DELETE), null,
                    ClusterType.SOURCE, null);


            String startTime = InstanceUtil.getTimeWrtSystemTime(-50);


            String feedOriginalSubmit;
            String feedUpdated;

            //add one source and one target , schedule only on source
            feedOriginalSubmit = InstanceUtil
                    .setFeedCluster(feed, XmlUtil.createValidity(startTime,
                            InstanceUtil.addMinsToTime(startTime, 65)),
                            XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                            Util.readClusterName(b2.getClusters().get(0)), ClusterType.SOURCE,
                            null);

            Util.print("Feed: " + feedOriginalSubmit);

            ServiceResponse response =
                    prismHelper.getFeedHelper().submitEntity(URLS.SUBMIT_URL, feedOriginalSubmit);
            Thread.sleep(10000);
            Util.assertSucceeded(response);

            //schedule on source

            response = ua2.getFeedHelper().schedule(URLS.SCHEDULE_URL, feedOriginalSubmit);
            Thread.sleep(20000);
            Util.assertSucceeded(response);

            Assert.assertEquals(InstanceUtil
                    .checkIfFeedCoordExist(ua2.getFeedHelper(),
                            Util.readDatasetName(feedOriginalSubmit),
                            "REPLICATION"), 0);
            Assert.assertEquals(InstanceUtil
                    .checkIfFeedCoordExist(ua2.getFeedHelper(),
                            Util.readDatasetName(feedOriginalSubmit), "RETENTION"),
                    1);
            Assert.assertEquals(InstanceUtil
                    .checkIfFeedCoordExist(ua3.getFeedHelper(),
                            Util.readDatasetName(feedOriginalSubmit),
                            "REPLICATION"), 0);
            Assert.assertEquals(InstanceUtil
                    .checkIfFeedCoordExist(ua3.getFeedHelper(),
                            Util.readDatasetName(feedOriginalSubmit), "RETENTION"),
                    0);
            Assert.assertEquals(InstanceUtil
                    .checkIfFeedCoordExist(ua1.getFeedHelper(),
                            Util.readDatasetName(feedOriginalSubmit),
                            "REPLICATION"), 0);
            Assert.assertEquals(InstanceUtil
                    .checkIfFeedCoordExist(ua1.getFeedHelper(),
                            Util.readDatasetName(feedOriginalSubmit),
                            "RETENTION"), 0);

            //prepare updated Feed
            feedUpdated = InstanceUtil
                    .setFeedCluster(feed, XmlUtil.createValidity(startTime,
                            InstanceUtil.addMinsToTime(startTime, 65)),
                            XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                            Util.readClusterName(b2.getClusters().get(0)), ClusterType.SOURCE,
                            "US/${cluster.colo}");
            feedUpdated = InstanceUtil.setFeedCluster(feedUpdated,
                    XmlUtil.createValidity(InstanceUtil.addMinsToTime(startTime, 20),
                            InstanceUtil.addMinsToTime(startTime, 85)),
                    XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                    Util.readClusterName(b1.getClusters().get(0)), ClusterType.TARGET, null);
            feedUpdated = InstanceUtil.setFeedCluster(feedUpdated,
                    XmlUtil.createValidity(InstanceUtil.addMinsToTime(startTime, 40),
                            InstanceUtil.addMinsToTime(startTime, 110)),
                    XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                    Util.readClusterName(b3.getClusters().get(0)), ClusterType.SOURCE,
                    "UK/${cluster.colo}");

            Util.print("Updated Feed: " + feedUpdated);

            response = prismHelper.getFeedHelper().update(feedUpdated, feedUpdated);
            Thread.sleep(20000);
            Util.assertSucceeded(response);

            prismHelper.getFeedHelper()
                    .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, feedUpdated);

            Assert.assertEquals(InstanceUtil
                    .checkIfFeedCoordExist(ua2.getFeedHelper(), Util.readDatasetName(feedUpdated),
                            "REPLICATION"), 0);
            Assert.assertEquals(InstanceUtil
                    .checkIfFeedCoordExist(ua2.getFeedHelper(), Util.readDatasetName(feedUpdated),
                            "RETENTION"), 2);
            Assert.assertEquals(InstanceUtil
                    .checkIfFeedCoordExist(ua3.getFeedHelper(), Util.readDatasetName(feedUpdated),
                            "REPLICATION"), 0);
            Assert.assertEquals(InstanceUtil
                    .checkIfFeedCoordExist(ua3.getFeedHelper(), Util.readDatasetName(feedUpdated),
                            "RETENTION"), 1);
            Assert.assertEquals(InstanceUtil
                    .checkIfFeedCoordExist(ua1.getFeedHelper(), Util.readDatasetName(feedUpdated),
                            "REPLICATION"), 2);
            Assert.assertEquals(InstanceUtil
                    .checkIfFeedCoordExist(ua1.getFeedHelper(), Util.readDatasetName(feedUpdated),
                            "RETENTION"), 1);


        } finally {

            b1.deleteBundle(prismHelper);
            b2.deleteBundle(prismHelper);
            b3.deleteBundle(prismHelper);
        }

    }

    @Test(enabled = false, groups = {"multiCluster"})
    public void deleteSourceCluster() throws Exception {
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

            Util.submitAllClusters(b1, b2, b3);


            String feed = b1.getDataSets().get(0);
            feed = InstanceUtil.setFeedCluster(feed,
                    XmlUtil.createValidity("2009-02-01T00:00Z", "2012-01-01T00:00Z"),
                    XmlUtil.createRtention("hours(10)", ActionType.DELETE), null,
                    ClusterType.SOURCE, null);

            String startTime = InstanceUtil.getTimeWrtSystemTime(-50);

            String feedOriginalSubmit;
            String feedUpdated;

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
                    Util.readClusterName(b1.getClusters().get(0)), ClusterType.TARGET, null);
            feedOriginalSubmit = InstanceUtil.setFeedCluster(feedOriginalSubmit,
                    XmlUtil.createValidity(InstanceUtil.addMinsToTime(startTime, 40),
                            InstanceUtil.addMinsToTime(startTime, 110)),
                    XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                    Util.readClusterName(b3.getClusters().get(0)), ClusterType.SOURCE,
                    "UK/${cluster.colo}");

            Util.print("Feed: " + feedOriginalSubmit);

            ServiceResponse response =
                    prismHelper.getFeedHelper().submitEntity(URLS.SUBMIT_URL, feedOriginalSubmit);
            Thread.sleep(10000);
            Util.assertSucceeded(response);

            //schedule on source

            response = prismHelper.getFeedHelper().schedule(URLS.SCHEDULE_URL, feedOriginalSubmit);
            Thread.sleep(20000);
            Util.assertSucceeded(response);

            Assert.assertEquals(InstanceUtil
                    .checkIfFeedCoordExist(ua2.getFeedHelper(),
                            Util.readDatasetName(feedOriginalSubmit),
                            "REPLICATION"), 0);
            Assert.assertEquals(InstanceUtil
                    .checkIfFeedCoordExist(ua2.getFeedHelper(),
                            Util.readDatasetName(feedOriginalSubmit), "RETENTION" +
                            ""), 1);
            Assert.assertEquals(InstanceUtil
                    .checkIfFeedCoordExist(ua3.getFeedHelper(),
                            Util.readDatasetName(feedOriginalSubmit),
                            "REPLICATION"), 0);
            Assert.assertEquals(InstanceUtil
                    .checkIfFeedCoordExist(ua3.getFeedHelper(),
                            Util.readDatasetName(feedOriginalSubmit),
                            "RETENTION"), 1);
            Assert.assertEquals(InstanceUtil
                    .checkIfFeedCoordExist(ua1.getFeedHelper(),
                            Util.readDatasetName(feedOriginalSubmit),
                            "REPLICATION"), 2);
            Assert.assertEquals(InstanceUtil
                    .checkIfFeedCoordExist(ua1.getFeedHelper(),
                            Util.readDatasetName(feedOriginalSubmit),
                            "RETENTION"), 1);

            //prepare updated Feed
            feedUpdated = InstanceUtil
                    .setFeedCluster(feed, XmlUtil.createValidity(startTime,
                            InstanceUtil.addMinsToTime(startTime, 65)),
                            XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                            Util.readClusterName(b2.getClusters().get(0)), ClusterType.SOURCE,
                            null);
            feedUpdated = InstanceUtil.setFeedCluster(feedUpdated,
                    XmlUtil.createValidity(InstanceUtil.addMinsToTime(startTime, 20),
                            InstanceUtil.addMinsToTime(startTime, 85)),
                    XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                    Util.readClusterName(b1.getClusters().get(0)), ClusterType.TARGET, null);

            response = prismHelper.getFeedHelper().update(feedUpdated, feedUpdated);
            Thread.sleep(20000);
            Util.assertSucceeded(response);

            response = ua3.getFeedHelper().getEntityDefinition(URLS.GET_ENTITY_DEFINITION, feedUpdated);
            Util.assertFailed(response);


            prismHelper.getFeedHelper()
                    .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, feedUpdated);
            Assert.assertEquals(InstanceUtil
                    .checkIfFeedCoordExist(ua2.getFeedHelper(), Util.readDatasetName(feedUpdated),
                            "REPLICATION"), 0);
            Assert.assertEquals(InstanceUtil
                    .checkIfFeedCoordExist(ua2.getFeedHelper(), Util.readDatasetName(feedUpdated),
                            "RETENTION"), 2);
            Assert.assertEquals(InstanceUtil
                    .checkIfFeedCoordExist(ua3.getFeedHelper(), Util.readDatasetName(feedUpdated),
                            "REPLICATION"), 0);
            Assert.assertEquals(InstanceUtil
                    .checkIfFeedCoordExist(ua3.getFeedHelper(), Util.readDatasetName(feedUpdated),
                            "RETENTION"), 1);
            Assert.assertEquals(InstanceUtil
                    .checkIfFeedCoordExist(ua1.getFeedHelper(), Util.readDatasetName(feedUpdated),
                            "REPLICATION"), 0);
            Assert.assertEquals(InstanceUtil
                    .checkIfFeedCoordExist(ua1.getFeedHelper(), Util.readDatasetName(feedUpdated),
                            "RETENTION"), 0);

        } finally {

            b1.deleteBundle(prismHelper);
            b2.deleteBundle(prismHelper);
            b3.deleteBundle(prismHelper);
        }
    }

    @Test(enabled = true, groups = {"multiCluster"})
    public void deleteTargetCluster() throws Exception {
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

            Util.submitAllClusters(b1, b2, b3);


            String feed = b1.getDataSets().get(0);
            feed = InstanceUtil.setFeedCluster(feed,
                    XmlUtil.createValidity("2009-02-01T00:00Z", "2012-01-01T00:00Z"),
                    XmlUtil.createRtention("hours(10)", ActionType.DELETE), null,
                    ClusterType.SOURCE, null);

            String startTime = InstanceUtil.getTimeWrtSystemTime(-50);

            String feedOriginalSubmit;
            String feedUpdated;

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
                    Util.readClusterName(b1.getClusters().get(0)), ClusterType.TARGET, null);
            feedOriginalSubmit = InstanceUtil.setFeedCluster(feedOriginalSubmit,
                    XmlUtil.createValidity(InstanceUtil.addMinsToTime(startTime, 40),
                            InstanceUtil.addMinsToTime(startTime, 110)),
                    XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                    Util.readClusterName(b3.getClusters().get(0)), ClusterType.SOURCE,
                    "UK/${cluster.colo}");

            Util.print("Feed: " + feedOriginalSubmit);

            ServiceResponse response =
                    prismHelper.getFeedHelper().submitEntity(URLS.SUBMIT_URL, feedOriginalSubmit);
            Thread.sleep(10000);
            Util.assertSucceeded(response);

            //schedule on source

            response = prismHelper.getFeedHelper().schedule(URLS.SCHEDULE_URL, feedOriginalSubmit);
            Thread.sleep(20000);
            Util.assertSucceeded(response);

            Assert.assertEquals(InstanceUtil
                    .checkIfFeedCoordExist(ua2.getFeedHelper(),
                            Util.readDatasetName(feedOriginalSubmit),
                            "REPLICATION"), 0);
            Assert.assertEquals(InstanceUtil
                    .checkIfFeedCoordExist(ua2.getFeedHelper(),
                            Util.readDatasetName(feedOriginalSubmit), "RETENTION"),
                    1);
            Assert.assertEquals(InstanceUtil
                    .checkIfFeedCoordExist(ua3.getFeedHelper(),
                            Util.readDatasetName(feedOriginalSubmit),
                            "REPLICATION"), 0);
            Assert.assertEquals(InstanceUtil
                    .checkIfFeedCoordExist(ua3.getFeedHelper(),
                            Util.readDatasetName(feedOriginalSubmit), "RETENTION"),
                    1);
            Assert.assertEquals(InstanceUtil
                    .checkIfFeedCoordExist(ua1.getFeedHelper(),
                            Util.readDatasetName(feedOriginalSubmit),
                            "REPLICATION"), 2);
            Assert.assertEquals(InstanceUtil
                    .checkIfFeedCoordExist(ua1.getFeedHelper(),
                            Util.readDatasetName(feedOriginalSubmit), "RETENTION"),
                    1);

            //prepare updated Feed
            feedUpdated = InstanceUtil
                    .setFeedCluster(feed, XmlUtil.createValidity(startTime,
                            InstanceUtil.addMinsToTime(startTime, 65)),
                            XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                            Util.readClusterName(b2.getClusters().get(0)), ClusterType.SOURCE,
                            null);
            feedUpdated = InstanceUtil.setFeedCluster(feedUpdated,
                    XmlUtil.createValidity(InstanceUtil.addMinsToTime(startTime, 40),
                            InstanceUtil.addMinsToTime(startTime, 110)),
                    XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                    Util.readClusterName(b3.getClusters().get(0)), ClusterType.SOURCE, null);

            Util.print("Feed: " + feedUpdated);

            response = prismHelper.getFeedHelper().update(feedUpdated, feedUpdated);
            Thread.sleep(20000);
            Util.assertSucceeded(response);

            response = ua1.getFeedHelper().getEntityDefinition(URLS.GET_ENTITY_DEFINITION, feedUpdated);
            Util.assertFailed(response);


            prismHelper.getFeedHelper()
                    .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, feedUpdated);
            Assert.assertEquals(InstanceUtil
                    .checkIfFeedCoordExist(ua2.getFeedHelper(), Util.readDatasetName(feedUpdated),
                            "REPLICATION"), 0);
            Assert.assertEquals(InstanceUtil
                    .checkIfFeedCoordExist(ua2.getFeedHelper(), Util.readDatasetName(feedUpdated),
                            "RETENTION"), 2);
            Assert.assertEquals(InstanceUtil
                    .checkIfFeedCoordExist(ua3.getFeedHelper(), Util.readDatasetName(feedUpdated),
                            "REPLICATION"), 0);
            Assert.assertEquals(InstanceUtil
                    .checkIfFeedCoordExist(ua3.getFeedHelper(), Util.readDatasetName(feedUpdated),
                            "RETENTION"), 2);
            Assert.assertEquals(InstanceUtil
                    .checkIfFeedCoordExist(ua1.getFeedHelper(), Util.readDatasetName(feedUpdated),
                            "REPLICATION"), 0);
            Assert.assertEquals(InstanceUtil
                    .checkIfFeedCoordExist(ua1.getFeedHelper(), Util.readDatasetName(feedUpdated),
                            "RETENTION"), 0);

        } finally {

            b1.deleteBundle(prismHelper);
            b2.deleteBundle(prismHelper);
            b3.deleteBundle(prismHelper);
        }
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
