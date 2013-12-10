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
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.core.util.Util.URLS;
import org.apache.falcon.regression.core.util.XmlUtil;
import org.apache.oozie.client.CoordinatorAction.Status;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;

public class PrismFeedReplicationUpdateTest {
    @BeforeMethod(alwaysRun = true)
    public void testName(Method method) throws Exception {
        Util.print("test name: " + method.getName());
        //restart server as precaution
        //	Util.restartService(ua1.getClusterHelper());
        //	Util.restartService(ua2.getClusterHelper());
        //	Util.restartService(ua3.getClusterHelper());


    }

    public PrismFeedReplicationUpdateTest() throws Exception {

    }

    PrismHelper prismHelper = new PrismHelper("prism.properties");

    ColoHelper ua1 = new ColoHelper("mk-qa.config.properties");

    ColoHelper ua2 = new ColoHelper("ivoryqa-1.config.properties");

    ColoHelper ua3 = new ColoHelper("gs1001.config.properties");

    @SuppressWarnings("deprecation")
    @Test(enabled = true, timeOut = 1200000)
    public void multipleSourceOneTarget() throws Exception {

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
                            "{HOUR}/${MINUTE}");

            Bundle.submitCluster(b1, b2, b3);

            String feed = b1.getDataSets().get(0);
            feed = InstanceUtil.setFeedCluster(feed,
                    XmlUtil.createValidity("2009-02-01T00:00Z", "2012-01-01T00:00Z"),
                    XmlUtil.createRtention("hours(10)", ActionType.DELETE), null,
                    ClusterType.SOURCE, null);

            String postFix = "/US/ua2";
            String prefix = b1.getFeedDataPathPrefix();
            Util.HDFSCleanup(ua2, prefix.substring(1));
            Util.lateDataReplenish(ua2, 80, 1, prefix, postFix);


            postFix = "/UK/ua3";
            prefix = b1.getFeedDataPathPrefix();
            Util.HDFSCleanup(ua3, prefix.substring(1));
            Util.lateDataReplenish(ua3, 80, 1, prefix, postFix);

            String startTime = InstanceUtil.getTimeWrtSystemTime(-30);


            feed = InstanceUtil
                    .setFeedCluster(feed, XmlUtil.createValidity(startTime,
                            InstanceUtil.addMinsToTime(startTime, 85)),
                            XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                            Util.readClusterName(b2.getClusters().get(0)), ClusterType.SOURCE,
                            "US/${cluster.colo}");
            feed = InstanceUtil.setFeedCluster(feed,
                    XmlUtil.createValidity(InstanceUtil.addMinsToTime(startTime, 20),
                            InstanceUtil.addMinsToTime(startTime, 105)),
                    XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                    Util.readClusterName(b1.getClusters().get(0)), ClusterType.TARGET, null);
            feed = InstanceUtil.setFeedCluster(feed,
                    XmlUtil.createValidity(InstanceUtil.addMinsToTime(startTime, 40),
                            InstanceUtil.addMinsToTime(startTime, 130)),
                    XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                    Util.readClusterName(b3.getClusters().get(0)), ClusterType.SOURCE,
                    "UK/${cluster.colo}");


            Util.print("feed: " + feed);

            ServiceResponse r = prismHelper.getFeedHelper().submitEntity(URLS.SUBMIT_URL, feed);
            Thread.sleep(10000);

            r = prismHelper.getFeedHelper().schedule(URLS.SCHEDULE_URL, feed);
            Thread.sleep(30000);

            //change feed location path
            feed = InstanceUtil.setFeedFilePath(feed,
                    "/newFeedPath/input-data/rawLogs/oozieExample/${YEAR}/${MONTH}/${DAY}/${HOUR" +
                            "}/${MINUTE}");

            Util.print("updated feed: " + feed);


            //update feed
            prismHelper.getFeedHelper().update(feed, feed);
            Thread.sleep(30000);


            Assert.assertEquals(
                    InstanceUtil
                            .checkIfFeedCoordExist(ua2.getFeedHelper(), Util.readDatasetName(feed),
                                    "REPLICATION" +
                                            ""), 0);
            Assert.assertEquals(
                    InstanceUtil
                            .checkIfFeedCoordExist(ua2.getFeedHelper(), Util.readDatasetName(feed),
                                    "RETENTION"),
                    2);
            Assert.assertEquals(
                    InstanceUtil
                            .checkIfFeedCoordExist(ua3.getFeedHelper(), Util.readDatasetName(feed),
                                    "REPLICATION"),
                    0);
            Assert.assertEquals(
                    InstanceUtil
                            .checkIfFeedCoordExist(ua3.getFeedHelper(), Util.readDatasetName(feed),
                                    "RETENTION"),
                    2);
            Assert.assertEquals(
                    InstanceUtil
                            .checkIfFeedCoordExist(ua1.getFeedHelper(), Util.readDatasetName(feed),
                                    "REPLICATION"),
                    4);
            Assert.assertEquals(
                    InstanceUtil
                            .checkIfFeedCoordExist(ua1.getFeedHelper(), Util.readDatasetName(feed),
                                    "RETENTION"),
                    2);

            //instanceUtil.getInputFoldersForInstanceForReplication(ua1,
            // instanceUtil.getReplicationCoordID
            // (instanceUtil.getLatestBundleID(ua1, Util.readDatasetName(feed), "FEED"),
            // ua1.getFeedHelper()).get(0),
            // 0);

        } finally {

            b1.deleteBundle(prismHelper);
            b2.deleteBundle(prismHelper);
            b3.deleteBundle(prismHelper);
        }
    }

    @Test(enabled = true, timeOut = 1200000)
    public void updateFeed_dependentProcessTest() throws Exception {
        //ua1 and ua3 are source. feed01 on ua1 target ua3, feed02 on ua3 target ua1

        //get 3 unique bundles
        Bundle b1 = (Bundle) Util.readELBundles()[0][0];
        b1.generateUniqueBundle();
        Bundle b2 = (Bundle) Util.readELBundles()[0][0];
        b2.generateUniqueBundle();
        Bundle b3 = (Bundle) Util.readELBundles()[0][0];
        b3.generateUniqueBundle();

        try {
            //generate bundles according to config files
            b1 = new Bundle(b1, ua1.getEnvFileName());
            b2 = new Bundle(b2, ua2.getEnvFileName());
            b3 = new Bundle(b3, ua3.getEnvFileName());

            //set cluster colos
            b1.setCLusterColo("ua1");
            Util.print("cluster b1: " + b1.getClusters().get(0));
            b2.setCLusterColo("ua2");
            Util.print("cluster b2: " + b2.getClusters().get(0));
            b3.setCLusterColo("ua3");
            Util.print("cluster b3: " + b3.getClusters().get(0));


            //submit 3 clusters
            Bundle.submitCluster(b1, b2, b3);

            //get 2 unique feeds
            String feed01 = Util.getInputFeedFromBundle(b1);
            String feed02 = Util.getInputFeedFromBundle(b2);
            String outputFeed = Util.getOutputFeedFromBundle(b1);

            //set source and target for the 2 feeds

            //set clusters to null;
            feed01 = InstanceUtil
                    .setFeedCluster(feed01,
                            XmlUtil.createValidity("2009-02-01T00:00Z", "2012-01-01T00:00Z"),
                            XmlUtil.createRtention("hours(10)", ActionType.DELETE), null,
                            ClusterType.SOURCE, null);
            feed02 = InstanceUtil
                    .setFeedCluster(feed02,
                            XmlUtil.createValidity("2009-02-01T00:00Z", "2012-01-01T00:00Z"),
                            XmlUtil.createRtention("hours(10)", ActionType.DELETE), null,
                            ClusterType.SOURCE, null);
            outputFeed = InstanceUtil
                    .setFeedCluster(outputFeed,
                            XmlUtil.createValidity("2009-02-01T00:00Z", "2012-01-01T00:00Z"),
                            XmlUtil.createRtention("hours(10)", ActionType.DELETE), null,
                            ClusterType.SOURCE, null);


            //set new feed input data
            feed01 = Util.setFeedPathValue(feed01,
                    "/samarthRetention/feed01/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}/");
            feed02 = Util.setFeedPathValue(feed02,
                    "/samarthRetention/feed02/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}/");


            //generate data in both the colos ua1 and ua3
            String prefix = InstanceUtil.getFeedPrefix(feed01);
            Util.HDFSCleanup(ua1, prefix.substring(1));
            Util.lateDataReplenish(ua1, 70, 1, prefix);


            prefix = InstanceUtil.getFeedPrefix(feed02);
            Util.HDFSCleanup(ua3, prefix.substring(1));
            Util.lateDataReplenish(ua3, 70, 1, prefix);


            String startTime = InstanceUtil.getTimeWrtSystemTime(-50);

            //set clusters for feed01
            feed01 = InstanceUtil
                    .setFeedCluster(feed01, XmlUtil.createValidity(startTime, "2099-01-01T00:00Z"),
                            XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                            Util.readClusterName(b1.getClusters().get(0)), ClusterType.SOURCE,
                            null);
            feed01 = InstanceUtil
                    .setFeedCluster(feed01, XmlUtil.createValidity(startTime, "2099-01-01T00:00Z"),
                            XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                            Util.readClusterName(b3.getClusters().get(0)), ClusterType.TARGET,
                            null);


            //set clusters for feed02
            feed02 = InstanceUtil
                    .setFeedCluster(feed02, XmlUtil.createValidity(startTime, "2099-01-01T00:00Z"),
                            XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                            Util.readClusterName(b1.getClusters().get(0)), ClusterType.TARGET,
                            null);
            feed02 = InstanceUtil
                    .setFeedCluster(feed02, XmlUtil.createValidity(startTime, "2099-01-01T00:00Z"),
                            XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                            Util.readClusterName(b3.getClusters().get(0)), ClusterType.SOURCE,
                            null);

            //set clusters for output feed
            outputFeed = InstanceUtil.setFeedCluster(outputFeed,
                    XmlUtil.createValidity(startTime, "2099-01-01T00:00Z"),
                    XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                    Util.readClusterName(b1.getClusters().get(0)), ClusterType.SOURCE, null);
            outputFeed = InstanceUtil.setFeedCluster(outputFeed,
                    XmlUtil.createValidity(startTime, "2099-01-01T00:00Z"),
                    XmlUtil.createRtention("hours(10)", ActionType.DELETE),
                    Util.readClusterName(b3.getClusters().get(0)), ClusterType.TARGET, null);


            //submit and schedule feeds
            Util.print("feed01: " + feed01);
            Util.print("feed02: " + feed02);
            Util.print("outputFeed: " + outputFeed);

            ServiceResponse r = prismHelper.getFeedHelper()
                    .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, feed01);
            r = prismHelper.getFeedHelper().submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, feed02);
            r = prismHelper.getFeedHelper().submitAndSchedule(URLS.SUBMIT_URL, outputFeed);


            //create a process with 2 clusters

            //get a process
            String process = b1.getProcessData();

            //add clusters to process

            String processStartTime = InstanceUtil.getTimeWrtSystemTime(-11);
            String processEndTime = InstanceUtil.getTimeWrtSystemTime(70);


            process = InstanceUtil
                    .setProcessCluster(process, null,
                            XmlUtil.createProcessValidity(startTime, "2099-01-01T00:00Z"));
            process = InstanceUtil
                    .setProcessCluster(process, Util.readClusterName(b1.getClusters().get(0)),
                            XmlUtil.createProcessValidity(processStartTime, processEndTime));
            process = InstanceUtil
                    .setProcessCluster(process, Util.readClusterName(b3.getClusters().get(0)),
                            XmlUtil.createProcessValidity(processStartTime, processEndTime));
            process = InstanceUtil
                    .addProcessInputFeed(process, Util.readDatasetName(feed02),
                            Util.readDatasetName(feed02));


            //submit and schedule process
            Util.print("process: " + process);

            r = prismHelper.getProcessHelper()
                    .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, process);

            Util.print("Wait till process goes into running ");

            for (int i = 0; i < 30; i++) {
                Status sUa1 =
                        InstanceUtil.getInstanceStatus(ua1, Util.getProcessName(process), 0, 0);
                Status sUa2 =
                        InstanceUtil.getInstanceStatus(ua3, Util.getProcessName(process), 0, 0);
                if ((sUa1.toString().equals("RUNNING") || (sUa1.toString().equals("SUCCEEDED "))) &&
                        (sUa2.toString().equals("RUNNING") || sUa2.toString().equals("SUCCEEDED")))
                    break;
                Thread.sleep(20000);

            }

            //update feed01
            //change feed location path
            feed01 = InstanceUtil.setFeedFilePath(feed01,
                    "/newFeedPath/input-data/rawLogs/oozieExample/${YEAR}/${MONTH}/${DAY}/${HOUR" +
                            "}/${MINUTE}");

            Util.print("updated feed: " + feed01);


            //	Util.shutDownService(ua3.getClusterHelper());


            //update feed first time
            prismHelper.getFeedHelper().update(feed01, feed01);

            //status of feed instance
            //	ProcessInstancesResult responseInstance  =  prismHelper.getFeedHelper().getProcessInstanceStatus(Util
            // .readDatasetName(feed01),"?start="+startTime+"&end="+instanceUtil.addMinsToTime(startTime,200));

            //status of process instance

            //	responseInstance  =  prismHelper.getProcessHelper().getProcessInstanceStatus(Util.readEntityName(b1
            // .getProcessData()),"?start="+processStartTime+"&end="+processEndTime);


            //	Util.startService(ua3.getClusterHelper());

            //re update for the second time : check update roll forward
            prismHelper.getFeedHelper().update(feed01, feed01);


            //status of feed instance
            //	responseInstance  =  prismHelper.getFeedHelper().getProcessInstanceStatus(Util.readDatasetName(feed01)
            // ,"?start="+startTime+"&end="+instanceUtil.addMinsToTime(startTime,200));

            //status of process instance

            //	responseInstance  =  prismHelper.getProcessHelper().getProcessInstanceStatus(Util.readEntityName(b1
            // .getProcessData()),"?start="+processStartTime+"&end="+processEndTime);


        } finally {
            b1.deleteBundle(prismHelper);
            b2.deleteBundle(prismHelper);
            b3.deleteBundle(prismHelper);

        }
    }
}
