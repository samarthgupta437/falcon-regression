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
import org.apache.falcon.entity.v0.feed.ActionType;
import org.apache.falcon.entity.v0.feed.ClusterType;
import org.apache.falcon.regression.core.enumsAndConstants.ENTITY_TYPE;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.response.ProcessInstancesResult;
import org.apache.falcon.regression.core.response.ServiceResponse;
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
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;

/**
 * Process instance mixed colo tests.
 */
@Test(groups = "embedded")
public class ProcessInstanceColoMixedTest extends BaseTestClass {

    private final String baseTestHDFSDir = baseHDFSDir + "/ProcessInstanceColoMixedTest";
    private final String datePattern = "/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}/";
    private final String feedPath = baseTestHDFSDir + "/feed0%d" + datePattern;
    private String aggregateWorkflowDir = baseTestHDFSDir + "/aggregator";
    ColoHelper cluster1 = servers.get(0);
    ColoHelper cluster2 = servers.get(1);
    FileSystem cluster1FS = serverFS.get(0);
    FileSystem cluster2FS = serverFS.get(1);
    private static final Logger logger = Logger.getLogger(ProcessInstanceColoMixedTest.class);

    @BeforeClass(alwaysRun = true)
    public void prepareClusters() throws Exception {
        logger.info("in @BeforeClass");
        HadoopUtil.uploadDir(cluster1FS, aggregateWorkflowDir, OSUtil.RESOURCES_OOZIE);
        HadoopUtil.uploadDir(cluster2FS, aggregateWorkflowDir, OSUtil.RESOURCES_OOZIE);
    }

    @BeforeMethod(alwaysRun = true)
    public void setup(Method method) throws Exception {
        logger.info("test name: " + method.getName());

        //get 3 unique bundles
        bundles[0] = BundleUtil.readELBundles()[0][0];
        bundles[0].generateUniqueBundle();
        bundles[1] = BundleUtil.readELBundles()[0][0];
        bundles[1].generateUniqueBundle();

        //generate bundles according to config files
        bundles[0] = new Bundle(bundles[0], cluster1);
        bundles[1] = new Bundle(bundles[1], cluster2);

        //set cluster colos
        bundles[0].setCLusterColo(bundles[0].getClusterHelper().getColoName());
        logger.info("cluster b1: " + Util.prettyPrintXml(bundles[0].getClusters().get(0)));
        bundles[1].setCLusterColo(bundles[1].getClusterHelper().getColoName());
        logger.info("cluster b2: " + Util.prettyPrintXml(bundles[1].getClusters().get(0)));

        bundles[0].setProcessWorkflow(aggregateWorkflowDir);
        bundles[1].setProcessWorkflow(aggregateWorkflowDir);
        //submit 2 clusters
        Bundle.submitCluster(bundles[0], bundles[1]);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown(Method method) {
        logger.info("tearDown " + method.getName());
        removeBundles();
    }

    @Test(timeOut = 12000000)
    public void mixed01_C1sC2sC1eC2e() throws Exception {
        //ua1 and ua3 are source. ua2 target.   feed01 on ua1 , feed02 on ua3
        //get 2 unique feeds
        String feed01 = BundleUtil.getInputFeedFromBundle(bundles[0]);
        String feed02 = BundleUtil.getInputFeedFromBundle(bundles[1]);
        String outputFeed = BundleUtil.getOutputFeedFromBundle(bundles[0]);
        //set source and target for the 2 feeds

        //set clusters to null;
        feed01 = InstanceUtil
            .setFeedCluster(feed01,
                XmlUtil.createValidity("2009-02-01T00:00Z", "2012-01-01T00:00Z"),
                XmlUtil.createRtention("days(10000)", ActionType.DELETE), null,
                ClusterType.SOURCE, null);
        feed02 = InstanceUtil
            .setFeedCluster(feed02,
                XmlUtil.createValidity("2009-02-01T00:00Z", "2012-01-01T00:00Z"),
                XmlUtil.createRtention("days(10000)", ActionType.DELETE), null,
                ClusterType.SOURCE, null);
        outputFeed = InstanceUtil
            .setFeedCluster(outputFeed,
                XmlUtil.createValidity("2009-02-01T00:00Z", "2012-01-01T00:00Z"),
                XmlUtil.createRtention("days(10000)", ActionType.DELETE), null,
                ClusterType.SOURCE, null);


        //set new feed input data
        feed01 = Util.setFeedPathValue(feed01, String.format(feedPath, 1));
        feed02 = Util.setFeedPathValue(feed02, String.format(feedPath, 2));

        //generate data in both the colos ua1 and ua3
        String prefix = InstanceUtil.getFeedPrefix(feed01);
        HadoopUtil.deleteDirIfExists(prefix.substring(1), cluster1FS);
        TimeUtil.createDataWithinDatesAndPrefix(cluster1,
            TimeUtil.oozieDateToDate(TimeUtil.getTimeWrtSystemTime(-100)),
            TimeUtil.oozieDateToDate(TimeUtil.getTimeWrtSystemTime(100)), prefix,
            1);

        prefix = InstanceUtil.getFeedPrefix(feed02);
        HadoopUtil.deleteDirIfExists(prefix.substring(1), cluster2FS);
        TimeUtil.createDataWithinDatesAndPrefix(cluster2,
            TimeUtil.oozieDateToDate(TimeUtil.getTimeWrtSystemTime(-100)),
            TimeUtil.oozieDateToDate(TimeUtil.getTimeWrtSystemTime(100)), prefix,
            1);

        String startTime = TimeUtil.getTimeWrtSystemTime(-70);

        //set clusters for feed01
        feed01 = InstanceUtil
            .setFeedCluster(feed01, XmlUtil.createValidity(startTime, "2099-01-01T00:00Z"),
                XmlUtil.createRtention("days(10000)", ActionType.DELETE),
                Util.readClusterName(bundles[0].getClusters().get(0)), ClusterType.SOURCE,
                null);
        feed01 = InstanceUtil
            .setFeedCluster(feed01, XmlUtil.createValidity(startTime, "2099-01-01T00:00Z"),
                XmlUtil.createRtention("days(10000)", ActionType.DELETE),
                Util.readClusterName(bundles[1].getClusters().get(0)), ClusterType.TARGET,
                null);

        //set clusters for feed02
        feed02 = InstanceUtil
            .setFeedCluster(feed02, XmlUtil.createValidity(startTime, "2099-01-01T00:00Z"),
                XmlUtil.createRtention("days(10000)", ActionType.DELETE),
                Util.readClusterName(bundles[0].getClusters().get(0)), ClusterType.TARGET,
                null);
        feed02 = InstanceUtil
            .setFeedCluster(feed02, XmlUtil.createValidity(startTime, "2099-01-01T00:00Z"),
                XmlUtil.createRtention("days(10000)", ActionType.DELETE),
                Util.readClusterName(bundles[1].getClusters().get(0)), ClusterType.SOURCE,
                null);

        //set clusters for output feed
        outputFeed = InstanceUtil.setFeedCluster(outputFeed,
            XmlUtil.createValidity(startTime, "2099-01-01T00:00Z"),
            XmlUtil.createRtention("days(10000)", ActionType.DELETE),
            Util.readClusterName(bundles[0].getClusters().get(0)), ClusterType.SOURCE, null);
        outputFeed = InstanceUtil.setFeedCluster(outputFeed,
            XmlUtil.createValidity(startTime, "2099-01-01T00:00Z"),
            XmlUtil.createRtention("days(10000)", ActionType.DELETE),
            Util.readClusterName(bundles[1].getClusters().get(0)), ClusterType.TARGET, null);

        //submit and schedule feeds
        logger.info("feed01: " + Util.prettyPrintXml(feed01));
        logger.info("feed02: " + Util.prettyPrintXml(feed02));
        logger.info("outputFeed: " + Util.prettyPrintXml(outputFeed));

        ServiceResponse r = prism.getFeedHelper()
            .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, feed01);
        AssertUtil.assertSucceeded(r);
        r = prism.getFeedHelper().submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, feed02);
        AssertUtil.assertSucceeded(r);
        r = prism.getFeedHelper()
            .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, outputFeed);
        AssertUtil.assertSucceeded(r);

        //create a process with 2 clusters

        //get a process
        String process = bundles[0].getProcessData();

        //add clusters to process

        String processStartTime = TimeUtil.getTimeWrtSystemTime(-16);
        // String processEndTime = InstanceUtil.getTimeWrtSystemTime(20);

        process = InstanceUtil
            .setProcessCluster(process, null,
                XmlUtil.createProcessValidity(startTime, "2099-01-01T00:00Z"));
        process = InstanceUtil
            .setProcessCluster(process, Util.readClusterName(bundles[0].getClusters().get(0)),
                XmlUtil.createProcessValidity(processStartTime,
                    TimeUtil.addMinsToTime(processStartTime, 35)));
        process = InstanceUtil
            .setProcessCluster(process, Util.readClusterName(bundles[1].getClusters().get(0)),
                XmlUtil.createProcessValidity(
                    TimeUtil.addMinsToTime(processStartTime, 16),
                    TimeUtil.addMinsToTime(processStartTime, 45)));
        process = InstanceUtil
            .addProcessInputFeed(process, Util.readDatasetName(feed02),
                Util.readDatasetName(feed02));

        //submit and schedule process
        logger.info("process: " + Util.prettyPrintXml(process));

        prism.getProcessHelper()
            .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, process);

        logger.info("Wait till process goes into running ");

        InstanceUtil.waitTillInstanceReachState(serverOC.get(0), Util.getProcessName(process), 1,
            Status.RUNNING, ENTITY_TYPE.PROCESS);
        InstanceUtil.waitTillInstanceReachState(serverOC.get(1), Util.getProcessName(process), 1,
            Status.RUNNING, ENTITY_TYPE.PROCESS);
        ProcessInstancesResult responseInstance = prism.getProcessHelper()
            .getProcessInstanceStatus(Util.readEntityName(bundles[0].getProcessData()),
                "?start=" + processStartTime + "&end=" + TimeUtil
                    .addMinsToTime(processStartTime, 45));
        AssertUtil.assertSucceeded(responseInstance);
        Assert.assertTrue(responseInstance.getInstances() != null);

        responseInstance = prism.getProcessHelper()
            .getProcessInstanceSuspend(Util.readEntityName(bundles[0].getProcessData()),
                "?start=" + TimeUtil
                    .addMinsToTime(processStartTime, 37) + "&end=" +
                    TimeUtil.addMinsToTime(processStartTime, 44));
        AssertUtil.assertSucceeded(responseInstance);
        Assert.assertTrue(responseInstance.getInstances() != null);

        responseInstance = prism.getProcessHelper()
            .getProcessInstanceStatus(Util.readEntityName(bundles[0].getProcessData()),
                "?start=" + TimeUtil
                    .addMinsToTime(processStartTime, 37) + "&end=" +
                    TimeUtil.addMinsToTime(processStartTime, 44));
        AssertUtil.assertSucceeded(responseInstance);
        Assert.assertTrue(responseInstance.getInstances() != null);

        responseInstance = prism.getProcessHelper()
            .getProcessInstanceResume(Util.readEntityName(bundles[0].getProcessData()),
                "?start=" + processStartTime + "&end=" + TimeUtil
                    .addMinsToTime(processStartTime, 7));
        AssertUtil.assertSucceeded(responseInstance);
        Assert.assertTrue(responseInstance.getInstances() != null);

        responseInstance = prism.getProcessHelper()
            .getProcessInstanceStatus(Util.readEntityName(bundles[0].getProcessData()),
                "?start=" + TimeUtil
                    .addMinsToTime(processStartTime, 16) + "&end=" +
                    TimeUtil.addMinsToTime(processStartTime, 45));
        AssertUtil.assertSucceeded(responseInstance);
        Assert.assertTrue(responseInstance.getInstances() != null);

        responseInstance = cluster1.getProcessHelper()
            .getProcessInstanceKill(Util.readEntityName(bundles[0].getProcessData()),
                "?start=" + processStartTime + "&end=" + TimeUtil
                    .addMinsToTime(processStartTime, 7));
        AssertUtil.assertSucceeded(responseInstance);
        Assert.assertTrue(responseInstance.getInstances() != null);

        responseInstance = prism.getProcessHelper()
            .getProcessInstanceRerun(Util.readEntityName(bundles[0].getProcessData()),
                "?start=" + processStartTime + "&end=" + TimeUtil
                    .addMinsToTime(processStartTime, 7));
        AssertUtil.assertSucceeded(responseInstance);
        Assert.assertTrue(responseInstance.getInstances() != null);
    }
}

