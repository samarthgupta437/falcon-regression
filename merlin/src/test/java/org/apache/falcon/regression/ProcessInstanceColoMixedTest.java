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
import org.apache.falcon.regression.core.response.ProcessInstancesResult;
import org.apache.falcon.regression.core.response.ServiceResponse;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.core.util.Util.URLS;
import org.apache.falcon.regression.core.util.XmlUtil;
import org.apache.falcon.regression.testHelper.BaseMultiClusterTests;
import org.apache.falcon.regression.testHelper.BaseSingleClusterTests;
import org.apache.oozie.client.CoordinatorAction.Status;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.lang.reflect.Method;

/**
 * Process instance mixed colo tests.
 */
@SuppressWarnings("deprecation")
public class ProcessInstanceColoMixedTest extends BaseMultiClusterTests {

    private final String baseTestHDFSDir = baseHDFSDir + "/ProcessInstanceColoMixedTest";
    private final String datePattern = "/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}/";
    private final String feedPath = baseTestHDFSDir + "/feed0%d" + datePattern;

    private Bundle b1 = new Bundle();
    private Bundle b2 = new Bundle();
    private Bundle b3 = new Bundle();

    public ProcessInstanceColoMixedTest() throws IOException {
        super();
    }

    @BeforeClass(alwaysRun = true)
    public void prepareClusters() throws Exception {
        Util.print("in @BeforeClass");

        Util.restartService(prism.getClusterHelper());
        Util.restartService(server1.getClusterHelper());
        Util.restartService(server3.getClusterHelper());
    }

    @BeforeMethod(alwaysRun = true)
    public void setup(Method method) throws Exception {
        Util.print("test name: " + method.getName());

        //get 3 unique bundles
        b1 = (Bundle) Util.readELBundles()[0][0];
        b1.generateUniqueBundle();
        b2 = (Bundle) Util.readELBundles()[0][0];
        b2.generateUniqueBundle();
        b3 = (Bundle) Util.readELBundles()[0][0];
        b3.generateUniqueBundle();

        //generate bundles according to config files
        b1 = new Bundle(b1, server1.getEnvFileName());
        b2 = new Bundle(b2, server2.getEnvFileName());
        b3 = new Bundle(b3, server3.getEnvFileName());

        //set cluster colos
        b1.setCLusterColo("ua1");
        Util.print("cluster b1: " + b1.getClusters().get(0));
        b2.setCLusterColo("ua2");
        Util.print("cluster b2: " + b2.getClusters().get(0));
        b3.setCLusterColo("ua3");
        Util.print("cluster b3: " + b3.getClusters().get(0));

        //submit 3 clusters
        Bundle.submitCluster(b1, b2, b3);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown(Method method) throws Exception {
        Util.print("tearDown " + method.getName());

        b1.deleteBundle(prism);
        b2.deleteBundle(prism);
        b3.deleteBundle(prism);
    }

    @Test(timeOut = 12000000)
    public void mixed01_C1sC2sC1eC2e() throws Exception {
        //ua1 and ua3 are source. ua2 target.   feed01 on ua1 , feed02 on ua3
        //get 2 unique feeds
        String feed01 = Util.getInputFeedFromBundle(b1);
        String feed02 = Util.getInputFeedFromBundle(b2);
        String outputFeed = Util.getOutputFeedFromBundle(b1);
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
        Util.HDFSCleanup(server1, prefix.substring(1));
        InstanceUtil.createDataWithinDatesAndPrefix(server1,
                    InstanceUtil.oozieDateToDate(InstanceUtil.getTimeWrtSystemTime(-100)),
                    InstanceUtil.oozieDateToDate(InstanceUtil.getTimeWrtSystemTime(100)), prefix,
                1);


        prefix = InstanceUtil.getFeedPrefix(feed02);
        Util.HDFSCleanup(server3, prefix.substring(1));
        InstanceUtil.createDataWithinDatesAndPrefix(server3,
                    InstanceUtil.oozieDateToDate(InstanceUtil.getTimeWrtSystemTime(-100)),
                    InstanceUtil.oozieDateToDate(InstanceUtil.getTimeWrtSystemTime(100)), prefix,
                1);

        String startTime = InstanceUtil.getTimeWrtSystemTime(-70);

        //set clusters for feed01
        feed01 = InstanceUtil
                .setFeedCluster(feed01, XmlUtil.createValidity(startTime, "2099-01-01T00:00Z"),
                        XmlUtil.createRtention("days(10000)", ActionType.DELETE),
                        Util.readClusterName(b1.getClusters().get(0)), ClusterType.SOURCE,
                        null);
        feed01 = InstanceUtil
                .setFeedCluster(feed01, XmlUtil.createValidity(startTime, "2099-01-01T00:00Z"),
                        XmlUtil.createRtention("days(10000)", ActionType.DELETE),
                        Util.readClusterName(b3.getClusters().get(0)), ClusterType.TARGET,
                        null);

        //set clusters for feed02
        feed02 = InstanceUtil
                .setFeedCluster(feed02, XmlUtil.createValidity(startTime, "2099-01-01T00:00Z"),
                        XmlUtil.createRtention("days(10000)", ActionType.DELETE),
                        Util.readClusterName(b1.getClusters().get(0)), ClusterType.TARGET,
                        null);
        feed02 = InstanceUtil
                .setFeedCluster(feed02, XmlUtil.createValidity(startTime, "2099-01-01T00:00Z"),
                        XmlUtil.createRtention("days(10000)", ActionType.DELETE),
                        Util.readClusterName(b3.getClusters().get(0)), ClusterType.SOURCE,
                        null);

        //set clusters for output feed
        outputFeed = InstanceUtil.setFeedCluster(outputFeed,
                XmlUtil.createValidity(startTime, "2099-01-01T00:00Z"),
                XmlUtil.createRtention("days(10000)", ActionType.DELETE),
                Util.readClusterName(b1.getClusters().get(0)), ClusterType.SOURCE, null);
        outputFeed = InstanceUtil.setFeedCluster(outputFeed,
                XmlUtil.createValidity(startTime, "2099-01-01T00:00Z"),
                XmlUtil.createRtention("days(10000)", ActionType.DELETE),
                Util.readClusterName(b3.getClusters().get(0)), ClusterType.TARGET, null);

        //submit and schedule feeds
        Util.print("feed01: " + feed01);
        Util.print("feed02: " + feed02);
        Util.print("outputFeed: " + outputFeed);

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
        String process = b1.getProcessData();

        //add clusters to process

        String processStartTime = InstanceUtil.getTimeWrtSystemTime(-16);
        // String processEndTime = InstanceUtil.getTimeWrtSystemTime(20);

        process = InstanceUtil
                .setProcessCluster(process, null,
                        XmlUtil.createProcessValidity(startTime, "2099-01-01T00:00Z"));
        process = InstanceUtil
                .setProcessCluster(process, Util.readClusterName(b1.getClusters().get(0)),
                        XmlUtil.createProcessValidity(processStartTime,
                                InstanceUtil.addMinsToTime(processStartTime, 35)));
        process = InstanceUtil
                .setProcessCluster(process, Util.readClusterName(b3.getClusters().get(0)),
                        XmlUtil.createProcessValidity(
                                InstanceUtil.addMinsToTime(processStartTime, 16),
                                InstanceUtil.addMinsToTime(processStartTime, 45)));
        process = InstanceUtil
                .addProcessInputFeed(process, Util.readDatasetName(feed02),
                        Util.readDatasetName(feed02));


        //submit and schedule process
        Util.print("process: " + process);

        prism.getProcessHelper()
                .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, process);

        Util.print("Wait till process goes into running ");

        int i;

        for (i = 0; i < 30; i++) {
            Status sUa1 =
                    InstanceUtil.getInstanceStatus(server1, Util.getProcessName(process), 0, 0);
            Status sUa2 =
                    InstanceUtil.getInstanceStatus(server3, Util.getProcessName(process), 0, 0);
            if ((sUa1.toString().equals("RUNNING") || (sUa1.toString().equals("SUCCEEDED"))) &&
                    (sUa2.toString().equals("RUNNING") || sUa2.toString().equals("SUCCEEDED")))
                break;
            Thread.sleep(20000);

        }

        if (i == 30)
            Assert.assertTrue(false);

        ProcessInstancesResult responseInstance = prism.getProcessHelper()
                .getProcessInstanceStatus(Util.readEntityName(b1.getProcessData()),
                        "?start=" + processStartTime + "&end=" + InstanceUtil
                                .addMinsToTime(processStartTime, 45));
        Util.assertSucceeded(responseInstance);
        Assert.assertTrue(responseInstance.getInstances() != null);

        responseInstance = prism.getProcessHelper()
                .getProcessInstanceSuspend(Util.readEntityName(b1.getProcessData()),
                        "?start=" + InstanceUtil
                                .addMinsToTime(processStartTime, 37) + "&end=" +
                                InstanceUtil.addMinsToTime(processStartTime, 44));
        Util.assertSucceeded(responseInstance);
        Assert.assertTrue(responseInstance.getInstances() != null);

        responseInstance = prism.getProcessHelper()
                .getProcessInstanceStatus(Util.readEntityName(b1.getProcessData()),
                        "?start=" + InstanceUtil
                                .addMinsToTime(processStartTime, 37) + "&end=" +
                                InstanceUtil.addMinsToTime(processStartTime, 44));
        Util.assertSucceeded(responseInstance);
        Assert.assertTrue(responseInstance.getInstances() == null);

        responseInstance = prism.getProcessHelper()
                .getProcessInstanceResume(Util.readEntityName(b1.getProcessData()),
                        "?start=" + processStartTime + "&end=" + InstanceUtil
                                .addMinsToTime(processStartTime, 7));
        Util.assertSucceeded(responseInstance);
        Assert.assertTrue(responseInstance.getInstances() != null);

        responseInstance = prism.getProcessHelper()
                .getProcessInstanceStatus(Util.readEntityName(b1.getProcessData()),
                        "?start=" + InstanceUtil
                                .addMinsToTime(processStartTime, 16) + "&end=" +
                                InstanceUtil.addMinsToTime(processStartTime, 45));
        Util.assertSucceeded(responseInstance);
        Assert.assertTrue(responseInstance.getInstances() != null);

        responseInstance = server1.getProcessHelper()
                .getProcessInstanceKill(Util.readEntityName(b1.getProcessData()),
                        "?start=" + processStartTime + "&end=" + InstanceUtil
                                .addMinsToTime(processStartTime, 7));
        Util.assertSucceeded(responseInstance);
        Assert.assertTrue(responseInstance.getInstances() != null);

        responseInstance = prism.getProcessHelper()
                .getProcessInstanceRerun(Util.readEntityName(b1.getProcessData()),
                        "?start=" + processStartTime + "&end=" + InstanceUtil
                                .addMinsToTime(processStartTime, 7));
        Util.assertSucceeded(responseInstance);
        Assert.assertTrue(responseInstance.getInstances() != null);
    }
}

