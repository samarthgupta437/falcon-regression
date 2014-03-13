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
import org.apache.falcon.regression.core.response.ProcessInstancesResult;
import org.apache.falcon.regression.core.response.ServiceResponse;
import org.apache.falcon.regression.core.util.AssertUtil;
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
    private String aggregateWorkflowDir = baseWorkflowDir + "/aggregator";
    ColoHelper cluster1 = servers.get(0);
    ColoHelper cluster3 = servers.get(2);
    FileSystem cluster1FS = serverFS.get(0);
    FileSystem cluster3FS = serverFS.get(2);

    @BeforeClass(alwaysRun = true)
    public void prepareClusters() throws Exception {
        Util.print("in @BeforeClass");
        HadoopUtil.uploadDir(cluster1FS, aggregateWorkflowDir, "src/test/resources/oozie");
        HadoopUtil.uploadDir(cluster3FS, aggregateWorkflowDir, "src/test/resources/oozie");
    }

    @BeforeMethod(alwaysRun = true)
    public void setup(Method method) throws Exception {
        Util.print("test name: " + method.getName());

        //get 3 unique bundles
        bundles[0] = Util.readELBundles()[0][0];
        bundles[0].generateUniqueBundle();
        bundles[2] = Util.readELBundles()[0][0];
        bundles[2].generateUniqueBundle();

        //generate bundles according to config files
        bundles[0] = new Bundle(bundles[0], cluster1.getEnvFileName(), cluster1.getPrefix());
        bundles[2] = new Bundle(bundles[2], cluster3.getEnvFileName(), cluster3.getPrefix());

        //set cluster colos
        bundles[0].setCLusterColo(bundles[0].getClusterHelper().getColo().split("=")[1]);
        Util.print("cluster b1: " + bundles[0].getClusters().get(0));
        bundles[2].setCLusterColo(bundles[2].getClusterHelper().getColo().split("=")[1]);
        Util.print("cluster b3: " + bundles[2].getClusters().get(0));

        bundles[0].setProcessWorkflow(aggregateWorkflowDir);
        bundles[2].setProcessWorkflow(aggregateWorkflowDir);
        //submit 3 clusters
        Bundle.submitCluster(bundles[0], bundles[2]);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown(Method method) throws Exception {
        Util.print("tearDown " + method.getName());
        removeBundles();
    }

    @Test(timeOut = 12000000)
    public void mixed01_C1sC2sC1eC2e() throws Exception {
        //ua1 and ua3 are source. ua2 target.   feed01 on ua1 , feed03 on ua3
        //get 2 unique feeds
        String feed01 = Util.getInputFeedFromBundle(bundles[0]);
        String feed03 = Util.getInputFeedFromBundle(bundles[2]);
        String outputFeed = Util.getOutputFeedFromBundle(bundles[0]);
        //set source and target for the 2 feeds

        //set clusters to null;
        feed01 = InstanceUtil
                .setFeedCluster(feed01,
                        XmlUtil.createValidity("2009-02-01T00:00Z", "2012-01-01T00:00Z"),
                        XmlUtil.createRtention("days(10000)", ActionType.DELETE), null,
                        ClusterType.SOURCE, null, null);
        feed03 = InstanceUtil
                .setFeedCluster(feed03,
                        XmlUtil.createValidity("2009-02-01T00:00Z", "2012-01-01T00:00Z"),
                        XmlUtil.createRtention("days(10000)", ActionType.DELETE), null,
                        ClusterType.SOURCE, null, null);
        outputFeed = InstanceUtil
                .setFeedCluster(outputFeed,
                        XmlUtil.createValidity("2009-02-01T00:00Z", "2012-01-01T00:00Z"),
                        XmlUtil.createRtention("days(10000)", ActionType.DELETE), null,
                        ClusterType.SOURCE, null, null);


        //set new feed input data
        feed01 = Util.setFeedPathValue(feed01, String.format(feedPath, 1));
        feed03 = Util.setFeedPathValue(feed03, String.format(feedPath, 3));

        //generate data in both the colos ua1 and ua3
        String prefix = InstanceUtil.getFeedPrefix(feed01);
        HadoopUtil.deleteDirIfExists(prefix.substring(1), cluster1FS);
        InstanceUtil.createDataWithinDatesAndPrefix(cluster1,
                    InstanceUtil.oozieDateToDate(InstanceUtil.getTimeWrtSystemTime(-100)),
                    InstanceUtil.oozieDateToDate(InstanceUtil.getTimeWrtSystemTime(100)), prefix,
                1);


        prefix = InstanceUtil.getFeedPrefix(feed03);
        HadoopUtil.deleteDirIfExists(prefix.substring(1), cluster3FS);
        InstanceUtil.createDataWithinDatesAndPrefix(cluster3,
                    InstanceUtil.oozieDateToDate(InstanceUtil.getTimeWrtSystemTime(-100)),
                    InstanceUtil.oozieDateToDate(InstanceUtil.getTimeWrtSystemTime(100)), prefix,
                1);

        String startTime = InstanceUtil.getTimeWrtSystemTime(-70);

        //set clusters for feed01
        feed01 = InstanceUtil
                .setFeedCluster(feed01, XmlUtil.createValidity(startTime, "2099-01-01T00:00Z"),
                        XmlUtil.createRtention("days(10000)", ActionType.DELETE),
                        Util.readClusterName(bundles[0].getClusters().get(0)), ClusterType.SOURCE,
                        null, null);
        feed01 = InstanceUtil
                .setFeedCluster(feed01, XmlUtil.createValidity(startTime, "2099-01-01T00:00Z"),
                        XmlUtil.createRtention("days(10000)", ActionType.DELETE),
                        Util.readClusterName(bundles[2].getClusters().get(0)), ClusterType.TARGET,
                        null, null);

        //set clusters for feed03
        feed03 = InstanceUtil
                .setFeedCluster(feed03, XmlUtil.createValidity(startTime, "2099-01-01T00:00Z"),
                        XmlUtil.createRtention("days(10000)", ActionType.DELETE),
                        Util.readClusterName(bundles[0].getClusters().get(0)), ClusterType.TARGET,
                        null, null);
        feed03 = InstanceUtil
                .setFeedCluster(feed03, XmlUtil.createValidity(startTime, "2099-01-01T00:00Z"),
                        XmlUtil.createRtention("days(10000)", ActionType.DELETE),
                        Util.readClusterName(bundles[2].getClusters().get(0)), ClusterType.SOURCE,
                        null, null);

        //set clusters for output feed
        outputFeed = InstanceUtil.setFeedCluster(outputFeed,
                XmlUtil.createValidity(startTime, "2099-01-01T00:00Z"),
                XmlUtil.createRtention("days(10000)", ActionType.DELETE),
                Util.readClusterName(bundles[0].getClusters().get(0)), ClusterType.SOURCE, null, null);
        outputFeed = InstanceUtil.setFeedCluster(outputFeed,
                XmlUtil.createValidity(startTime, "2099-01-01T00:00Z"),
                XmlUtil.createRtention("days(10000)", ActionType.DELETE),
                Util.readClusterName(bundles[2].getClusters().get(0)), ClusterType.TARGET, null, null);

        //submit and schedule feeds
        Util.print("feed01: " + feed01);
        Util.print("feed03: " + feed03);
        Util.print("outputFeed: " + outputFeed);

        ServiceResponse r = prism.getFeedHelper()
                .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, feed01);
        AssertUtil.assertSucceeded(r);
        r = prism.getFeedHelper().submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, feed03);
        AssertUtil.assertSucceeded(r);
        r = prism.getFeedHelper()
                .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, outputFeed);
        AssertUtil.assertSucceeded(r);

        //create a process with 2 clusters

        //get a process
        String process = bundles[0].getProcessData();

        //add clusters to process

        String processStartTime = InstanceUtil.getTimeWrtSystemTime(-16);
        // String processEndTime = InstanceUtil.getTimeWrtSystemTime(20);

        process = InstanceUtil
                .setProcessCluster(process, null,
                        XmlUtil.createProcessValidity(startTime, "2099-01-01T00:00Z"));
        process = InstanceUtil
                .setProcessCluster(process, Util.readClusterName(bundles[0].getClusters().get(0)),
                        XmlUtil.createProcessValidity(processStartTime,
                                InstanceUtil.addMinsToTime(processStartTime, 35)));
        process = InstanceUtil
                .setProcessCluster(process, Util.readClusterName(bundles[2].getClusters().get(0)),
                        XmlUtil.createProcessValidity(
                                InstanceUtil.addMinsToTime(processStartTime, 16),
                                InstanceUtil.addMinsToTime(processStartTime, 45)));
        process = InstanceUtil
                .addProcessInputFeed(process, Util.readDatasetName(feed03),
                        Util.readDatasetName(feed03));


        //submit and schedule process
        Util.print("process: " + process);

        prism.getProcessHelper()
                .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, process);

        Util.print("Wait till process goes into running ");

        int i;

        Status sUa1 = null, sUa2 = null;
        for (i = 0; i < 30; i++) {
            sUa1 = InstanceUtil.getInstanceStatus(cluster1, Util.getProcessName(process), 0, 0);
            sUa2 = InstanceUtil.getInstanceStatus(cluster3, Util.getProcessName(process), 0, 0);
            if (sUa1 != null && sUa2 != null &&
                    (sUa1.toString().equals("RUNNING") || sUa1.toString().equals("SUCCEEDED") ||
                            sUa1.toString().equals("KILLED")) &&
                    (sUa2.toString().equals("RUNNING") || sUa2.toString().equals("SUCCEEDED") ||
                            sUa2.toString().equals("KILLED")))
                break;
            Thread.sleep(20000);

        }

        Assert.assertNotNull(sUa1);
        Assert.assertTrue(sUa1.toString().equals("RUNNING") || sUa1.toString().equals("SUCCEEDED"));
        Assert.assertNotNull(sUa2);
        Assert.assertTrue(sUa2.toString().equals("RUNNING") || sUa2.toString().equals("SUCCEEDED"));

        ProcessInstancesResult responseInstance = prism.getProcessHelper()
                .getProcessInstanceStatus(Util.readEntityName(bundles[0].getProcessData()),
                        "?start=" + processStartTime + "&end=" + InstanceUtil
                                .addMinsToTime(processStartTime, 45));
        Util.assertSucceeded(responseInstance);
        Assert.assertTrue(responseInstance.getInstances() != null);

        responseInstance = prism.getProcessHelper()
                .getProcessInstanceSuspend(Util.readEntityName(bundles[0].getProcessData()),
                        "?start=" + InstanceUtil
                                .addMinsToTime(processStartTime, 37) + "&end=" +
                                InstanceUtil.addMinsToTime(processStartTime, 44));
        Util.assertSucceeded(responseInstance);
        Assert.assertTrue(responseInstance.getInstances() != null);

        responseInstance = prism.getProcessHelper()
                .getProcessInstanceStatus(Util.readEntityName(bundles[0].getProcessData()),
                        "?start=" + InstanceUtil
                                .addMinsToTime(processStartTime, 37) + "&end=" +
                                InstanceUtil.addMinsToTime(processStartTime, 44));
        Util.assertSucceeded(responseInstance);
        Assert.assertTrue(responseInstance.getInstances() == null);

        responseInstance = prism.getProcessHelper()
                .getProcessInstanceResume(Util.readEntityName(bundles[0].getProcessData()),
                        "?start=" + processStartTime + "&end=" + InstanceUtil
                                .addMinsToTime(processStartTime, 7));
        Util.assertSucceeded(responseInstance);
        Assert.assertTrue(responseInstance.getInstances() != null);

        responseInstance = prism.getProcessHelper()
                .getProcessInstanceStatus(Util.readEntityName(bundles[0].getProcessData()),
                        "?start=" + InstanceUtil
                                .addMinsToTime(processStartTime, 16) + "&end=" +
                                InstanceUtil.addMinsToTime(processStartTime, 45));
        Util.assertSucceeded(responseInstance);
        Assert.assertTrue(responseInstance.getInstances() != null);

        responseInstance = cluster1.getProcessHelper()
                .getProcessInstanceKill(Util.readEntityName(bundles[0].getProcessData()),
                        "?start=" + processStartTime + "&end=" + InstanceUtil
                                .addMinsToTime(processStartTime, 7));
        Util.assertSucceeded(responseInstance);
        Assert.assertTrue(responseInstance.getInstances() != null);

        responseInstance = prism.getProcessHelper()
                .getProcessInstanceRerun(Util.readEntityName(bundles[0].getProcessData()),
                        "?start=" + processStartTime + "&end=" + InstanceUtil
                                .addMinsToTime(processStartTime, 7));
        Util.assertSucceeded(responseInstance);
        Assert.assertTrue(responseInstance.getInstances() != null);
    }
}

