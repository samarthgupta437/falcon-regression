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
import org.apache.falcon.regression.core.enumsAndConstants.ENTITY_TYPE;
import org.apache.falcon.entity.v0.Frequency.TimeUnit;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.response.ProcessInstancesResult;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.core.util.TimeUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowJob.Status;
import org.joda.time.DateTime;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

@Test(groups = "embedded")
public class ProcessInstanceRerunTest extends BaseTestClass {

    String baseTestDir = baseHDFSDir + "/ProcessInstanceRerunTest";
    String aggregateWorkflowDir = baseTestDir + "/aggregator";
    String feedInputPath = baseTestDir + "/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}";
    String feedOutputPath = baseTestDir + "/output-data/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}";
    String feedInputTimedOutPath =
        baseTestDir + "/timedout/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}";

    ColoHelper cluster = servers.get(0);
    FileSystem clusterFS = serverFS.get(0);
    OozieClient clusterOC = serverOC.get(0);
    private static final Logger logger = Logger.getLogger(ProcessInstanceRerunTest.class);

    @BeforeClass(alwaysRun = true)
    public void createTestData() throws Exception {

        logger.info("in @BeforeClass");

        HadoopUtil.uploadDir(clusterFS, aggregateWorkflowDir, OSUtil.RESOURCES_OOZIE);
        Bundle b = BundleUtil.readELBundles()[0][0];

        b = new Bundle(b, cluster);

        String startDate = "2010-01-01T20:00Z";
        String endDate = "2010-01-03T01:04Z";

        b.setInputFeedDataPath(feedInputPath);
        String prefix = b.getFeedDataPathPrefix();
        HadoopUtil.deleteDirIfExists(prefix.substring(1), clusterFS);

        List<String> dataDates = TimeUtil.getMinuteDatesOnEitherSide(startDate, endDate, 20);

        HadoopUtil.flattenAndPutDataInFolder(clusterFS, OSUtil.NORMAL_INPUT, prefix, dataDates);
    }


    @BeforeMethod(alwaysRun = true)
    public void setup(Method method) throws Exception {
        logger.info("test name: " + method.getName());
        bundles[0] = BundleUtil.readELBundles()[0][0];
        bundles[0] = new Bundle(bundles[0], cluster);
        bundles[0].setInputFeedDataPath(feedInputPath);
        bundles[0].setProcessWorkflow(aggregateWorkflowDir);
    }


    @AfterMethod(alwaysRun = true)
    public void tearDown(Method method) {
        logger.info("tearDown " + method.getName());
        removeBundles();
    }

    /**
     * Schedule process. Kill some instances. Rerun some of that killed. Check that
     * instances were rerun correctly and other are still killed.
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void testProcessInstanceRerun_someKilled02() throws Exception {
        bundles[0].setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:26Z");
        bundles[0].setProcessPeriodicity(5, TimeUnit.minutes);
        bundles[0].setOutputFeedPeriodicity(5, TimeUnit.minutes);
        bundles[0].setOutputFeedLocationData(feedOutputPath);
        bundles[0].setProcessConcurrency(5);
        bundles[0].submitAndScheduleBundle(prism);
        InstanceUtil.waitTillInstancesAreCreated(cluster, bundles[0].getProcessData(), 0, 10);
        ProcessInstancesResult r = prism.getProcessHelper()
            .getProcessInstanceKill(Util.readEntityName(bundles[0].getProcessData()),
                "?start=2010-01-02T01:00Z&end=2010-01-02T01:16Z");
        InstanceUtil.validateResponse(r, 4, 0, 0, 0, 4);
        List<String> wfIDs =
            InstanceUtil.getWorkflows(cluster, Util.getProcessName(bundles[0].getProcessData
                ()));
        prism.getProcessHelper()
            .getProcessInstanceRerun(Util.readEntityName(bundles[0].getProcessData()),
                "?start=2010-01-02T01:00Z&end=2010-01-02T01:11Z");
        InstanceUtil.areWorkflowsRunning(clusterOC, wfIDs, 6, 5, 1, 0);
    }

    /**
     * Schedule process. Kill all instances. Rerun them. Check that they were rerun.
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void testProcessInstanceRerun_multipleKilled() throws Exception {
        bundles[0].setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:11Z");
        bundles[0].setProcessPeriodicity(5, TimeUnit.minutes);
        bundles[0].setOutputFeedPeriodicity(5, TimeUnit.minutes);
        bundles[0].setOutputFeedLocationData(feedOutputPath);
        bundles[0].setProcessConcurrency(5);

        logger.info("process: " + Util.prettyPrintXml(bundles[0].getProcessData()));

        bundles[0].submitAndScheduleBundle(prism);
        InstanceUtil.waitTillInstancesAreCreated(cluster, bundles[0].getProcessData(), 0, 10);
        ProcessInstancesResult r = prism.getProcessHelper()
            .getProcessInstanceKill(Util.readEntityName(bundles[0].getProcessData()),
                "?start=2010-01-02T01:00Z&end=2010-01-02T01:11Z");
        InstanceUtil.validateResponse(r, 3, 0, 0, 0, 3);
        List<String> wfIDs =
            InstanceUtil.getWorkflows(cluster, Util.getProcessName(bundles[0].getProcessData
                ()));
        prism.getProcessHelper()
            .getProcessInstanceRerun(Util.readEntityName(bundles[0].getProcessData()),
                "?start=2010-01-02T01:00Z&end=2010-01-02T01:11Z");
        InstanceUtil
            .areWorkflowsRunning(clusterOC, wfIDs, 3, 3, 0,
                0);
    }

    /**
     * Schedule process. Kill some instances. Rerun them. Check that there are no killed
     * instances left.
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void testProcessInstanceRerun_someKilled01() throws Exception {
        bundles[0].setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:26Z");
        bundles[0].setProcessPeriodicity(5, TimeUnit.minutes);
        bundles[0].setOutputFeedPeriodicity(5, TimeUnit.minutes);
        bundles[0].setOutputFeedLocationData(feedOutputPath);
        bundles[0].setProcessConcurrency(5);
        bundles[0].submitAndScheduleBundle(prism);
        InstanceUtil.waitTillInstancesAreCreated(cluster, bundles[0].getProcessData(), 0, 10);
        ProcessInstancesResult r = prism.getProcessHelper()
            .getProcessInstanceKill(Util.readEntityName(bundles[0].getProcessData()),
                "?start=2010-01-02T01:00Z&end=2010-01-02T01:11Z");
        InstanceUtil.validateResponse(r, 3, 0, 0, 0, 3);
        List<String> wfIDs =
            InstanceUtil.getWorkflows(cluster, Util.getProcessName(bundles[0].getProcessData
                ()));
        prism.getProcessHelper()
            .getProcessInstanceRerun(Util.readEntityName(bundles[0].getProcessData()),
                "?start=2010-01-02T01:00Z&end=2010-01-02T01:11Z");
        InstanceUtil.areWorkflowsRunning(clusterOC, wfIDs, 6, 6, 0, 0);
    }

    /**
     * Schedule process. Kill single instance. Rerun it. Check it was rerun.
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void testProcessInstanceRerun_singleKilled() throws Exception {
        bundles[0].setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:04Z");
        bundles[0].setProcessPeriodicity(5, TimeUnit.minutes);
        bundles[0].setOutputFeedPeriodicity(5, TimeUnit.minutes);
        bundles[0].setOutputFeedLocationData(feedOutputPath);
        bundles[0].setProcessConcurrency(1);
        bundles[0].submitAndScheduleBundle(prism);
        InstanceUtil.waitTillInstancesAreCreated(cluster, bundles[0].getProcessData(), 0, 10);
        prism.getProcessHelper()
            .getProcessInstanceKill(Util.readEntityName(bundles[0].getProcessData()),
                "?start=2010-01-02T01:00Z");
        String wfID = InstanceUtil.getWorkflows(cluster,
            Util.getProcessName(bundles[0].getProcessData()),
            Status.KILLED)
            .get(0);
        prism.getProcessHelper()
            .getProcessInstanceRerun(Util.readEntityName(bundles[0].getProcessData()),
                "?start=2010-01-02T01:00Z");
        Assert.assertTrue(InstanceUtil.isWorkflowRunning(clusterOC,
            wfID));
    }

    /**
     * Schedule process. Wait till it got succeeded. Rerun first succeeded instance. Check if it
     * is running.
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void testProcessInstanceRerun_singleSucceeded() throws Exception {
        bundles[0].setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:04Z");
        bundles[0].setProcessPeriodicity(5, TimeUnit.minutes);
        bundles[0].setOutputFeedPeriodicity(5, TimeUnit.minutes);
        bundles[0].setOutputFeedLocationData(feedOutputPath);
        bundles[0].setProcessConcurrency(6);
        bundles[0].submitAndScheduleBundle(prism);
        InstanceUtil.waitTillInstancesAreCreated(cluster, bundles[0].getProcessData(), 0, 10);
        String wfID = InstanceUtil.getWorkflows(cluster, Util.getProcessName(bundles[0]
            .getProcessData()), Status.RUNNING, Status.SUCCEEDED).get(0);
        InstanceUtil.waitTillInstanceReachState(clusterOC, Util.readEntityName(bundles[0]
            .getProcessData()), 0, CoordinatorAction
            .Status.SUCCEEDED, 10, ENTITY_TYPE.PROCESS);
        prism.getProcessHelper()
            .getProcessInstanceRerun(Util.readEntityName(bundles[0].getProcessData()),
                "?start=2010-01-02T01:00Z");
        Assert.assertTrue(InstanceUtil.isWorkflowRunning(clusterOC, wfID));
    }

    /**
     * Schedule process. Suspend its instances. Try to rerun them. Check that instances weren't
     * rerun and are still suspended.
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void testProcessInstanceRerun_singleSuspended() throws Exception {
        bundles[0].setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:06Z");
        bundles[0].setProcessPeriodicity(5, TimeUnit.minutes);
        bundles[0].setOutputFeedPeriodicity(5, TimeUnit.minutes);
        bundles[0].setOutputFeedLocationData(feedOutputPath);
        bundles[0].setProcessConcurrency(2);
        bundles[0].submitAndScheduleBundle(prism);
        prism.getProcessHelper()
            .getProcessInstanceSuspend(Util.readEntityName(bundles[0].getProcessData()),
                "?start=2010-01-02T01:00Z&end=2010-01-02T01:06Z");
        prism.getProcessHelper()
            .getProcessInstanceRerun(Util.readEntityName(bundles[0].getProcessData()),
                "?start=2010-01-02T01:00Z&end=2010-01-02T01:06Z");
        Assert.assertEquals(InstanceUtil
            .getInstanceStatus(cluster, Util.getProcessName(bundles[0].getProcessData()), 0, 1),
            CoordinatorAction.Status.SUSPENDED);
    }

    /**
     * Schedule process. Wait till its instances succeed. Rerun them all. Check they are running.
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void testProcessInstanceRerun_multipleSucceeded() throws Exception {
        bundles[0].setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:11Z");
        bundles[0].setProcessPeriodicity(5, TimeUnit.minutes);
        bundles[0].setOutputFeedPeriodicity(5, TimeUnit.minutes);
        bundles[0].setOutputFeedLocationData(feedOutputPath);
        bundles[0].setProcessConcurrency(3);
        bundles[0].submitAndScheduleBundle(prism);
        InstanceUtil.waitTillInstancesAreCreated(cluster, bundles[0].getProcessData(), 0, 10);
        InstanceUtil.waitTillInstanceReachState(clusterOC, Util.readEntityName(bundles[0]
            .getProcessData()), 2, CoordinatorAction.Status.SUCCEEDED, 10, ENTITY_TYPE.PROCESS);
        List<String> wfIDs =
            InstanceUtil.getWorkflows(cluster, Util.getProcessName(bundles[0].getProcessData
                ()));
        prism.getProcessHelper()
            .getProcessInstanceRerun(Util.readEntityName(bundles[0].getProcessData()),
                "?start=2010-01-02T01:00Z&end=2010-01-02T01:11Z");
        InstanceUtil
            .areWorkflowsRunning(clusterOC, wfIDs,
                3,
                3, 0,
                0);
    }

    /**
     * Schedule process with invalid input feed data path. Wait till process got timed-out. Rerun
     * it's instances. Check that they were rerun and are waiting (wait for input data).
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void testProcessInstanceRerun_timedOut() throws Exception {
        bundles[0].setInputFeedDataPath(feedInputTimedOutPath);
        bundles[0].setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:11Z");
        bundles[0].setProcessPeriodicity(5, TimeUnit.minutes);
        bundles[0].setProcessTimeOut(2, TimeUnit.minutes);
        bundles[0].setOutputFeedPeriodicity(5, TimeUnit.minutes);
        bundles[0].setOutputFeedLocationData(feedOutputPath);
        bundles[0].setProcessConcurrency(3);
        bundles[0].submitAndScheduleBundle(prism);
        CoordinatorAction.Status s = null;
        InstanceUtil.waitTillInstanceReachState(clusterOC, Util.getProcessName(bundles[0]
            .getProcessData()), 1, CoordinatorAction.Status.TIMEDOUT, 20, ENTITY_TYPE.PROCESS);
        prism.getProcessHelper()
            .getProcessInstanceRerun(Util.readEntityName(bundles[0].getProcessData()),
                "?start=2010-01-02T01:00Z&end=2010-01-02T01:11Z");
        s = InstanceUtil
            .getInstanceStatus(cluster, Util.readEntityName(bundles[0].getProcessData()), 0, 0);
        Assert.assertTrue(CoordinatorAction.Status.WAITING.equals(s),
            "instance should have been in WAITING state");
    }


    @AfterClass(alwaysRun = true)
    public void deleteData() throws Exception {
        logger.info("in @AfterClass");
        Bundle b = BundleUtil.readELBundles()[0][0];
        b = new Bundle(b, cluster);

        b.setInputFeedDataPath(feedInputPath);
        String prefix = b.getFeedDataPathPrefix();
        HadoopUtil.deleteDirIfExists(prefix.substring(1), clusterFS);
    }
}
