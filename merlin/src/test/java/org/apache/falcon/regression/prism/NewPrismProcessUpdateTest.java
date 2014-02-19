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
import org.apache.falcon.regression.core.generated.dependencies.Frequency;
import org.apache.falcon.regression.core.generated.dependencies.Frequency.TimeUnit;
import org.apache.falcon.regression.core.generated.feed.ClusterType;
import org.apache.falcon.regression.core.generated.process.ExecutionType;
import org.apache.falcon.regression.core.generated.process.Process;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.helpers.PrismHelper;
import org.apache.falcon.regression.core.response.APIResult;
import org.apache.falcon.regression.core.response.ServiceResponse;
import org.apache.falcon.regression.core.supportClasses.ENTITY_TYPE;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.core.util.Util.URLS;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.oozie.client.BundleJob;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.XOozieClient;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Minutes;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.*;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Date;
import java.util.Random;

public class NewPrismProcessUpdateTest extends BaseTestClass {

    String baseTestDir = baseHDFSDir + "/NewPrismProcessUpdateTest";
    String inputFeedPath = baseTestDir + "/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}";
    String WORKFLOW_PATH = baseWorkflowDir + "/falcon-oozie-wf";
    String WORKFLOW_PATH2 = baseWorkflowDir + "/falcon-oozie-wf2";
    String aggreagator1Path = baseWorkflowDir + "/aggregator1";
    ColoHelper cluster1 = servers.get(0);
    ColoHelper cluster2 = servers.get(1);
    ColoHelper cluster3 = servers.get(2);
    FileSystem cluster1FS = serverFS.get(0);
    FileSystem cluster2FS = serverFS.get(1);
    FileSystem cluster3FS = serverFS.get(2);
    OozieClient cluster2OC = serverOC.get(1);
    OozieClient cluster3OC = serverOC.get(2);

    @BeforeMethod(alwaysRun = true)
    public void testSetup(Method method) throws Exception {
        Util.print("test name: " + method.getName());
        Bundle b = (Bundle) Bundle.readBundle("updateBundle")[0][0];
        b.generateUniqueBundle();
        bundles[0] = new Bundle(b, cluster1);
        bundles[1] = new Bundle(b, cluster2);
        bundles[2] = new Bundle(b, cluster3);
        setBundleWFPath(bundles[0], bundles[1], bundles[2]);
        bundles[1].addClusterToBundle(bundles[2].getClusters().get(0), ClusterType.TARGET);
        usualGrind(cluster3, bundles[1]);
    }

    @BeforeClass
    public void setup() throws Exception {
        for (FileSystem fs : new FileSystem[]{cluster1FS, cluster2FS, cluster3FS}) {
            HadoopUtil.deleteDirIfExists(baseHDFSDir, fs);
            setupOozieData(fs, WORKFLOW_PATH, WORKFLOW_PATH2, aggreagator1Path);
        }
        Util.restartService(cluster3.getClusterHelper());
    }

    public void setupOozieData(FileSystem fs, String... workflowPaths) throws IOException {
        for (String workflowPath : workflowPaths) {
            HadoopUtil.deleteDirIfExists(workflowPath, fs);
            //create dir on hdfs
            fs.mkdirs(new Path(workflowPath));
            fs.setPermission(new Path(workflowPath), new FsPermission("777"));
            fs.mkdirs(new Path(workflowPath + "/lib"));
            fs.copyFromLocalFile(new Path("src/test/resources/oozie/workflow.xml"),
                    new Path(workflowPath + "/workflow.xml"));
            fs.copyFromLocalFile(new Path("src/test/resources/oozie/lib/oozie-examples-3.1.5.jar"),
                    new Path(workflowPath + "/lib/oozie-examples-3.1.5.jar"));
        }
    }


    @Test(groups = {"multiCluster"}, timeOut = 1200000)
    public void updateProcessFrequencyInEachColoWithOneProcessRunning_Monthly()
            throws Exception {
        final String START_TIME = InstanceUtil.getTimeWrtSystemTime(-20);
        String endTime = InstanceUtil.getTimeWrtSystemTime(4000 * 60);
        bundles[1].setProcessPeriodicity(1, TimeUnit.months);
        bundles[1].setOutputFeedPeriodicity(1, TimeUnit.months);
        bundles[1].setProcessValidity(START_TIME, endTime);

        bundles[1].submitBundle(prism);
        //now to schedule in 1 colo and let it remain in another
        Util.assertSucceeded(
                cluster3.getProcessHelper()
                        .schedule(URLS.SCHEDULE_URL, bundles[1].getProcessData()));
        String oldBundleId = InstanceUtil
                .getLatestBundleID(cluster3,
                        Util.readEntityName(bundles[1].getProcessData()), ENTITY_TYPE.PROCESS);

        waitForProcessToReachACertainState(cluster3, bundles[1], Job.Status.RUNNING);
        int coordCount = Util.getNumberOfWorkflowInstances(cluster3, oldBundleId);

        String updatedProcess = InstanceUtil
                .setProcessFrequency(bundles[1].getProcessData(),
                        new Frequency(5, TimeUnit.minutes));

        Util.print("updated process: " + updatedProcess);

        //now to update
        while (Util
                .parseResponse(prism.getProcessHelper()
                        .update((bundles[1].getProcessData()), updatedProcess))
                .getStatus() != APIResult.Status.SUCCEEDED) {
            System.out.println("update didnt SUCCEED in last attempt");
            Thread.sleep(10000);
        }

        String prismString = getResponse(prism, bundles[1], true);
        Assert.assertEquals(Util.getProcessObject(prismString).getFrequency(),
                Util.getProcessObject(updatedProcess).getFrequency());
        Thread.sleep(60000);
        dualComparison(bundles[1], cluster3);
        //ensure that the running process has new coordinators created; while the submitted
        // one is updated correctly.
        Util.verifyNewBundleCreation(cluster3, oldBundleId, coordCount,
                Util.readEntityName(bundles[1].getProcessData()), true);
        waitingForBundleFinish(cluster3, oldBundleId, 5);
        int finalNumberOfInstances =
                InstanceUtil.getProcessInstanceListFromAllBundles(cluster3,
                        Util.getProcessName(bundles[1].getProcessData()), ENTITY_TYPE.PROCESS).size();

        int expectedInstances = getExpectedNumberOfWorkflowInstances(
                bundles[1].getProcessObject().getClusters().getCluster().get(0).getValidity()
                        .getStart(),
                bundles[1].getProcessObject().getClusters().getCluster().get(0).getValidity()
                        .getEnd());

        Assert.assertEquals(finalNumberOfInstances, expectedInstances,
                "number of instances doesnt match :(");
    }

    @Test(groups = {"multiCluster"}, timeOut = 1200000)
    @SuppressWarnings("SleepWhileInLoop")
    public void updateProcessRollStartTimeForwardInEachColoWithOneProcessRunning()
            throws Exception {
        //bundles[1].generateUniqueBundle();
        bundles[1].submitBundle(prism);
        //now to schedule in 1 colo and let it remain in another
        Util.assertSucceeded(
                cluster3.getProcessHelper()
                        .schedule(URLS.SCHEDULE_URL, bundles[1].getProcessData()));
        String oldBundleId = InstanceUtil
                .getLatestBundleID(cluster3,
                        Util.readEntityName(bundles[1].getProcessData()), ENTITY_TYPE.PROCESS);

        Thread.sleep(10000);

        int coordCount = Util.getNumberOfWorkflowInstances(cluster3, oldBundleId);

        String newStartTime = InstanceUtil.addMinsToTime(InstanceUtil.dateToOozieDate(
                bundles[1].getProcessObject().getClusters().getCluster().get(0).getValidity()
                        .getStart()), 20);
        String newEndTime = InstanceUtil.addMinsToTime(InstanceUtil.dateToOozieDate(
                bundles[1].getProcessObject().getClusters().getCluster().get(0).getValidity()
                        .getStart()), 30);

        bundles[1].setProcessValidity(newStartTime, newEndTime);

        waitForProcessToReachACertainState(cluster3, bundles[1], Job.Status.RUNNING);

        System.out.println("updated process: " + bundles[1].getProcessData());
        while (Util.parseResponse(
                prism.getProcessHelper()
                        .update(bundles[1].getProcessData(), bundles[1].getProcessData()))
                .getStatus() != APIResult.Status.SUCCEEDED) {
            System.out.println("update didnt SUCCEED in last attempt");
            Thread.sleep(10000);
        }

        Util.verifyNewBundleCreation(cluster3, oldBundleId, coordCount,
                Util.readEntityName(bundles[1].getProcessData()), true);

        String prismString = getResponse(prism, bundles[1], true);

        dualComparison(bundles[1], cluster3);
        while (!Util.isBundleOver(cluster3, oldBundleId)) {
        }
        //ensure that the running process has new coordinators created; while the submitted
        // one is updated correctly.
        int finalNumberOfInstances =
                InstanceUtil.getProcessInstanceListFromAllBundles(cluster3,
                        Util.getProcessName(bundles[1].getProcessData()), ENTITY_TYPE.PROCESS).size();
        Assert.assertEquals(finalNumberOfInstances,
                getExpectedNumberOfWorkflowInstances(InstanceUtil
                        .dateToOozieDate(
                                bundles[1].getProcessObject().getClusters().getCluster().get(0)
                                        .getValidity().getStart()),
                        InstanceUtil
                                .dateToOozieDate(
                                        bundles[1].getProcessObject().getClusters().getCluster()
                                                .get(0).getValidity()
                                                .getEnd())));
        AssertUtil.checkNotStatus(cluster2OC, ENTITY_TYPE.PROCESS, bundles[1], Job.Status.RUNNING);
        int expectedNumberOfWorkflows =
                getExpectedNumberOfWorkflowInstances(newStartTime, InstanceUtil
                        .dateToOozieDate(
                                bundles[1].getProcessObject().getClusters().getCluster().get(0)
                                        .getValidity().getEnd()));
        Assert.assertEquals(Util.getNumberOfWorkflowInstances(cluster3, oldBundleId),
                expectedNumberOfWorkflows);
    }

    @Test(groups = {"multiCluster"}, timeOut = 1800000)
    public void updateProcessConcurrencyWorkflowExecutionInEachColoWithOneColoDown()
            throws Exception {
        try {
            //bundles[1].generateUniqueBundle();
            bundles[1].submitBundle(prism);
            //now to schedule in 1 colo and let it remain in another
            Util.assertSucceeded(
                    cluster3.getProcessHelper()
                            .schedule(URLS.SCHEDULE_URL, bundles[1].getProcessData()));
            String oldBundleId = InstanceUtil
                    .getLatestBundleID(cluster3,
                            Util.readEntityName(bundles[1].getProcessData()), ENTITY_TYPE.PROCESS);
            Thread.sleep(25000);
            int coordCount = Util.getNumberOfWorkflowInstances(cluster3, oldBundleId);

            int initialConcurrency = bundles[1].getProcessObject().getParallel();

            bundles[1].setProcessConcurrency(bundles[1].getProcessObject().getParallel() + 3);
            bundles[1].setProcessWorkflow(WORKFLOW_PATH2);
            bundles[1].getProcessObject().setOrder(getRandomExecutionType(bundles[1]));
            //suspend
            Util.shutDownService(cluster3.getProcessHelper());
            Util.assertPartialSucceeded(
                    prism.getProcessHelper()
                            .update(bundles[1].getProcessData(), bundles[1].getProcessData()));
            //now to update

            String prismString = getResponse(prism, bundles[1], true);
            Assert.assertEquals(Util.getProcessObject(prismString).getParallel(),
                    initialConcurrency);
            Assert.assertEquals(Util.getProcessObject(prismString).getWorkflow().getPath(),
                    WORKFLOW_PATH);
            Assert.assertEquals(Util.getProcessObject(prismString).getOrder(),
                    bundles[1].getProcessObject().getOrder());

            String coloString = getResponse(cluster2, bundles[1], true);
            Assert.assertEquals(Util.getProcessObject(coloString).getWorkflow().getPath(),
                    WORKFLOW_PATH2);

            Util.startService(cluster3.getProcessHelper());

            dualComparisonFailure(bundles[1], cluster3);
            //ensure that the running process has new coordinators created; while the submitted
            // one is updated correctly.
            AssertUtil.checkNotStatus(cluster2OC, ENTITY_TYPE.PROCESS, bundles[1], Job.Status.RUNNING);

            waitForProcessToReachACertainState(cluster3, bundles[1], Job.Status.RUNNING);
            while (Util.parseResponse(
                    prism.getProcessHelper()
                            .update(bundles[1].getProcessData(), bundles[1].getProcessData()))
                    .getStatus() != APIResult.Status.SUCCEEDED) {
                System.out.println("WARNING: update did not scceed, retyring ");
                Thread.sleep(20000);
            }
            prismString = getResponse(prism, bundles[1], true);
            Assert.assertEquals(Util.getProcessObject(prismString).getParallel(),
                    initialConcurrency + 3);
            Assert.assertEquals(Util.getProcessObject(prismString).getWorkflow().getPath(),
                    WORKFLOW_PATH2);
            Assert.assertEquals(Util.getProcessObject(prismString).getOrder(),
                    bundles[1].getProcessObject().getOrder());
            dualComparison(bundles[1], cluster3);
            AssertUtil.checkNotStatus(cluster2OC, ENTITY_TYPE.PROCESS, bundles[1], Job.Status.RUNNING);
            waitingForBundleFinish(cluster3, oldBundleId);
            int finalNumberOfInstances =
                    InstanceUtil.getProcessInstanceListFromAllBundles(cluster3,
                            Util.getProcessName(bundles[1].getProcessData()), ENTITY_TYPE.PROCESS).size();

            int expectedInstances =
                    getExpectedNumberOfWorkflowInstances(InstanceUtil.dateToOozieDate(
                            bundles[1].getProcessObject().getClusters().getCluster().get(0)
                                    .getValidity().getStart()),
                            InstanceUtil
                                    .dateToOozieDate(
                                            bundles[1].getProcessObject().getClusters().getCluster()
                                                    .get(0).getValidity()
                                                    .getEnd()));
            Assert.assertEquals(finalNumberOfInstances, expectedInstances,
                    "number of instances doesnt match :(");
        } finally {
            Util.restartService(cluster3.getClusterHelper());
        }
    }


    @Test(groups = {"multiCluster"}, timeOut = 1200000)
    public void updateProcessFrequencyInEachColoWithOneProcessRunning() throws Exception {
        String startTime = InstanceUtil.getTimeWrtSystemTime(-2);
        String endTime = InstanceUtil.getTimeWrtSystemTime(20);
        bundles[1].setProcessValidity(startTime, endTime);

        //bundles[1].generateUniqueBundle();
        bundles[1].submitBundle(prism);
        //now to schedule in 1 colo and let it remain in another
        Util.assertSucceeded(
                cluster3.getProcessHelper()
                        .schedule(URLS.SCHEDULE_URL, bundles[1].getProcessData()));
        String oldBundleId = InstanceUtil
                .getLatestBundleID(cluster3,
                        Util.readEntityName(bundles[1].getProcessData()), ENTITY_TYPE.PROCESS);

        waitForProcessToReachACertainState(cluster3, bundles[1], Job.Status.RUNNING);
        int coordCount = Util.getNumberOfWorkflowInstances(cluster3, oldBundleId);

        Util.print("original process: " + bundles[1].getProcessData());

        String updatedProcess = InstanceUtil
                .setProcessFrequency(bundles[1].getProcessData(),
                        new Frequency(7, TimeUnit.minutes));

        Util.print("updated process: " + updatedProcess);

        //now to update
        String updatedTime = new DateTime(DateTimeZone.UTC).plusMinutes(2).toString();

        ServiceResponse response =
                prism.getProcessHelper().update(updatedProcess, updatedProcess);
        Util.assertSucceeded(response);
        String prismString = getResponse(prism, bundles[1], true);
        Assert.assertEquals(Util.getProcessObject(prismString).getFrequency(),
                Util.getProcessObject(updatedProcess).getFrequency());
        dualComparison(bundles[1], cluster3);
        //ensure that the running process has new coordinators created; while the submitted
        // one is updated
        // correctly.
        Util.verifyNewBundleCreation(cluster3, oldBundleId, coordCount,
                Util.readEntityName(bundles[1].getProcessData()), true);
        AssertUtil.checkNotStatus(cluster2OC, ENTITY_TYPE.PROCESS, bundles[1], Job.Status.RUNNING);
    }


    @Test(groups = {"multiCluster"}, timeOut = 1200000)
    public void updateProcessNameInEachColoWithOneProcessRunning() throws Exception {
        //bundles[1].generateUniqueBundle();
        bundles[1].submitBundle(prism);
        //now to schedule in 1 colo and let it remain in another
        Util.assertSucceeded(
                cluster3.getProcessHelper()
                        .schedule(URLS.SCHEDULE_URL, bundles[1].getProcessData()));
        String originalProcessData = bundles[1].getProcessData();
        String oldBundleId = InstanceUtil
                .getLatestBundleID(cluster3,
                        Util.readEntityName(bundles[1].getProcessData()), ENTITY_TYPE.PROCESS);

        waitForProcessToReachACertainState(cluster3, bundles[1], Job.Status.RUNNING);
        Thread.sleep(20000);
        int coordCount = Util.getNumberOfWorkflowInstances(cluster3, oldBundleId);

        String oldName = new String(bundles[1].getProcessObject().getName());
        bundles[1].setProcessName("myNewProcessName");

        //now to update
        ServiceResponse response =
                prism.getProcessHelper().update((bundles[1].getProcessData()), bundles[1].getProcessData());
        Util.assertFailed(response);
        String prismString = getResponse(prism, bundles[1], false);
        Util.verifyNewBundleCreation(cluster3, oldBundleId, coordCount,
                Util.readEntityName(originalProcessData), false);
        AssertUtil.checkNotStatus(cluster2OC, ENTITY_TYPE.PROCESS, bundles[1], Job.Status.RUNNING);
    }

    @Test(groups = {"multiCluster"}, timeOut = 1200000)
    public void updateProcessConcurrencyInEachColoWithOneProcessRunning()
            throws Exception {
        String startTime = InstanceUtil.getTimeWrtSystemTime(-2);
        String endTime = InstanceUtil.getTimeWrtSystemTime(10);
        bundles[1].setProcessValidity(startTime, endTime);

        //bundles[1].generateUniqueBundle();
        bundles[1].submitBundle(prism);
        //now to schedule in 1 colo and let it remain in another
        Util.assertSucceeded(
                cluster3.getProcessHelper()
                        .schedule(URLS.SCHEDULE_URL, bundles[1].getProcessData()));

        String oldBundleId = InstanceUtil
                .getLatestBundleID(cluster3,
                        Util.readEntityName(bundles[1].getProcessData()), ENTITY_TYPE.PROCESS);

        waitForProcessToReachACertainState(cluster3, bundles[1], Job.Status.RUNNING);

        //now to update
        DateTime updateTime = new DateTime(DateTimeZone.UTC);
        Thread.sleep(60000);

        System.out.println("updating at " + updateTime);
        while (Util
                .parseResponse(updateProcessConcurrency(bundles[1],
                        bundles[1].getProcessObject().getParallel() + 3))
                .getStatus() != APIResult.Status.SUCCEEDED) {
            System.out.println("WARNING: update did not scceed, retyring ");
            Thread.sleep(20000);
        }

        String prismString = getResponse(prism, bundles[1], true);
        Assert.assertEquals(Util.getProcessObject(prismString).getParallel(),
                bundles[1].getProcessObject().getParallel() + 3);
        dualComparison(bundles[1], cluster3);
        //ensure that the running process has new coordinators created; while the submitted
        // one is updated
        // correctly.
        Util.verifyNewBundleCreation(cluster3, oldBundleId, 0,
                Util.readEntityName(bundles[1].getProcessData()),
                false);
        AssertUtil.checkNotStatus(cluster2OC, ENTITY_TYPE.PROCESS, bundles[1], Job.Status.RUNNING);

        Job.Status status = Util.getOozieJobStatus(cluster3.getFeedHelper().getOozieClient(),
                Util.readEntityName(bundles[1].getProcessData()), ENTITY_TYPE.PROCESS);

        boolean doesExist = false;
        while (status != Job.Status.SUCCEEDED && status != Job.Status.FAILED &&
                status != Job.Status.DONEWITHERROR) {
            int statusCount = InstanceUtil
                    .getInstanceCountWithStatus(cluster3,
                            Util.readEntityName(bundles[1].getProcessData()),
                            org.apache.oozie.client.CoordinatorAction.Status.RUNNING,
                            ENTITY_TYPE.PROCESS);
            if (statusCount == bundles[1].getProcessObject().getParallel() + 3) {
                doesExist = true;
                break;
            }
            status = Util.getOozieJobStatus(cluster3.getFeedHelper().getOozieClient(),
                    Util.readEntityName(bundles[1].getProcessData()), ENTITY_TYPE.PROCESS);
            Assert.assertNotNull(status,
                    "status must not be null!");
            Thread.sleep(30000);
        }

        Assert.assertTrue(doesExist, "Er! The desired concurrency levels are never reached!!!");
        int expectedNumberOfInstances =
                getExpectedNumberOfWorkflowInstances(InstanceUtil.dateToOozieDate(
                        bundles[1].getProcessObject().getClusters().getCluster().get(0)
                                .getValidity().getStart()),
                        InstanceUtil
                                .dateToOozieDate(
                                        bundles[1].getProcessObject().getClusters().getCluster()
                                                .get(0).getValidity()
                                                .getEnd()));
        Assert.assertEquals(Util.getNumberOfWorkflowInstances(cluster3, oldBundleId),
                expectedNumberOfInstances);
    }


    @Test(groups = {"multiCluster"}, timeOut = 1200000)
    @SuppressWarnings("SleepWhileInLoop")
    public void updateProcessIncreaseValidityInEachColoWithOneProcessRunning() throws Exception {
        bundles[1].submitBundle(prism);
        //now to schedule in 1 colo and let it remain in another
        Util.assertSucceeded(
                cluster3.getProcessHelper()
                        .schedule(URLS.SCHEDULE_URL, bundles[1].getProcessData()));
        String oldBundleId = InstanceUtil
                .getLatestBundleID(cluster3,
                        Util.readEntityName(bundles[1].getProcessData()), ENTITY_TYPE.PROCESS);
        Thread.sleep(15000);

        int coordCount = Util.getNumberOfWorkflowInstances(cluster3, oldBundleId);

        String newEndTime = InstanceUtil.addMinsToTime(InstanceUtil.dateToOozieDate(
                bundles[1].getProcessObject().getClusters().getCluster().get(0).getValidity()
                        .getEnd()), 4);
        bundles[1].setProcessValidity(InstanceUtil.dateToOozieDate(
                bundles[1].getProcessObject().getClusters().getCluster().get(0).getValidity()
                        .getStart()),
                newEndTime);

        waitForProcessToReachACertainState(cluster3, bundles[1], Job.Status.RUNNING);

        while (Util.parseResponse(
                prism.getProcessHelper()
                        .update(bundles[1].getProcessData(), bundles[1].getProcessData()))
                .getStatus() != APIResult.Status.SUCCEEDED) {
        }

        Util.verifyNewBundleCreation(cluster3, oldBundleId, coordCount,
                Util.readEntityName(bundles[1].getProcessData()), false);

        int i = 0;

        while (Util.getNumberOfWorkflowInstances(cluster3, oldBundleId)
                != getExpectedNumberOfWorkflowInstances(InstanceUtil.dateToOozieDate(
                bundles[1].getProcessObject().getClusters().getCluster().get(0).getValidity()
                        .getStart()),
                InstanceUtil
                        .dateToOozieDate(
                                bundles[1].getProcessObject().getClusters().getCluster().get(0)
                                        .getValidity()
                                        .getEnd()))
                && i < 10) {
            Thread.sleep(1000);
            i++;
        }

        String prismString = getResponse(prism, bundles[1], true);

        bundles[1].verifyDependencyListing();

        dualComparison(bundles[1], cluster3);
        waitingForBundleFinish(cluster3, oldBundleId);
        //ensure that the running process has new coordinators created; while the submitted
        // one is updated
        // correctly.
        int finalNumberOfInstances = InstanceUtil
                .getProcessInstanceList(cluster3,
                        Util.getProcessName(bundles[1].getProcessData()), ENTITY_TYPE.PROCESS)
                .size();
        Assert.assertEquals(finalNumberOfInstances,
                getExpectedNumberOfWorkflowInstances(InstanceUtil
                        .dateToOozieDate(
                                bundles[1].getProcessObject().getClusters().getCluster().get(0)
                                        .getValidity().getStart()),
                        InstanceUtil
                                .dateToOozieDate(
                                        bundles[1].getProcessObject().getClusters().getCluster()
                                                .get(0).getValidity()
                                                .getEnd())));
        AssertUtil.checkNotStatus(cluster2OC, ENTITY_TYPE.PROCESS, bundles[1], Job.Status.RUNNING);
    }

    @Test(groups = {"multiCluster"}, timeOut = 1200000)
    public void updateProcessConcurrencyInEachColoWithOneProcessSuspended()
            throws Exception {
        String startTime = InstanceUtil.getTimeWrtSystemTime(-2);
        String endTime = InstanceUtil.getTimeWrtSystemTime(7);
        bundles[1].setProcessValidity(startTime, endTime);

        //bundles[1].generateUniqueBundle();
        bundles[1].submitBundle(prism);
        //now to schedule in 1 colo and let it remain in another
        Util.assertSucceeded(
                cluster3.getProcessHelper()
                        .schedule(URLS.SCHEDULE_URL, bundles[1].getProcessData()));
        String oldBundleId = InstanceUtil
                .getLatestBundleID(cluster3,
                        Util.readEntityName(bundles[1].getProcessData()), ENTITY_TYPE.PROCESS);
        Util.assertSucceeded(
                cluster3.getProcessHelper()
                        .schedule(URLS.SCHEDULE_URL, bundles[1].getProcessData()));

        waitForProcessToReachACertainState(cluster3, bundles[1], Job.Status.RUNNING);
        Thread.sleep(20000);
        int coordCount = Util.getNumberOfWorkflowInstances(cluster3, oldBundleId);

        Util.assertSucceeded(
                cluster3.getProcessHelper()
                        .suspend(URLS.SUSPEND_URL, bundles[1].getProcessData()));
        //now to update
        while (Util
                .parseResponse(updateProcessConcurrency(bundles[1],
                        bundles[1].getProcessObject().getParallel() + 3))
                .getStatus() != APIResult.Status.SUCCEEDED) {
            System.out.println("WARNING: update did not scceed, retyring ");
            Thread.sleep(20000);
        }

        String prismString = getResponse(prism, bundles[1], true);
        Assert.assertEquals(Util.getProcessObject(prismString).getParallel(),
                bundles[1].getProcessObject().getParallel() + 3);
        dualComparison(bundles[1], cluster3);
        //ensure that the running process has new coordinators created; while the submitted
        // one is updated correctly.
        Util.verifyNewBundleCreation(cluster3, oldBundleId, coordCount,
                Util.readEntityName(bundles[1].getProcessData()), false);
        AssertUtil.checkNotStatus(cluster2OC, ENTITY_TYPE.PROCESS, bundles[1], Job.Status.RUNNING);
        Util.assertSucceeded(cluster3.getProcessHelper()
                .resume(URLS.RESUME_URL, bundles[1].getProcessData()));
        AssertUtil.checkStatus(cluster3OC, ENTITY_TYPE.PROCESS, bundles[1], Job.Status.RUNNING);

        Job.Status status = Util.getOozieJobStatus(cluster3.getFeedHelper().getOozieClient(),
                Util.readEntityName(bundles[1].getProcessData()), ENTITY_TYPE.PROCESS);

        boolean doesExist = false;
        while (status != Job.Status.SUCCEEDED && status != Job.Status.FAILED &&
                status != Job.Status.DONEWITHERROR) {
            if (InstanceUtil
                    .getInstanceCountWithStatus(cluster3,
                            Util.readEntityName(bundles[1].getProcessData()),
                            org.apache.oozie.client.CoordinatorAction.Status.RUNNING,
                            ENTITY_TYPE.PROCESS) ==
                    bundles[1].getProcessObject().getParallel()) {
                doesExist = true;
                break;
            }
            status = Util.getOozieJobStatus(cluster3.getFeedHelper().getOozieClient(),
                    Util.readEntityName(bundles[1].getProcessData()), ENTITY_TYPE.PROCESS);
        }

        Assert.assertTrue(doesExist, "Er! The desired concurrency levels are never reached!!!");

        waitingForBundleFinish(cluster3, oldBundleId);

        int finalNumberOfInstances =
                InstanceUtil.getProcessInstanceListFromAllBundles(cluster3,
                        Util.getProcessName(bundles[1].getProcessData()), ENTITY_TYPE.PROCESS).size();

        int expectedInstances =
                getExpectedNumberOfWorkflowInstances(InstanceUtil.dateToOozieDate(
                        bundles[1].getProcessObject().getClusters().getCluster().get(0)
                                .getValidity().getStart()),
                        InstanceUtil
                                .dateToOozieDate(
                                        bundles[1].getProcessObject().getClusters().getCluster()
                                                .get(0).getValidity()
                                                .getEnd()));

        Assert.assertEquals(finalNumberOfInstances, expectedInstances,
                "number of instances doesnt match :(");
    }

    @Test(groups = {"multiCluster"}, timeOut = 1200000)
    public void updateProcessConcurrencyInEachColoWithOneColoDown() throws Exception {
        try {
            String startTime = InstanceUtil.getTimeWrtSystemTime(-1);
            String endTime = InstanceUtil.getTimeWrtSystemTime(5);
            bundles[1].setProcessValidity(startTime, endTime);

            bundles[1].submitBundle(prism);
            //now to schedule in 1 colo and let it remain in another

            System.out.println("process to be scheduled: " + bundles[1].getProcessData());

            Util.assertSucceeded(
                    cluster3.getProcessHelper()
                            .schedule(URLS.SCHEDULE_URL, bundles[1].getProcessData()));
            String oldBundleId = InstanceUtil
                    .getLatestBundleID(cluster3,
                            Util.readEntityName(bundles[1].getProcessData()), ENTITY_TYPE.PROCESS);

            int coordCount = Util.getNumberOfWorkflowInstances(cluster3, oldBundleId);

            //now to update
            Util.shutDownService(cluster3.getClusterHelper());

            ServiceResponse response =
                    updateProcessConcurrency(bundles[1],
                            bundles[1].getProcessObject().getParallel() + 3);
            Util.assertPartialSucceeded(response);

            Util.startService(cluster3.getClusterHelper());

            String prismString = getResponse(prism, bundles[1], true);
            dualComparisonFailure(bundles[1], cluster3);
            Assert.assertEquals(Util.getProcessObject(prismString).getParallel(),
                    bundles[1].getProcessObject().getParallel());

            //ensure that the running process has new coordinators created; while the submitted
            // one is updated correctly.
            AssertUtil.checkNotStatus(cluster2OC, ENTITY_TYPE.PROCESS, bundles[1],
                    Job.Status.RUNNING);

            while (Util
                    .parseResponse(updateProcessConcurrency(bundles[1],
                            bundles[1].getProcessObject().getParallel() + 3))
                    .getStatus() != APIResult.Status.SUCCEEDED) {
                System.out.println("WARNING: update did not scceed, retyring ");
                Thread.sleep(20000);
            }
            prismString = dualComparison(bundles[1], cluster3);
            dualComparison(bundles[1], cluster2);

            Job.Status status = Util.getOozieJobStatus(cluster3.getFeedHelper().getOozieClient(),
                    Util.readEntityName(bundles[1].getProcessData()), ENTITY_TYPE.PROCESS);

            boolean doesExist = false;
            while (status != Job.Status.SUCCEEDED && status != Job.Status.FAILED &&
                    status != Job.Status.DONEWITHERROR) {
                if (InstanceUtil
                        .getInstanceCountWithStatus(cluster3,
                                Util.readEntityName(bundles[1].getProcessData()),
                                org.apache.oozie.client.CoordinatorAction.Status.RUNNING,
                                ENTITY_TYPE.PROCESS) ==
                        bundles[1].getProcessObject().getParallel() + 3) {
                    doesExist = true;
                    break;
                }
                status = Util.getOozieJobStatus(cluster3.getFeedHelper().getOozieClient(),
                        Util.readEntityName(bundles[1].getProcessData()), ENTITY_TYPE.PROCESS);
                Thread.sleep(30000);
            }
            Assert.assertTrue(doesExist, "Er! The desired concurrency levels are never reached!!!");
            Util.verifyNewBundleCreation(cluster3, InstanceUtil
                    .getLatestBundleID(cluster3,
                            Util.readEntityName(bundles[1].getProcessData()), ENTITY_TYPE.PROCESS),
                    coordCount, Util.readEntityName(bundles[1].getProcessData()), false);

            waitingForBundleFinish(cluster3, oldBundleId);

            int finalNumberOfInstances =
                    InstanceUtil.getProcessInstanceListFromAllBundles(cluster3,
                            Util.getProcessName(bundles[1].getProcessData()), ENTITY_TYPE.PROCESS).size();

            int expectedInstances =
                    getExpectedNumberOfWorkflowInstances(InstanceUtil.dateToOozieDate(
                            bundles[1].getProcessObject().getClusters().getCluster().get(0)
                                    .getValidity().getStart()),
                            InstanceUtil
                                    .dateToOozieDate(
                                            bundles[1].getProcessObject().getClusters().getCluster()
                                                    .get(0).getValidity()
                                                    .getEnd()));
            Assert.assertEquals(finalNumberOfInstances, expectedInstances,
                    "number of instances doesnt match :(");
        } finally {
            Util.restartService(cluster3.getProcessHelper());
        }
    }

    @Test(groups = {"multiCluster"}, timeOut = 1200000)
    public void updateProcessConcurrencyExecutionWorkflowInEachColoWithOneProcessRunning()
            throws Exception {
        String startTime = InstanceUtil.getTimeWrtSystemTime(-2);
        String endTime = InstanceUtil.getTimeWrtSystemTime(6);
        bundles[1].setProcessValidity(startTime, endTime);

        bundles[1].submitBundle(prism);
        //now to schedule in 1 colo and let it remain in another
        Util.assertSucceeded(
                cluster3.getProcessHelper()
                        .schedule(URLS.SCHEDULE_URL, bundles[1].getProcessData()));
        String oldBundleId = InstanceUtil
                .getLatestBundleID(cluster3,
                        Util.readEntityName(bundles[1].getProcessData()), ENTITY_TYPE.PROCESS);
        Thread.sleep(30000);
        waitForProcessToReachACertainState(cluster3, bundles[1], Job.Status.RUNNING);

        int coordCount = Util.getNumberOfWorkflowInstances(cluster3, oldBundleId);

        int initialConcurrency = bundles[1].getProcessObject().getParallel();

        bundles[1].setProcessConcurrency(bundles[1].getProcessObject().getParallel() + 3);
        bundles[1].setProcessWorkflow(aggreagator1Path);
        bundles[1].getProcessObject().setOrder(getRandomExecutionType(bundles[1]));

        //now to update

        String updateTime = new DateTime(DateTimeZone.UTC).plusMinutes(2).toString();

        System.out.println("updating @ " + updateTime);

        while (Util.parseResponse(
                prism.getProcessHelper().update((bundles[1].getProcessData()), bundles[1]
                        .getProcessData())).getStatus() != APIResult.Status.SUCCEEDED) {
            Thread.sleep(10000);
        }
        String prismString = getResponse(prism, bundles[1], true);
        Assert.assertEquals(Util.getProcessObject(prismString).getParallel(),
                initialConcurrency + 3);
        Assert.assertEquals(Util.getProcessObject(prismString).getWorkflow().getPath(),
                aggreagator1Path);
        Assert.assertEquals(Util.getProcessObject(prismString).getOrder(),
                bundles[1].getProcessObject().getOrder());
        dualComparison(bundles[1], cluster3);
        //ensure that the running process has new coordinators created; while the submitted
        // one is updated correctly.
        waitingForBundleFinish(cluster3, oldBundleId);
        Util.verifyNewBundleCreation(cluster3, oldBundleId, coordCount,
                Util.readEntityName(bundles[1].getProcessData()), true);
        AssertUtil.checkNotStatus(cluster2OC, ENTITY_TYPE.PROCESS, bundles[1], Job.Status.RUNNING);
        int finalNumberOfInstances =
                InstanceUtil.getProcessInstanceListFromAllBundles(cluster3,
                        Util.getProcessName(bundles[1].getProcessData()), ENTITY_TYPE.PROCESS).size();
        int expectedInstances =
                getExpectedNumberOfWorkflowInstances(InstanceUtil.dateToOozieDate(
                        bundles[1].getProcessObject().getClusters().getCluster().get(0)
                                .getValidity().getStart()),
                        InstanceUtil
                                .dateToOozieDate(
                                        bundles[1].getProcessObject().getClusters().getCluster()
                                                .get(0).getValidity()
                                                .getEnd()));

        Assert.assertEquals(finalNumberOfInstances, expectedInstances,
                "number of instances doesnt match :(");
    }

    @Test(groups = {"multiCluster"}, timeOut = 1200000)
    public void updateProcessConcurrencyExecutionWorkflowInEachColoWithOneProcessSuspended()
            throws Exception {
        String startTime = InstanceUtil.getTimeWrtSystemTime(2);
        String endTime = InstanceUtil.getTimeWrtSystemTime(6);
        bundles[1].setProcessValidity(startTime, endTime);

        bundles[1].submitBundle(prism);
        //now to schedule in 1 colo and let it remain in another
        Util.assertSucceeded(
                cluster3.getProcessHelper()
                        .schedule(URLS.SCHEDULE_URL, bundles[1].getProcessData()));
        String oldBundleId = InstanceUtil
                .getLatestBundleID(cluster3,
                        Util.readEntityName(bundles[1].getProcessData()), ENTITY_TYPE.PROCESS);

        Thread.sleep(25000);
        waitForProcessToReachACertainState(cluster3, bundles[1], Job.Status.RUNNING);

        int coordCount = Util.getNumberOfWorkflowInstances(cluster3, oldBundleId);

        int initialConcurrency = bundles[1].getProcessObject().getParallel();

        bundles[1].setProcessConcurrency(bundles[1].getProcessObject().getParallel() + 3);
        bundles[1].setProcessWorkflow(aggreagator1Path);
        bundles[1].getProcessObject().setOrder(getRandomExecutionType(bundles[1]));
        //suspend
        Util.assertSucceeded(
                cluster3.getProcessHelper()
                        .suspend(URLS.SUSPEND_URL, bundles[1].getProcessData()));

        //now to update
        String updateTime = new DateTime(DateTimeZone.UTC).plusMinutes(2).toString();
        System.out.println("updating @ " + updateTime);
        while (Util.parseResponse(
                prism.getProcessHelper()
                        .update((bundles[1].getProcessData()), bundles[1].getProcessData()))
                .getStatus() != APIResult.Status.SUCCEEDED) {
            //keep waiting
        }

        Util.assertSucceeded(cluster3.getProcessHelper()
                .resume(URLS.RESUME_URL, bundles[1].getProcessData()));

        String prismString = getResponse(prism, bundles[1], true);
        Assert.assertEquals(Util.getProcessObject(prismString).getParallel(),
                initialConcurrency + 3);
        Assert.assertEquals(Util.getProcessObject(prismString).getWorkflow().getPath(),
                aggreagator1Path);
        Assert.assertEquals(Util.getProcessObject(prismString).getOrder(),
                bundles[1].getProcessObject().getOrder());
        dualComparison(bundles[1], cluster3);
        //ensure that the running process has new coordinators created; while the submitted
        // one is updated correctly.
        waitingForBundleFinish(cluster3, oldBundleId);
        Util.verifyNewBundleCreation(cluster3, oldBundleId, coordCount,
                Util.readEntityName(bundles[1].getProcessData()), true);
        AssertUtil.checkNotStatus(cluster3OC, ENTITY_TYPE.PROCESS, bundles[1], Job.Status.RUNNING);
        int finalNumberOfInstances =
                InstanceUtil.getProcessInstanceListFromAllBundles(cluster3,
                        Util.getProcessName(bundles[1].getProcessData()), ENTITY_TYPE.PROCESS).size();

        int expectedInstances =
                getExpectedNumberOfWorkflowInstances(InstanceUtil.dateToOozieDate(
                        bundles[1].getProcessObject().getClusters().getCluster().get(0)
                                .getValidity().getStart()),
                        InstanceUtil
                                .dateToOozieDate(
                                        bundles[1].getProcessObject().getClusters().getCluster()
                                                .get(0).getValidity()
                                                .getEnd()));

        Assert.assertEquals(finalNumberOfInstances, expectedInstances,
                "number of instances doesnt match :(");
    }

    @Test(groups = {"multiCluster"}, timeOut = 1200000)
    public void updateProcessAddNewInputInEachColoWithOneProcessRunning() throws Exception {
        String startTime = InstanceUtil.getTimeWrtSystemTime(-2);
        String endTime = InstanceUtil.getTimeWrtSystemTime(6);
        bundles[1].setProcessValidity(startTime, endTime);

        bundles[1].submitBundle(prism);
        //now to schedule in 1 colo and let it remain in another
        Util.assertSucceeded(
                cluster3.getProcessHelper()
                        .schedule(URLS.SCHEDULE_URL, bundles[1].getProcessData()));
        String oldBundleId = InstanceUtil
                .getLatestBundleID(cluster3,
                        Util.readEntityName(bundles[1].getProcessData()), ENTITY_TYPE.PROCESS);

        Thread.sleep(20000);
        waitForProcessToReachACertainState(cluster3, bundles[1], Job.Status.RUNNING);
        int coordCount = Util.getNumberOfWorkflowInstances(cluster3, oldBundleId);


        String newFeedName = Util.getInputFeedNameFromBundle(bundles[1]) + "2";
        String inputFeed = Util.getInputFeedFromBundle(bundles[1]);

        bundles[1].addProcessInput(newFeedName, "inputData2");
        inputFeed = Util.setFeedName(inputFeed, newFeedName);

        System.out.println(inputFeed);
        Util.assertSucceeded(
                prism.getFeedHelper().submitEntity(URLS.SUBMIT_URL, inputFeed));

        while (Util.parseResponse(
                prism.getProcessHelper()
                        .update((bundles[1].getProcessData()), bundles[1].getProcessData()))
                .getStatus() != APIResult.Status.SUCCEEDED) {
            Thread.sleep(20000);
        }

        String prismString = getResponse(prism, bundles[1], true);

        bundles[1].verifyDependencyListing();

        dualComparison(bundles[1], cluster3);
        //ensure that the running process has new coordinators created; while the submitted
        // one is updated correctly.
        Util.verifyNewBundleCreation(cluster3, oldBundleId, coordCount,
                Util.readEntityName(bundles[1].getProcessData()), true);
        AssertUtil.checkNotStatus(cluster2OC, ENTITY_TYPE.PROCESS, bundles[1], Job.Status.RUNNING);
        waitingForBundleFinish(cluster3, oldBundleId);

        int finalNumberOfInstances =
                InstanceUtil.getProcessInstanceListFromAllBundles(cluster3,
                        Util.getProcessName(bundles[1].getProcessData()), ENTITY_TYPE.PROCESS).size();

        int expectedInstances =
                getExpectedNumberOfWorkflowInstances(InstanceUtil.dateToOozieDate(
                        bundles[1].getProcessObject().getClusters().getCluster().get(0)
                                .getValidity().getStart()),
                        InstanceUtil
                                .dateToOozieDate(
                                        bundles[1].getProcessObject().getClusters().getCluster()
                                                .get(0).getValidity()
                                                .getEnd()));

        Assert.assertEquals(finalNumberOfInstances, expectedInstances,
                "number of instances doesnt match :(");
    }

    @Test(groups = {"multiCluster"}, timeOut = 1200000)
    public void updateProcessAddNewInputInEachColoWithOneProcessSuspended() throws Exception {
        String startTime = InstanceUtil.getTimeWrtSystemTime(1);
        String endTime = InstanceUtil.getTimeWrtSystemTime(6);
        bundles[1].setProcessValidity(startTime, endTime);

        bundles[1].submitBundle(prism);
        //now to schedule in 1 colo and let it remain in another
        Util.assertSucceeded(
                cluster3.getProcessHelper()
                        .schedule(URLS.SCHEDULE_URL, bundles[1].getProcessData()));
        String oldBundleId = InstanceUtil
                .getLatestBundleID(cluster3,
                        Util.readEntityName(bundles[1].getProcessData()), ENTITY_TYPE.PROCESS);

        Thread.sleep(10000);
        waitForProcessToReachACertainState(cluster3, bundles[1], Job.Status.RUNNING);
        int coordCount = Util.getNumberOfWorkflowInstances(cluster3, oldBundleId);


        String newFeedName = Util.getInputFeedNameFromBundle(bundles[1]) + "2";
        String inputFeed = Util.getInputFeedFromBundle(bundles[1]);

        bundles[1].addProcessInput(newFeedName, "inputData2");
        inputFeed = Util.setFeedName(inputFeed, newFeedName);

        Util.assertSucceeded(
                cluster3.getProcessHelper()
                        .suspend(URLS.SUSPEND_URL, bundles[1].getProcessData()));
        Util.assertSucceeded(
                prism.getFeedHelper().submitEntity(URLS.SUBMIT_URL, inputFeed));

        while (Util.parseResponse(
                prism.getProcessHelper()
                        .update((bundles[1].getProcessData()), bundles[1].getProcessData()))
                .getStatus() != APIResult.Status.SUCCEEDED) {
            //keep waiting
        }
        Util.assertSucceeded(cluster3.getProcessHelper()
                .resume(URLS.RESUME_URL, bundles[1].getProcessData()));


        String prismString = dualComparison(bundles[1], cluster2);

        bundles[1].verifyDependencyListing();

        dualComparison(bundles[1], cluster3);
        //ensure that the running process has new coordinators created; while the submitted
        // one is updated correctly.
        waitingForBundleFinish(cluster3, oldBundleId);
        Util.verifyNewBundleCreation(cluster3, oldBundleId, coordCount,
                Util.readEntityName(bundles[1].getProcessData()), true);
        AssertUtil.checkNotStatus(cluster3OC, ENTITY_TYPE.PROCESS, bundles[1], Job.Status.RUNNING);

        int finalNumberOfInstances =
                InstanceUtil.getProcessInstanceListFromAllBundles(cluster3,
                        Util.getProcessName(bundles[1].getProcessData()), ENTITY_TYPE.PROCESS).size();

        int expectedInstances =
                getExpectedNumberOfWorkflowInstances(InstanceUtil.dateToOozieDate(
                        bundles[1].getProcessObject().getClusters().getCluster().get(0)
                                .getValidity().getStart()),
                        InstanceUtil
                                .dateToOozieDate(
                                        bundles[1].getProcessObject().getClusters().getCluster()
                                                .get(0).getValidity()
                                                .getEnd()));

        Assert.assertEquals(finalNumberOfInstances, expectedInstances,
                "number of instances doesnt match :(");
    }

    @Test(groups = {"multiCluster"}, timeOut = 1200000)
    public void updateProcessAddNewInputInEachColoWithOneColoDown() throws Exception {
        try {
            String startTime = InstanceUtil.getTimeWrtSystemTime(-2);
            String endTime = InstanceUtil.getTimeWrtSystemTime(6);
            bundles[1].setProcessValidity(startTime, endTime);

            bundles[1].submitBundle(prism);
            //now to schedule in 1 colo and let it remain in another
            Util.assertSucceeded(
                    cluster3.getProcessHelper()
                            .schedule(URLS.SCHEDULE_URL, bundles[1].getProcessData()));
            String oldBundleId = InstanceUtil
                    .getLatestBundleID(cluster3,
                            Util.readEntityName(bundles[1].getProcessData()), ENTITY_TYPE.PROCESS);


            int coordCount = Util.getNumberOfWorkflowInstances(cluster3, oldBundleId);


            String newFeedName = Util.getInputFeedNameFromBundle(bundles[1]) + "2";
            String inputFeed = Util.getInputFeedFromBundle(bundles[1]);

            bundles[1].addProcessInput(newFeedName, "inputData2");
            inputFeed = Util.setFeedName(inputFeed, newFeedName);

            Util.assertSucceeded(
                    prism.getFeedHelper().submitEntity(URLS.SUBMIT_URL, inputFeed));

            Util.shutDownService(cluster3.getProcessHelper());

            Util.assertPartialSucceeded(
                    prism.getProcessHelper()
                            .update(bundles[1].getProcessData(), bundles[1].getProcessData()));

            Util.startService(cluster3.getProcessHelper());

            String prismString = getResponse(prism, bundles[1], true);

            bundles[1].verifyDependencyListing();

            dualComparisonFailure(bundles[1], cluster3);
            //ensure that the running process has new coordinators created; while the submitted
            // one is updated correctly.
            Util.verifyNewBundleCreation(cluster3, oldBundleId, coordCount,
                    Util.readEntityName(bundles[1].getProcessData()), false);
            AssertUtil.checkNotStatus(cluster2OC, ENTITY_TYPE.PROCESS, bundles[1], Job.Status.RUNNING);
            waitForProcessToReachACertainState(cluster3, bundles[1], Job.Status.RUNNING);

            while (Util.parseResponse(
                    prism.getProcessHelper()
                            .update((bundles[1].getProcessData()), bundles[1].getProcessData()))
                    .getStatus() != APIResult.Status.SUCCEEDED) {
                System.out.println("update didnt SUCCEED in last attempt");
                Thread.sleep(10000);
            }
            prismString = getResponse(prism, bundles[1], true);
            dualComparisonFailure(bundles[1], cluster3);
            bundles[1].verifyDependencyListing();
            Util.verifyNewBundleCreation(cluster3, oldBundleId, coordCount,
                    Util.readEntityName(bundles[1].getProcessData()), true);
            AssertUtil.checkNotStatus(cluster2OC, ENTITY_TYPE.PROCESS, bundles[1], Job.Status.RUNNING);
            waitingForBundleFinish(cluster3, oldBundleId);

            int finalNumberOfInstances =
                    InstanceUtil.getProcessInstanceListFromAllBundles(cluster3,
                            Util.getProcessName(bundles[1].getProcessData()), ENTITY_TYPE.PROCESS).size();

            int expectedInstances =
                    getExpectedNumberOfWorkflowInstances(InstanceUtil.dateToOozieDate(
                            bundles[1].getProcessObject().getClusters().getCluster().get(0)
                                    .getValidity().getStart()),
                            InstanceUtil
                                    .dateToOozieDate(
                                            bundles[1].getProcessObject().getClusters().getCluster()
                                                    .get(0).getValidity()
                                                    .getEnd()));

            Assert.assertEquals(finalNumberOfInstances, expectedInstances,
                    "number of instances doesnt match :(");
        } finally {
            Util.restartService(cluster3.getProcessHelper());
        }
    }

    @Test(groups = {"multiCluster"}, timeOut = 1200000)
    @SuppressWarnings("SleepWhileInLoop")
    public void updateProcessDecreaseValidityInEachColoWithOneProcessRunning() throws Exception {
        bundles[1].submitBundle(prism);
        //now to schedule in 1 colo and let it remain in another
        Util.assertSucceeded(
                cluster3.getProcessHelper()
                        .schedule(URLS.SCHEDULE_URL, bundles[1].getProcessData()));
        String oldBundleId = InstanceUtil
                .getLatestBundleID(cluster3,
                        Util.readEntityName(bundles[1].getProcessData()), ENTITY_TYPE.PROCESS);

        waitForProcessToReachACertainState(cluster3, bundles[1], Job.Status.RUNNING);

        int coordCount = Util.getNumberOfWorkflowInstances(cluster3, oldBundleId);

        String newEndTime = InstanceUtil.addMinsToTime(InstanceUtil.dateToOozieDate(
                bundles[1].getProcessObject().getClusters().getCluster().get(0).getValidity()
                        .getEnd()), -2);
        bundles[1].setProcessValidity(InstanceUtil.dateToOozieDate(
                bundles[1].getProcessObject().getClusters().getCluster().get(0).getValidity()
                        .getStart()),
                newEndTime);
        while (Util.parseResponse(
                (prism.getProcessHelper()
                        .update(bundles[1].getProcessData(), bundles[1].getProcessData())))
                .getStatus() != APIResult.Status.SUCCEEDED) {
            System.out.println("update didnt SUCCEED in last attempt");
            Thread.sleep(10000);
        }
        Util.verifyNewBundleCreation(cluster3, oldBundleId, coordCount,
                Util.readEntityName(bundles[1].getProcessData()), false);

        String prismString = getResponse(prism, bundles[1], true);

        bundles[1].verifyDependencyListing();

        dualComparison(bundles[1], cluster3);
        waitingForBundleFinish(cluster3, oldBundleId);
        //ensure that the running process has new coordinators created; while the submitted
        // one is updated correctly.
        int finalNumberOfInstances = InstanceUtil
                .getProcessInstanceList(cluster3,
                        Util.getProcessName(bundles[1].getProcessData()), ENTITY_TYPE.PROCESS)
                .size();
        Assert.assertEquals(finalNumberOfInstances,
                getExpectedNumberOfWorkflowInstances(bundles[1]
                        .getProcessObject().getClusters().getCluster().get(0).getValidity()
                        .getStart(),
                        bundles[1].getProcessObject().getClusters().getCluster().get(0)
                                .getValidity().getEnd()));
        AssertUtil.checkNotStatus(cluster2OC, ENTITY_TYPE.PROCESS, bundles[1], Job.Status.RUNNING);
        int expectedNumberOfWorkflows =
                getExpectedNumberOfWorkflowInstances(InstanceUtil.dateToOozieDate(
                        bundles[1].getProcessObject().getClusters().getCluster().get(0)
                                .getValidity().getStart()),
                        newEndTime);
        Assert.assertEquals(Util.getNumberOfWorkflowInstances(cluster3, oldBundleId),
                expectedNumberOfWorkflows);
    }

    @Test(groups = {"multiCluster"}, timeOut = 1200000)
    public void updateProcessIncreaseValidityInEachColoWithOneProcessSuspended() throws Exception {
        String startTime = InstanceUtil.getTimeWrtSystemTime(-1);
        String endTime = InstanceUtil.getTimeWrtSystemTime(3);
        bundles[1].setProcessValidity(startTime, endTime);

        bundles[1].submitBundle(prism);
        //now to schedule in 1 colo and let it remain in another
        Util.assertSucceeded(
                cluster3.getProcessHelper()
                        .schedule(URLS.SCHEDULE_URL, bundles[1].getProcessData()));
        String oldBundleId = InstanceUtil
                .getLatestBundleID(cluster3,
                        Util.readEntityName(bundles[1].getProcessData()), ENTITY_TYPE.PROCESS);

        Thread.sleep(30000);
        waitForProcessToReachACertainState(cluster3, bundles[1], Job.Status.RUNNING);

        int coordCount = Util.getNumberOfWorkflowInstances(cluster3, oldBundleId);

        String newEndTime = InstanceUtil.addMinsToTime(InstanceUtil.dateToOozieDate(
                bundles[1].getProcessObject().getClusters().getCluster().get(0).getValidity()
                        .getEnd()), 4);
        bundles[1].setProcessValidity(InstanceUtil.dateToOozieDate(
                bundles[1].getProcessObject().getClusters().getCluster().get(0).getValidity()
                        .getStart()),
                newEndTime);

        Util.assertSucceeded(
                cluster3.getProcessHelper()
                        .suspend(URLS.SUSPEND_URL, bundles[1].getProcessData()));
        while (Util.parseResponse(
                prism.getProcessHelper()
                        .update((bundles[1].getProcessData()), bundles[1].getProcessData()))
                .getStatus() != APIResult.Status.SUCCEEDED) {
            System.out.println("update didnt SUCCEED in last attempt");
            Thread.sleep(10000);
        }
        Util.assertSucceeded(cluster3.getProcessHelper()
                .resume(URLS.RESUME_URL, bundles[1].getProcessData()));

        String prismString = dualComparison(bundles[1], cluster2);

        dualComparison(bundles[1], cluster3);
        waitingForBundleFinish(cluster3, oldBundleId);
        //ensure that the running process has new coordinators created; while the submitted
        // one is updated correctly.
        int finalNumberOfInstances = InstanceUtil
                .getProcessInstanceList(cluster3,
                        Util.getProcessName(bundles[1].getProcessData()), ENTITY_TYPE.PROCESS)
                .size();
        Assert.assertEquals(finalNumberOfInstances,
                getExpectedNumberOfWorkflowInstances(bundles[1]
                        .getProcessObject().getClusters().getCluster().get(0).getValidity()
                        .getStart(),
                        bundles[1].getProcessObject().getClusters().getCluster().get(0)
                                .getValidity().getEnd()));
        AssertUtil.checkNotStatus(cluster2OC, ENTITY_TYPE.PROCESS, bundles[1], Job.Status.RUNNING);
    }

    private void setBundleWFPath(Bundle... bundles) throws Exception {
        for (Bundle bundle : bundles) {
            bundle.setProcessWorkflow(WORKFLOW_PATH);
        }
    }

    @Test(groups = {"multiCluster"}, timeOut = 1200000)
    public void updateProcessFrequencyInEachColoWithOneProcessRunning_Daily() throws Exception {
        //set daily process
        final String START_TIME = InstanceUtil.getTimeWrtSystemTime(-20);
        String endTime = InstanceUtil.getTimeWrtSystemTime(4000);
        bundles[1].setProcessPeriodicity(1, TimeUnit.days);
        bundles[1].setOutputFeedPeriodicity(1, TimeUnit.days);
        bundles[1].setProcessValidity(START_TIME, endTime);

        bundles[1].submitBundle(prism);
        //now to schedule in 1 colo and let it remain in another
        Util.assertSucceeded(
                cluster3.getProcessHelper()
                        .schedule(URLS.SCHEDULE_URL, bundles[1].getProcessData()));
        String oldBundleId = InstanceUtil
                .getLatestBundleID(cluster3,
                        Util.readEntityName(bundles[1].getProcessData()), ENTITY_TYPE.PROCESS);

        waitForProcessToReachACertainState(cluster3, bundles[1], Job.Status.RUNNING);
        int coordCount = Util.getNumberOfWorkflowInstances(cluster3, oldBundleId);

        Util.print("original process: " + bundles[1].getProcessData());

        String updatedProcess = InstanceUtil
                .setProcessFrequency(bundles[1].getProcessData(),
                        new Frequency(5, TimeUnit.minutes));

        Util.print("updated process: " + updatedProcess);

        //now to update
        String updatedTime = new DateTime(DateTimeZone.UTC).plusMinutes(2).toString();

        ServiceResponse response =
                prism.getProcessHelper().update(updatedProcess, updatedProcess);
        Util.assertSucceeded(response);
        String prismString = dualComparison(bundles[1], cluster2);
        Assert.assertEquals(Util.getProcessObject(prismString).getFrequency(),
                new Frequency(5, TimeUnit.minutes));
        dualComparison(bundles[1], cluster3);
        //ensure that the running process has new coordinators created; while the submitted
        // one is updated
        // correctly.
        Util.verifyNewBundleCreation(cluster3, oldBundleId, coordCount,
                Util.readEntityName(bundles[1].getProcessData()), true);
        AssertUtil.checkNotStatus(cluster2OC, ENTITY_TYPE.PROCESS, bundles[1], Job.Status.RUNNING);
    }


    @Test(groups = {"multiCluster"}, timeOut = 1200000)
    public void
    updateProcessFrequencyInEachColoWithOneProcessRunning_dailyToMonthly_withStartChange()
            throws Exception {
        //set daily process
        final String START_TIME = InstanceUtil.getTimeWrtSystemTime(-20);
        String endTime = InstanceUtil.getTimeWrtSystemTime(4000 * 60);
        bundles[1].setProcessPeriodicity(1, TimeUnit.days);
        bundles[1].setOutputFeedPeriodicity(1, TimeUnit.days);
        bundles[1].setProcessValidity(START_TIME, endTime);

        bundles[1].submitBundle(prism);
        //now to schedule in 1 colo and let it remain in another
        Util.assertSucceeded(
                cluster3.getProcessHelper()
                        .schedule(URLS.SCHEDULE_URL, bundles[1].getProcessData()));
        String oldBundleId = InstanceUtil
                .getLatestBundleID(cluster3,
                        Util.readEntityName(bundles[1].getProcessData()), ENTITY_TYPE.PROCESS);

        waitForProcessToReachACertainState(cluster3, bundles[1], Job.Status.RUNNING);
        int coordCount = Util.getNumberOfWorkflowInstances(cluster3, oldBundleId);

        Util.print("original process: " + bundles[1].getProcessData());

        String updatedProcess = InstanceUtil
                .setProcessFrequency(bundles[1].getProcessData(),
                        new Frequency(1, TimeUnit.months));
        updatedProcess = InstanceUtil
                .setProcessValidity(updatedProcess, InstanceUtil.getTimeWrtSystemTime(10),
                        endTime);

        Util.print("updated process: " + updatedProcess);

        //now to update
        String updatedTime = new DateTime(DateTimeZone.UTC).plusMinutes(2).toString();


        ServiceResponse response =
                prism.getProcessHelper().update(updatedProcess, updatedProcess);
        Util.assertSucceeded(response);
        String prismString = dualComparison(bundles[1], cluster2);
        Assert.assertEquals(Util.getProcessObject(prismString).getFrequency(),
                new Frequency(1, TimeUnit.months));
        dualComparison(bundles[1], cluster3);
        //ensure that the running process has new coordinators created; while the submitted
        // one is updated correctly.
        Util.verifyNewBundleCreation(cluster3, oldBundleId, coordCount,
                Util.readEntityName(bundles[1].getProcessData()), true);
        AssertUtil.checkNotStatus(cluster2OC, ENTITY_TYPE.PROCESS, bundles[1], Job.Status.RUNNING);
    }


    @Test(groups = {"multiCluster"}, timeOut = 1200000)
    @SuppressWarnings("SleepWhileInLoop")
    public void updateProcessRollStartTimeBackwardsToPastInEachColoWithOneProcessRunning()
            throws Exception {
        bundles[1].submitBundle(prism);
        //now to schedule in 1 colo and let it remain in another
        Util.assertSucceeded(
                cluster3.getProcessHelper()
                        .schedule(URLS.SCHEDULE_URL, bundles[1].getProcessData()));
        String oldBundleId = InstanceUtil
                .getLatestBundleID(cluster3,
                        Util.readEntityName(bundles[1].getProcessData()), ENTITY_TYPE.PROCESS);
        Thread.sleep(10000);

        int coordCount = Util.getNumberOfWorkflowInstances(cluster3, oldBundleId);

        String oldStartTime = InstanceUtil.dateToOozieDate(
                bundles[1].getProcessObject().getClusters().getCluster().get(0).getValidity()
                        .getStart());
        String newStartTime = InstanceUtil.addMinsToTime(InstanceUtil.dateToOozieDate(
                bundles[1].getProcessObject().getClusters().getCluster().get(0).getValidity()
                        .getStart()), -3);
        bundles[1].setProcessValidity(newStartTime, InstanceUtil.dateToOozieDate(
                bundles[1].getProcessObject().getClusters().getCluster().get(0).getValidity()
                        .getEnd()));

        waitForProcessToReachACertainState(cluster3, bundles[1], Job.Status.RUNNING);

        Util.assertSucceeded(
                prism.getProcessHelper()
                        .update(bundles[1].getProcessData(), bundles[1].getProcessData()));

        Util.verifyNewBundleCreation(cluster3, oldBundleId, coordCount,
                Util.readEntityName(bundles[1].getProcessData()), true);

        String prismString = dualComparison(bundles[1], cluster2);

        bundles[1].verifyDependencyListing();

        dualComparison(bundles[1], cluster3);
        //ensure that the running process has new coordinators created; while the submitted
        // one is updated correctly.
        int finalNumberOfInstances =
                InstanceUtil.getProcessInstanceListFromAllBundles(cluster3,
                        Util.getProcessName(bundles[1].getProcessData()), ENTITY_TYPE.PROCESS).size();
        Assert.assertEquals(finalNumberOfInstances,
                getExpectedNumberOfWorkflowInstances(oldStartTime,
                        bundles[1].getProcessObject().getClusters().getCluster().get(0)
                                .getValidity().getEnd()));
        AssertUtil.checkNotStatus(cluster2OC, ENTITY_TYPE.PROCESS, bundles[1], Job.Status.RUNNING);
        int expectedNumberOfWorkflows = getExpectedNumberOfWorkflowInstances(newStartTime,
                bundles[1].getProcessObject().getClusters().getCluster().get(0).getValidity()
                        .getEnd());
        Assert.assertEquals(Util.getNumberOfWorkflowInstances(cluster3, oldBundleId),
                expectedNumberOfWorkflows);
    }

    @Test(groups = {"multiCluster"}, timeOut = 1200000)
    public void updateProcessRollStartTimeForwardInEachColoWithOneProcessSuspended()
            throws Exception {
        bundles[1].submitBundle(prism);
        //now to schedule in 1 colo and let it remain in another
        Util.assertSucceeded(
                cluster3.getProcessHelper()
                        .schedule(URLS.SCHEDULE_URL, bundles[1].getProcessData()));
        String oldBundleId = InstanceUtil
                .getLatestBundleID(cluster3,
                        Util.readEntityName(bundles[1].getProcessData()), ENTITY_TYPE.PROCESS);
        Thread.sleep(30000);

        int coordCount = Util.getNumberOfWorkflowInstances(cluster3, oldBundleId);
        String oldStartTime = InstanceUtil.dateToOozieDate(
                bundles[1].getProcessObject().getClusters().getCluster().get(0).getValidity()
                        .getStart());
        String newStartTime = InstanceUtil.addMinsToTime(InstanceUtil.dateToOozieDate(
                bundles[1].getProcessObject().getClusters().getCluster().get(0).getValidity()
                        .getStart()), 3);
        bundles[1].setProcessValidity(newStartTime, InstanceUtil.dateToOozieDate(
                bundles[1].getProcessObject().getClusters().getCluster().get(0).getValidity()
                        .getEnd()));

        waitForProcessToReachACertainState(cluster3, bundles[1], Job.Status.RUNNING);

        Util.assertSucceeded(
                cluster3.getProcessHelper()
                        .suspend(URLS.SUSPEND_URL, bundles[1].getProcessData()));

        Util.assertSucceeded(
                prism.getProcessHelper()
                        .update(bundles[1].getProcessData(), bundles[1].getProcessData()));

        String prismString = dualComparison(bundles[1], cluster2);

        bundles[1].verifyDependencyListing();

        dualComparison(bundles[1], cluster3);
        //ensure that the running process has new coordinators created; while the submitted
        // one is updated correctly.
        int finalNumberOfInstances =
                InstanceUtil.getProcessInstanceListFromAllBundles(cluster3,
                        Util.getProcessName(bundles[1].getProcessData()), ENTITY_TYPE.PROCESS).size();
        Assert.assertEquals(finalNumberOfInstances,
                getExpectedNumberOfWorkflowInstances(oldStartTime,
                        bundles[1].getProcessObject().getClusters().getCluster().get(0)
                                .getValidity().getEnd()));
        Assert.assertEquals(InstanceUtil
                .getProcessInstanceList(cluster3,
                        Util.readEntityName(bundles[1].getProcessData()), ENTITY_TYPE.PROCESS)
                .size(), getExpectedNumberOfWorkflowInstances(newStartTime,
                bundles[1].getProcessObject().getClusters().getCluster().get(0).getValidity()
                        .getEnd()));

        AssertUtil.checkNotStatus(cluster2OC, ENTITY_TYPE.PROCESS, bundles[1], Job.Status.RUNNING);
    }

    @Test(groups = {"multiCluster"}, timeOut = 1200000)
    public void updateProcessRollStartTimeBackwar45321ZdsInEachColoWithOneProcessSuspended()
            throws Exception {
        bundles[1].submitBundle(prism);
        //now to schedule in 1 colo and let it remain in another
        Util.assertSucceeded(
                cluster3.getProcessHelper()
                        .schedule(URLS.SCHEDULE_URL, bundles[1].getProcessData()));
        String oldBundleId = InstanceUtil
                .getLatestBundleID(cluster3,
                        Util.readEntityName(bundles[1].getProcessData()), ENTITY_TYPE.PROCESS);
        Thread.sleep(30000);

        String oldStartTime = InstanceUtil.dateToOozieDate(
                bundles[1].getProcessObject().getClusters().getCluster().get(0).getValidity()
                        .getStart());
        String newStartTime = InstanceUtil.addMinsToTime(InstanceUtil.dateToOozieDate(
                bundles[1].getProcessObject().getClusters().getCluster().get(0).getValidity()
                        .getStart()), -3);
        bundles[1].setProcessValidity(newStartTime, InstanceUtil.dateToOozieDate(
                bundles[1].getProcessObject().getClusters().getCluster().get(0).getValidity()
                        .getEnd()));

        waitForProcessToReachACertainState(cluster3, bundles[1], Job.Status.RUNNING);

        Util.assertSucceeded(
                cluster3.getProcessHelper()
                        .suspend(URLS.SUSPEND_URL, bundles[1].getProcessData()));
        Util.assertSucceeded(
                prism.getProcessHelper()
                        .update(bundles[1].getProcessData(), bundles[1].getProcessData()));
        Util.assertSucceeded(cluster3.getProcessHelper()
                .resume(URLS.RESUME_URL, bundles[1].getProcessData()));
        int coordCount = Util.getNumberOfWorkflowInstances(cluster3, oldBundleId);
        Util.verifyNewBundleCreation(cluster3, oldBundleId, coordCount,
                Util.readEntityName(bundles[1].getProcessData()), true);

        String prismString = dualComparison(bundles[1], cluster2);

        bundles[1].verifyDependencyListing();

        dualComparison(bundles[1], cluster3);
        waitingForBundleFinish(cluster3, oldBundleId);

        int finalNumberOfInstances =
                InstanceUtil.getProcessInstanceListFromAllBundles(cluster3,
                        Util.getProcessName(bundles[1].getProcessData()), ENTITY_TYPE.PROCESS).size();
        Assert.assertEquals(finalNumberOfInstances,
                getExpectedNumberOfWorkflowInstances(oldStartTime,
                        bundles[1].getProcessObject().getClusters().getCluster().get(0)
                                .getValidity().getEnd()));
        AssertUtil.checkNotStatus(cluster2OC, ENTITY_TYPE.PROCESS, bundles[1], Job.Status.RUNNING);
    }

    private String setProcessTimeOut(String process, int mag, TimeUnit unit) throws Exception {
        Process p = InstanceUtil.getProcessElement(process);
        Frequency f = new Frequency(mag, unit);
        p.setTimeout(f);
        return InstanceUtil.processToString(p);
    }

    public ServiceResponse updateProcessConcurrency(Bundle bundle, int concurrency)
            throws Exception {
        String oldData = new String(bundle.getProcessData());
        Process updatedProcess = bundle.getProcessObject();
        updatedProcess.setParallel(concurrency);

        return prism.getProcessHelper()
                .update(oldData, prism.getProcessHelper().toString(updatedProcess));
    }

    private String dualComparison(Bundle bundle, ColoHelper coloHelper) throws Exception {
        String prismResponse = getResponse(prism, bundle, true);
        String coloResponse = getResponse(coloHelper, bundle, true);
        AssertJUnit.assertEquals(prismResponse, coloResponse);
        return getResponse(prism, bundle, true);
    }

    private void dualComparisonFailure(Bundle bundle, ColoHelper coloHelper) throws Exception {
        AssertJUnit.assertNotSame(getResponse(prism, bundle, true),
                getResponse(coloHelper, bundle, true));
    }

    private String getResponse(PrismHelper prism, Bundle bundle, boolean bool)
            throws Exception {
        ServiceResponse response = prism.getProcessHelper()
                .getEntityDefinition(Util.URLS.GET_ENTITY_DEFINITION, bundle.getProcessData());
        if (bool)
            Util.assertSucceeded(response);
        else
            Util.assertFailed(response);
        String result = response.getMessage();
        Assert.assertNotNull(result);

        return result;

    }



    private void waitForProcessToReachACertainState(ColoHelper coloHelper, Bundle bundle,
                                                    Job.Status state)
            throws Exception {

        while (Util.getOozieJobStatus(coloHelper.getFeedHelper().getOozieClient(),
                Util.readEntityName(bundle.getProcessData()), ENTITY_TYPE.PROCESS) != state) {
            //keep waiting
            Thread.sleep(10000);
        }

        //now check if the coordinator is in desired state
        CoordinatorJob coord = getDefaultOozieCoord(coloHelper, InstanceUtil
                .getLatestBundleID(coloHelper, Util.readEntityName(bundle.getProcessData()),
                        ENTITY_TYPE.PROCESS));

        while (coord.getStatus() != state) {
            Thread.sleep(10000);
            coord = getDefaultOozieCoord(coloHelper, InstanceUtil
                    .getLatestBundleID(coloHelper, Util.readEntityName(bundle.getProcessData()),
                            ENTITY_TYPE.PROCESS));
        }
    }

    private Bundle usualGrind(PrismHelper prism, Bundle b) throws Exception {
        System.setProperty("java.security.krb5.realm", "");
        System.setProperty("java.security.krb5.kdc", "");
        b.setInputFeedDataPath(inputFeedPath);
        String prefix = b.getFeedDataPathPrefix();
        HadoopUtil.deleteDirIfExists(prefix.substring(1), cluster1FS);
        Util.lateDataReplenish(prism, 60, 1, prefix);
        final String START_TIME = InstanceUtil.getTimeWrtSystemTime(-2);
        String endTime = InstanceUtil.getTimeWrtSystemTime(6);
        b.setProcessPeriodicity(1, TimeUnit.minutes);
        b.setOutputFeedPeriodicity(1, TimeUnit.minutes);
        b.setProcessValidity(START_TIME, endTime);
        return b;
    }

    private ExecutionType getRandomExecutionType(Bundle bundle) throws Exception {
        ExecutionType current = bundle.getProcessObject().getOrder();
        Random r = new Random();
        ExecutionType[] values = ExecutionType.values();
        int i = 0;
        do {

            i = r.nextInt(values.length);
        } while (current.equals(values[i]));
        return values[i];
    }

    public ServiceResponse updateProcessFrequency(Bundle bundle,
                                                  org.apache.falcon.regression.core.generated.dependencies.Frequency frequency)
            throws Exception {
        String oldData = new String(bundle.getProcessData());
        Process updatedProcess = bundle.getProcessObject();
        updatedProcess.setFrequency(frequency);
        return prism.getProcessHelper()
                .update(oldData, prism.getProcessHelper().toString(updatedProcess));
    }

    //need to expand this function more later
    private int getExpectedNumberOfWorkflowInstances(String start, String end) throws Exception {
        DateTime startDate = new DateTime(start);
        DateTime endDate = new DateTime(end);
        Minutes minutes = Minutes.minutesBetween((startDate), (endDate));
        return minutes.getMinutes();
    }

    private int getExpectedNumberOfWorkflowInstances(Date start, Date end) throws Exception {
        DateTime startDate = new DateTime(start);
        DateTime endDate = new DateTime(end);
        Minutes minutes = Minutes.minutesBetween((startDate), (endDate));
        return minutes.getMinutes();
    }

    private int getExpectedNumberOfWorkflowInstances(String start, Date end) throws Exception {
        DateTime startDate = new DateTime(start);
        DateTime endDate = new DateTime(end);
        Minutes minutes = Minutes.minutesBetween((startDate), (endDate));
        return minutes.getMinutes();
    }

    private Frequency getRandomFrequency(Bundle bundle) throws Exception {
        org.apache.falcon.regression.core.generated.dependencies.Frequency current =
                bundle.getProcessObject().getFrequency();
        Random r = new Random();
        TimeUnit[] values = TimeUnit.values();
        int i = 0;
        do {
            i = r.nextInt(values.length);
        } while (current.getTimeUnit().equals(values[i]));
        return new Frequency(current.getFrequency() + 1, values[i]);
    }

    private void waitingForBundleFinish(ColoHelper coloHelper, String bundleId, int minutes)
            throws Exception {
        int wait = 0;
        while (!Util.isBundleOver(coloHelper, bundleId)) {
            //keep waiting
            System.out.println("bundle not over .. waiting");
            Thread.sleep(60000);
            wait++;
            if (wait == minutes) {
                Assert.assertTrue(false);
                break;
            }
        }
    }

    private void waitingForBundleFinish(ColoHelper coloHelper, String bundleId) throws Exception {
        int wait = 0;
        while (!Util.isBundleOver(coloHelper, bundleId)) {
            //keep waiting
            System.out.println("bundle not over .. waiting, bundleId: " + bundleId);
            Thread.sleep(60000);
            wait++;
            if (wait == 15) {
                Assert.assertTrue(false);
                break;
            }
        }
    }

    private CoordinatorJob getDefaultOozieCoord(ColoHelper coloHelper, String bundleId)
            throws Exception {
        XOozieClient client = new XOozieClient(coloHelper.getFeedHelper().getOozieURL());
        BundleJob bundlejob = client.getBundleJobInfo(bundleId);

        for (CoordinatorJob coord : bundlejob.getCoordinators()) {
            if (coord.getAppName().contains("DEFAULT")) {
                return client.getCoordJobInfo(coord.getId());
            }
        }
        return null;
    }

}
