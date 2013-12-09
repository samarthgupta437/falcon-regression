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

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
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
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.core.util.Util.URLS;
import org.apache.oozie.client.BundleJob;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.Job.Status;
import org.apache.oozie.client.XOozieClient;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Minutes;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.TestNGException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.Date;
import java.util.Random;

public class NewPrismProcessUpdateTest {

    @BeforeMethod(alwaysRun = true)
    public void testName(Method method) {
        Util.print("test name: " + method.getName());
    }


    PrismHelper prismHelper = new PrismHelper("prism.properties");
    ColoHelper UA1ColoHelper = new ColoHelper("mk-qa.config.properties");
    ColoHelper UA2ColoHelper = new ColoHelper("ivoryqa-1.config.properties");
    ColoHelper UA3ColoHelper = new ColoHelper("gs1001.config.properties");
    DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy/MM/dd/HH/mm");

    @Test(dataProvider = "DP", groups = {"multiCluster"}, dataProviderClass = Bundle.class,
            timeOut = 1200000)
    public void updateProcessFrequencyInEachColoWithOneProcessRunning_Monthly(Bundle bundle)
    throws Exception {
        Bundle UA1Bundle = new Bundle(bundle, UA1ColoHelper);
        Bundle UA2Bundle = new Bundle(bundle, UA2ColoHelper);
        Bundle UA3Bundle = new Bundle(bundle, UA3ColoHelper);
        try {
            UA2Bundle.addClusterToBundle(UA3Bundle.getClusters().get(0), ClusterType.TARGET);
            //UA2Bundle.setProcessWorkflow("/examples/apps/phailFs/workflow.xml");
            usualGrind(UA3ColoHelper, UA2Bundle);

            final String START_TIME = InstanceUtil.getTimeWrtSystemTime(-20);
            String endTime = InstanceUtil.getTimeWrtSystemTime(4000 * 60);
            UA2Bundle.setProcessPeriodicity(1, TimeUnit.months);
            UA2Bundle.setOutputFeedPeriodicity(1, TimeUnit.months);
            UA2Bundle.setProcessValidity(START_TIME, endTime);

            //UA2Bundle.generateUniqueBundle();
            UA2Bundle.submitBundle(prismHelper);
            //now to schedule in 1 colo and let it remain in another
            Util.assertSucceeded(
                    UA3ColoHelper.getProcessHelper()
                            .schedule(URLS.SCHEDULE_URL, UA2Bundle.getProcessData()));
            String oldBundleId = InstanceUtil
                    .getLatestBundleID(UA3ColoHelper,
                            Util.readEntityName(UA2Bundle.getProcessData()), "PROCESS");


            waitForProcessToReachACertainState(UA3ColoHelper, UA2Bundle, "RUNNING");
            int coordCount = Util.getNumberOfWorkflowInstances(UA3ColoHelper, oldBundleId);

            //UA2Bundle.getProcessObject().setFrequency(getRandomFrequency(UA2Bundle));
            String updatedProcess = InstanceUtil
                    .setProcessFrequency(UA2Bundle.getProcessData(),
                            new Frequency(5, TimeUnit.minutes));

            Util.print("updated process: " + updatedProcess);

            //now to update
            String updatedTime = new DateTime(DateTimeZone.UTC).plusMinutes(2).toString();
            while (Util
                    .parseResponse(prismHelper.getProcessHelper()
                            .update((UA2Bundle.getProcessData()), updatedProcess))
                    .getStatus() != APIResult.Status.SUCCEEDED) {
                System.out.println("update didnt SUCCEED in last attempt");
                Thread.sleep(10000);
            }

            String prismString = getResponse(prismHelper, UA2Bundle, true);
            Assert.assertEquals(Util.getProcessObject(prismString).getFrequency(),
                    Util.getProcessObject(updatedProcess).getFrequency());
            Thread.sleep(60000);
            dualComparison(UA2Bundle, UA3ColoHelper);
            //ensure that the running process has new coordinators created; while the submitted
            // one is updated
            // correctly.
            verifyNewBundleCreation(UA3ColoHelper, oldBundleId, coordCount,
                    Util.readEntityName(UA2Bundle.getProcessData()), true);
            //	Util.verifyNoJobsFoundInOozie(Util.getOozieJobStatus(UA3ColoHelper,
            // Util.readEntityName(UA2Bundle.getProcessData()), "RUNNING"));
            waitingForBundleFinish(UA3ColoHelper, oldBundleId, 5);
            int finalNumberOfInstances =
                    InstanceUtil.getProcessInstanceListFromAllBundles(UA3ColoHelper,
                            Util.getProcessName(UA2Bundle.getProcessData()), "PROCESS").size();

            int expectedInstances = getExpectedNumberOfWorkflowInstances(
                    UA2Bundle.getProcessObject().getClusters().getCluster().get(0).getValidity()
                            .getStart(),
                    UA2Bundle.getProcessObject().getClusters().getCluster().get(0).getValidity()
                            .getEnd());

            Assert.assertEquals(finalNumberOfInstances, expectedInstances,
                    "number of instances doesnt match :(");
        } finally {
            UA1Bundle.deleteBundle(prismHelper);
            UA2Bundle.deleteBundle(prismHelper);
            UA3Bundle.deleteBundle(prismHelper);

        }

    }

    @Test(groups = {"multiCluster"}, timeOut = 1200000)
    @SuppressWarnings("SleepWhileInLoop")
    public void updateProcessRollStartTimeForwardInEachColoWithOneProcessRunning()
    throws Exception {
        Bundle b = (Bundle) Util.readELBundles()[0][0];
        b.generateUniqueBundle();
        Bundle UA1Bundle = new Bundle(b, UA1ColoHelper);
        Bundle UA2Bundle = new Bundle(b, UA2ColoHelper);
        Bundle UA3Bundle = new Bundle(b, UA3ColoHelper);
        try {
            UA2Bundle.addClusterToBundle(UA3Bundle.getClusters().get(0), ClusterType.TARGET);
            UA2Bundle.setProcessWorkflow("/examples/apps/phailFs/workflow.xml");
            usualGrind(UA3ColoHelper, UA2Bundle);

            //UA2Bundle.generateUniqueBundle();
            UA2Bundle.submitBundle(prismHelper);
            //now to schedule in 1 colo and let it remain in another
            Util.assertSucceeded(
                    UA3ColoHelper.getProcessHelper()
                            .schedule(URLS.SCHEDULE_URL, UA2Bundle.getProcessData()));
            String oldBundleId = InstanceUtil
                    .getLatestBundleID(UA3ColoHelper,
                            Util.readEntityName(UA2Bundle.getProcessData()), "PROCESS");

            Thread.sleep(10000);

            int coordCount = Util.getNumberOfWorkflowInstances(UA3ColoHelper, oldBundleId);

            //    String newStartTime = instanceUtil.addMinsToTime(UA2Bundle.getProcessObject()
            // .getClusters()
            // .getCluster().get(0).getValidity().getStart(), 3);


            String newStartTime = InstanceUtil.addMinsToTime(InstanceUtil.dateToOozieDate(
                    UA2Bundle.getProcessObject().getClusters().getCluster().get(0).getValidity()
                            .getStart()), 20);
            String newEndTime = InstanceUtil.addMinsToTime(InstanceUtil.dateToOozieDate(
                    UA2Bundle.getProcessObject().getClusters().getCluster().get(0).getValidity()
                            .getStart()), 30);

            UA2Bundle.setProcessValidity(newStartTime, newEndTime);

            waitForProcessToReachACertainState(UA3ColoHelper, UA2Bundle, "RUNNING");

            //Util.assertSucceeded(prismHelper.getProcessHelper().update(UA2Bundle.getProcessData(),
            // UA2Bundle.getProcessData()));
            System.out.println("updated process: " + UA2Bundle.getProcessData());
            while (Util.parseResponse(
                    prismHelper.getProcessHelper()
                            .update(UA2Bundle.getProcessData(), UA2Bundle.getProcessData()))
                    .getStatus() != APIResult.Status.SUCCEEDED) {
                System.out.println("update didnt SUCCEED in last attempt");
                Thread.sleep(10000);
            }

            verifyNewBundleCreation(UA3ColoHelper, oldBundleId, coordCount,
                    Util.readEntityName(UA2Bundle.getProcessData()), true);

            String prismString = getResponse(prismHelper, UA2Bundle, true);

            //UA2Bundle.verifyDependencyListing();

            dualComparison(UA2Bundle, UA3ColoHelper);
            while (!Util.isBundleOver(UA3ColoHelper, oldBundleId)) {
            }
            //ensure that the running process has new coordinators created; while the submitted
            // one is updated
            // correctly.
            int finalNumberOfInstances =
                    InstanceUtil.getProcessInstanceListFromAllBundles(UA3ColoHelper,
                            Util.getProcessName(UA2Bundle.getProcessData()), "PROCESS").size();
            Assert.assertEquals(finalNumberOfInstances,
                    getExpectedNumberOfWorkflowInstances(InstanceUtil
                            .dateToOozieDate(
                                    UA2Bundle.getProcessObject().getClusters().getCluster().get(0)
                                            .getValidity().getStart()),
                            InstanceUtil
                                    .dateToOozieDate(
                                            UA2Bundle.getProcessObject().getClusters().getCluster()
                                                    .get(0).getValidity()
                                                    .getEnd())));
            Util.verifyNoJobsFoundInOozie(
                    Util.getOozieJobStatus(UA2ColoHelper,
                            Util.readEntityName(UA2Bundle.getProcessData()), "RUNNING"));

            int expectedNumberOfWorkflows =
                    getExpectedNumberOfWorkflowInstances(newStartTime, InstanceUtil
                            .dateToOozieDate(
                                    UA2Bundle.getProcessObject().getClusters().getCluster().get(0)
                                            .getValidity().getEnd()));
            Assert.assertEquals(Util.getNumberOfWorkflowInstances(UA3ColoHelper, oldBundleId),
                    expectedNumberOfWorkflows);
        } finally {
            UA1Bundle.deleteBundle(prismHelper);
            UA2Bundle.deleteBundle(prismHelper);
            UA3Bundle.deleteBundle(prismHelper);

        }
    }

    @Test(dataProvider = "DP", groups = {"multiCluster"}, dataProviderClass = Bundle.class,
            timeOut = 1800000)
    public void updateProcessConcurrencyWorkflowExecutionInEachColoWithOneColoDown(Bundle bundle)
    throws Exception {
        Bundle b = (Bundle) Bundle.readBundle("src/test/resources/updateBundle")[0][0];
        b.generateUniqueBundle();
        Bundle UA2Bundle = new Bundle(b, UA2ColoHelper);
        Bundle UA3Bundle = new Bundle(b, UA3ColoHelper);

        try {

            UA2Bundle.addClusterToBundle(UA3Bundle.getClusters().get(0), ClusterType.TARGET);
            UA2Bundle.setProcessWorkflow("/examples/apps/phailFs/workflow.xml");
            usualGrind(UA3ColoHelper, UA2Bundle);

            //UA2Bundle.generateUniqueBundle();
            UA2Bundle.submitBundle(prismHelper);
            //now to schedule in 1 colo and let it remain in another
            Util.assertSucceeded(
                    UA3ColoHelper.getProcessHelper()
                            .schedule(URLS.SCHEDULE_URL, UA2Bundle.getProcessData()));
            String oldBundleId = InstanceUtil
                    .getLatestBundleID(UA3ColoHelper,
                            Util.readEntityName(UA2Bundle.getProcessData()), "PROCESS");

            Thread.sleep(25000);

            int coordCount = Util.getNumberOfWorkflowInstances(UA3ColoHelper, oldBundleId);

            int initialConcurrency = UA2Bundle.getProcessObject().getParallel();

            UA2Bundle.setProcessConcurrency(UA2Bundle.getProcessObject().getParallel() + 3);
            UA2Bundle.setProcessWorkflow("/examples/apps/aggregator1");
            UA2Bundle.getProcessObject().setOrder(getRandomExecutionType(UA2Bundle));
            //suspend
            Util.shutDownService(UA3ColoHelper.getProcessHelper());
            //Thread.sleep(30000);

            Util.assertPartialSucceeded(
                    prismHelper.getProcessHelper()
                            .update(UA2Bundle.getProcessData(), UA2Bundle.getProcessData()));
            //now to update


            String prismString = getResponse(prismHelper, UA2Bundle, true);
            Assert.assertEquals(Util.getProcessObject(prismString).getParallel(),
                    initialConcurrency);
            Assert.assertEquals(Util.getProcessObject(prismString).getWorkflow().getPath(),
                    "/examples/apps/phailFs/workflow.xml");
            Assert.assertEquals(Util.getProcessObject(prismString).getOrder(),
                    UA2Bundle.getProcessObject().getOrder());

            String coloString = getResponse(UA2ColoHelper, UA2Bundle, true);
            Assert.assertEquals(Util.getProcessObject(coloString).getWorkflow().getPath(),
                    "/examples/apps/aggregator1");

            Util.startService(UA3ColoHelper.getProcessHelper());

            dualComparisonFailure(UA2Bundle, UA3ColoHelper);
            //ensure that the running process has new coordinators created; while the submitted
            // one is updated
            // correctly.
            Util.verifyNoJobsFoundInOozie(
                    Util.getOozieJobStatus(UA2ColoHelper,
                            Util.readEntityName(UA2Bundle.getProcessData()), "RUNNING"));


            waitForProcessToReachACertainState(UA3ColoHelper, UA2Bundle, "RUNNING");

            while (Util.parseResponse(
                    prismHelper.getProcessHelper()
                            .update(UA2Bundle.getProcessData(), UA2Bundle.getProcessData()))
                    .getStatus() != APIResult.Status.SUCCEEDED) {
                System.out.println("WARNING: update did not scceed, retyring ");
                Thread.sleep(20000);
            }

            prismString = getResponse(prismHelper, UA2Bundle, true);
            Assert.assertEquals(Util.getProcessObject(prismString).getParallel(),
                    initialConcurrency + 3);
            Assert.assertEquals(Util.getProcessObject(prismString).getWorkflow().getPath(),
                    "/examples/apps/aggregator1");
            Assert.assertEquals(Util.getProcessObject(prismString).getOrder(),
                    UA2Bundle.getProcessObject().getOrder());
            dualComparison(UA2Bundle, UA3ColoHelper);
            Util.verifyNoJobsFoundInOozie(
                    Util.getOozieJobStatus(UA2ColoHelper,
                            Util.readEntityName(UA2Bundle.getProcessData()), "RUNNING"));
            waitingForBundleFinish(UA3ColoHelper, oldBundleId);
            int finalNumberOfInstances =
                    InstanceUtil.getProcessInstanceListFromAllBundles(UA3ColoHelper,
                            Util.getProcessName(UA2Bundle.getProcessData()), "PROCESS").size();

            int expectedInstances =
                    getExpectedNumberOfWorkflowInstances(InstanceUtil.dateToOozieDate(
                            UA2Bundle.getProcessObject().getClusters().getCluster().get(0)
                                    .getValidity().getStart()),
                            InstanceUtil
                                    .dateToOozieDate(
                                            UA2Bundle.getProcessObject().getClusters().getCluster()
                                                    .get(0).getValidity()
                                                    .getEnd()));

            Assert.assertEquals(finalNumberOfInstances, expectedInstances,
                    "number of instances doesnt match :(");

        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getCause());
        } finally {

            Util.restartService(UA3ColoHelper.getClusterHelper());
            UA2Bundle.deleteBundle(prismHelper);
            UA3Bundle.deleteBundle(prismHelper);
        }
    }


    @Test(groups = {"multiCluster"}, timeOut = 1200000)
    public void updateProcessFrequencyInEachColoWithOneProcessRunning() throws Exception {
        Bundle b = (Bundle) Bundle.readBundle("src/test/resources/updateBundle")[0][0];
        b.generateUniqueBundle();
        Bundle UA2Bundle = new Bundle(b, UA2ColoHelper);
        Bundle UA3Bundle = new Bundle(b, UA3ColoHelper);
        try {
            UA2Bundle.addClusterToBundle(UA3Bundle.getClusters().get(0), ClusterType.TARGET);

            usualGrind(UA3ColoHelper, UA2Bundle);

            String startTime = InstanceUtil.getTimeWrtSystemTime(-2);
            String endTime = InstanceUtil.getTimeWrtSystemTime(20);
            UA2Bundle.setProcessValidity(startTime, endTime);

            //UA2Bundle.generateUniqueBundle();
            UA2Bundle.submitBundle(prismHelper);
            //now to schedule in 1 colo and let it remain in another
            Util.assertSucceeded(
                    UA3ColoHelper.getProcessHelper()
                            .schedule(URLS.SCHEDULE_URL, UA2Bundle.getProcessData()));
            String oldBundleId = InstanceUtil
                    .getLatestBundleID(UA3ColoHelper,
                            Util.readEntityName(UA2Bundle.getProcessData()), "PROCESS");


            waitForProcessToReachACertainState(UA3ColoHelper, UA2Bundle, "RUNNING");
            int coordCount = Util.getNumberOfWorkflowInstances(UA3ColoHelper, oldBundleId);

            Util.print("original process: " + UA2Bundle.getProcessData());

            String updatedProcess = InstanceUtil
                    .setProcessFrequency(UA2Bundle.getProcessData(),
                            new Frequency(7, TimeUnit.minutes));

            Util.print("updated process: " + updatedProcess);

            //now to update
            String updatedTime = new DateTime(DateTimeZone.UTC).plusMinutes(2).toString();


            ServiceResponse response =
                    prismHelper.getProcessHelper().update(updatedProcess, updatedProcess);
            Util.assertSucceeded(response);
            String prismString = getResponse(prismHelper, UA2Bundle, true);
            Assert.assertEquals(Util.getProcessObject(prismString).getFrequency(),
                    Util.getProcessObject(updatedProcess).getFrequency());
            dualComparison(UA2Bundle, UA3ColoHelper);
            //ensure that the running process has new coordinators created; while the submitted
            // one is updated
            // correctly.
            verifyNewBundleCreation(UA3ColoHelper, oldBundleId, coordCount,
                    Util.readEntityName(UA2Bundle.getProcessData()), true);
            Util.verifyNoJobsFoundInOozie(
                    Util.getOozieJobStatus(UA2ColoHelper,
                            Util.readEntityName(UA2Bundle.getProcessData()), "RUNNING"));
        } finally {
            UA2Bundle.deleteBundle(prismHelper);
            UA3Bundle.deleteBundle(prismHelper);

        }

    }


    @Test(dataProvider = "DP", groups = {"multiCluster"}, dataProviderClass = Bundle.class,
            timeOut = 1200000)
    public void updateProcessNameInEachColoWithOneProcessRunning(Bundle bundle) throws Exception {

        System.out.println("executing: updateProcessNameInEachColoWithOneProcessRunning");
        Bundle UA1Bundle = new Bundle(bundle, UA1ColoHelper);
        Bundle UA2Bundle = new Bundle(bundle, UA2ColoHelper);
        Bundle UA3Bundle = new Bundle(bundle, UA3ColoHelper);
        try {
            UA2Bundle.addClusterToBundle(UA3Bundle.getClusters().get(0), ClusterType.TARGET);
            UA2Bundle.setProcessWorkflow("/examples/apps/phailFs/workflow.xml");
            usualGrind(UA3ColoHelper, UA2Bundle);

            //UA2Bundle.generateUniqueBundle();
            UA2Bundle.submitBundle(prismHelper);
            //now to schedule in 1 colo and let it remain in another
            Util.assertSucceeded(
                    UA3ColoHelper.getProcessHelper()
                            .schedule(URLS.SCHEDULE_URL, UA2Bundle.getProcessData()));
            String oldBundleId = InstanceUtil
                    .getLatestBundleID(UA3ColoHelper,
                            Util.readEntityName(UA2Bundle.getProcessData()), "PROCESS");


            waitForProcessToReachACertainState(UA3ColoHelper, UA2Bundle, "RUNNING");
            Thread.sleep(20000);
            int coordCount = Util.getNumberOfWorkflowInstances(UA3ColoHelper, oldBundleId);

            String oldName = new String(UA2Bundle.getProcessObject().getName());
            UA2Bundle.setProcessName("myNewProcessName");


            //now to update
            ServiceResponse response =
                    prismHelper.getProcessHelper()
                            .update((UA2Bundle.getProcessData()), UA2Bundle.getProcessData());
            Util.assertFailed(response);
            String prismString = getResponse(prismHelper, UA2Bundle, false);
            Assert.assertEquals(Util.getProcessObject(prismString).getName(), oldName);
            dualComparison(UA2Bundle, UA3ColoHelper);
            //ensure that the running process has new coordinators created; while the submitted
            // one is updated
            // correctly.
            verifyNewBundleCreation(UA3ColoHelper, oldBundleId, coordCount,
                    Util.readEntityName(UA2Bundle.getProcessData()), false);
            Util.verifyNoJobsFoundInOozie(
                    Util.getOozieJobStatus(UA2ColoHelper,
                            Util.readEntityName(UA2Bundle.getProcessData()), "RUNNING"));
        } finally {
            UA1Bundle.deleteBundle(prismHelper);
            UA2Bundle.deleteBundle(prismHelper);
            UA3Bundle.deleteBundle(prismHelper);

        }
    }

    @Test(dataProvider = "DP", groups = {"multiCluster"}, dataProviderClass = Bundle.class,
            timeOut = 1200000)
    public void updateProcessConcurrencyInEachColoWithOneProcessRunning(Bundle bundle)
    throws Exception {
        Bundle b = new Bundle();
        b = (Bundle) Bundle.readBundle("src/test/resources/updateBundle")[0][0];
        b.generateUniqueBundle();
        Bundle UA2Bundle = new Bundle(b, UA2ColoHelper);
        Bundle UA3Bundle = new Bundle(b, UA3ColoHelper);

        try {
            UA2Bundle.addClusterToBundle(UA3Bundle.getClusters().get(0), ClusterType.TARGET);
            //UA2Bundle.setProcessWorkflow("/examples/apps/phailFs/workflow.xml");
            usualGrind(UA3ColoHelper, UA2Bundle);

            String startTime = InstanceUtil.getTimeWrtSystemTime(-2);
            String endTime = InstanceUtil.getTimeWrtSystemTime(10);
            UA2Bundle.setProcessValidity(startTime, endTime);

            //UA2Bundle.generateUniqueBundle();
            UA2Bundle.submitBundle(prismHelper);
            //now to schedule in 1 colo and let it remain in another
            Util.assertSucceeded(
                    UA3ColoHelper.getProcessHelper()
                            .schedule(URLS.SCHEDULE_URL, UA2Bundle.getProcessData()));

            String oldBundleId = InstanceUtil
                    .getLatestBundleID(UA3ColoHelper,
                            Util.readEntityName(UA2Bundle.getProcessData()), "PROCESS");


            waitForProcessToReachACertainState(UA3ColoHelper, UA2Bundle, "RUNNING");


            //now to update
            DateTime updateTime = new DateTime(DateTimeZone.UTC);
            Thread.sleep(60000);

            System.out.println("updating at " + updateTime);
            while (Util
                    .parseResponse(updateProcessConcurrency(UA2Bundle,
                            UA2Bundle.getProcessObject().getParallel() + 3))
                    .getStatus() != APIResult.Status.SUCCEEDED) {
                System.out.println("WARNING: update did not scceed, retyring ");
                Thread.sleep(20000);
            }

            String prismString = getResponse(prismHelper, UA2Bundle, true);
            Assert.assertEquals(Util.getProcessObject(prismString).getParallel(),
                    UA2Bundle.getProcessObject().getParallel() + 3);
            dualComparison(UA2Bundle, UA3ColoHelper);
            //ensure that the running process has new coordinators created; while the submitted
            // one is updated
            // correctly.
            verifyNewBundleCreation(UA3ColoHelper, oldBundleId, 0,
                    Util.readEntityName(UA2Bundle.getProcessData()),
                    false);
            Util.verifyNoJobsFoundInOozie(
                    Util.getOozieJobStatus(UA2ColoHelper, Util.readEntityName(UA2Bundle
                            .getProcessData()), "RUNNING"));

            String status =
                    Util.getOozieJobStatus(UA3ColoHelper,
                            Util.readEntityName(UA2Bundle.getProcessData())).get(0);

            boolean doesExist = false;
            while (!status.equalsIgnoreCase("SUCCEEDED") || !status.equalsIgnoreCase("FAILED") ||
                    !status.equalsIgnoreCase("DONEWITHERROR")) {
                int statusCount = InstanceUtil
                        .getInstanceCountWithStatus(UA3ColoHelper,
                                Util.readEntityName(UA2Bundle.getProcessData()),
                                org.apache.oozie.client.CoordinatorAction.Status.RUNNING,
                                "PROCESS");
                if (statusCount == UA2Bundle.getProcessObject().getParallel() + 3) {
                    doesExist = true;
                    break;
                }
                status = Util.getOozieJobStatus(UA3ColoHelper,
                        Util.readEntityName(UA2Bundle.getProcessData())).get(0);
                if (status.contains("No Jobs match your criteria!"))
                    Assert.assertTrue(false,
                            "message No Jobs match your criteria! should not been there");
                Thread.sleep(30000);
            }

            Assert.assertTrue(doesExist, "Er! The desired concurrency levels are never reached!!!");
            int expectedNumberOfInstances =
                    getExpectedNumberOfWorkflowInstances(InstanceUtil.dateToOozieDate(
                            UA2Bundle.getProcessObject().getClusters().getCluster().get(0)
                                    .getValidity().getStart()),
                            InstanceUtil
                                    .dateToOozieDate(
                                            UA2Bundle.getProcessObject().getClusters().getCluster()
                                                    .get(0).getValidity()
                                                    .getEnd()));
            Assert.assertEquals(Util.getNumberOfWorkflowInstances(UA3ColoHelper, oldBundleId),
                    expectedNumberOfInstances);
        } finally {
            UA2Bundle.deleteBundle(prismHelper);
            UA3Bundle.deleteBundle(prismHelper);

        }

    }


    @Test(groups = {"multiCluster"}, timeOut = 1200000)
    @SuppressWarnings("SleepWhileInLoop")
    public void updateProcessIncreaseValidityInEachColoWithOneProcessRunning() throws Exception {
        Bundle b = (Bundle) Bundle.readBundle("src/test/resources/updateBundle")[0][0];
        b.generateUniqueBundle();
        Bundle UA1Bundle = new Bundle(b, UA1ColoHelper);
        Bundle UA2Bundle = new Bundle(b, UA2ColoHelper);
        Bundle UA3Bundle = new Bundle(b, UA3ColoHelper);
        try {
            UA2Bundle.addClusterToBundle(UA3Bundle.getClusters().get(0), ClusterType.TARGET);
            UA2Bundle.setProcessWorkflow("/examples/apps/phailFs/workflow.xml");
            usualGrind(UA3ColoHelper, UA2Bundle);

            //UA2Bundle.generateUniqueBundle();
            UA2Bundle.submitBundle(prismHelper);
            //now to schedule in 1 colo and let it remain in another
            Util.assertSucceeded(
                    UA3ColoHelper.getProcessHelper()
                            .schedule(URLS.SCHEDULE_URL, UA2Bundle.getProcessData()));
            String oldBundleId = InstanceUtil
                    .getLatestBundleID(UA3ColoHelper,
                            Util.readEntityName(UA2Bundle.getProcessData()), "PROCESS");

            Thread.sleep(15000);

            int coordCount = Util.getNumberOfWorkflowInstances(UA3ColoHelper, oldBundleId);

            String newEndTime = InstanceUtil.addMinsToTime(InstanceUtil.dateToOozieDate(
                    UA2Bundle.getProcessObject().getClusters().getCluster().get(0).getValidity()
                            .getEnd()), 4);
            UA2Bundle.setProcessValidity(InstanceUtil.dateToOozieDate(
                    UA2Bundle.getProcessObject().getClusters().getCluster().get(0).getValidity()
                            .getStart()),
                    newEndTime);

            //Thread.sleep(60000);
            waitForProcessToReachACertainState(UA3ColoHelper, UA2Bundle, "RUNNING");

            while (Util.parseResponse(
                    prismHelper.getProcessHelper()
                            .update(UA2Bundle.getProcessData(), UA2Bundle.getProcessData()))
                    .getStatus() != APIResult.Status.SUCCEEDED) {
            }

            verifyNewBundleCreation(UA3ColoHelper, oldBundleId, coordCount,
                    Util.readEntityName(UA2Bundle.getProcessData()), false);

            int i = 0;

            while (Util.getNumberOfWorkflowInstances(UA3ColoHelper, oldBundleId)
                    != getExpectedNumberOfWorkflowInstances(InstanceUtil.dateToOozieDate(
                    UA2Bundle.getProcessObject().getClusters().getCluster().get(0).getValidity()
                            .getStart()),
                    InstanceUtil
                            .dateToOozieDate(
                                    UA2Bundle.getProcessObject().getClusters().getCluster().get(0)
                                            .getValidity()
                                            .getEnd()))
                    && i < 10) {
                Thread.sleep(1000);
                i++;
            }

            String prismString = getResponse(prismHelper, UA2Bundle, true);

            UA2Bundle.verifyDependencyListing();

            dualComparison(UA2Bundle, UA3ColoHelper);
            waitingForBundleFinish(UA3ColoHelper, oldBundleId);
            //ensure that the running process has new coordinators created; while the submitted
            // one is updated
            // correctly.
            int finalNumberOfInstances = InstanceUtil
                    .getProcessInstanceList(UA3ColoHelper,
                            Util.getProcessName(UA2Bundle.getProcessData()), "PROCESS")
                    .size();
            Assert.assertEquals(finalNumberOfInstances,
                    getExpectedNumberOfWorkflowInstances(InstanceUtil
                            .dateToOozieDate(
                                    UA2Bundle.getProcessObject().getClusters().getCluster().get(0)
                                            .getValidity().getStart()),
                            InstanceUtil
                                    .dateToOozieDate(
                                            UA2Bundle.getProcessObject().getClusters().getCluster()
                                                    .get(0).getValidity()
                                                    .getEnd())));
            Util.verifyNoJobsFoundInOozie(
                    Util.getOozieJobStatus(UA2ColoHelper,
                            Util.readEntityName(UA2Bundle.getProcessData()), "RUNNING"));

        } finally {
            UA1Bundle.deleteBundle(prismHelper);
            UA2Bundle.deleteBundle(prismHelper);
            UA3Bundle.deleteBundle(prismHelper);

        }
    }

    @Test(dataProvider = "DP", groups = {"multiCluster"}, dataProviderClass = Bundle.class,
            timeOut = 1200000)
    public void updateProcessConcurrencyInEachColoWithOneProcessSuspended(Bundle bundle)
    throws Exception {
        Bundle b = new Bundle();
        b = (Bundle) Bundle.readBundle("src/test/resources/updateBundle")[0][0];
        b.generateUniqueBundle();
        Bundle UA2Bundle = new Bundle(b, UA2ColoHelper);
        Bundle UA3Bundle = new Bundle(b, UA3ColoHelper);
        try {
            UA2Bundle.addClusterToBundle(UA3Bundle.getClusters().get(0), ClusterType.TARGET);
            UA2Bundle.setProcessWorkflow("/examples/apps/phailFs/workflow.xml");
            usualGrind(UA3ColoHelper, UA2Bundle);

            String startTime = InstanceUtil.getTimeWrtSystemTime(-2);
            String endTime = InstanceUtil.getTimeWrtSystemTime(7);
            UA2Bundle.setProcessValidity(startTime, endTime);

            //UA2Bundle.generateUniqueBundle();
            UA2Bundle.submitBundle(prismHelper);
            //now to schedule in 1 colo and let it remain in another
            Util.assertSucceeded(
                    UA3ColoHelper.getProcessHelper()
                            .schedule(URLS.SCHEDULE_URL, UA2Bundle.getProcessData()));
            String oldBundleId = InstanceUtil
                    .getLatestBundleID(UA3ColoHelper,
                            Util.readEntityName(UA2Bundle.getProcessData()), "PROCESS");
            Util.assertSucceeded(
                    UA3ColoHelper.getProcessHelper()
                            .schedule(URLS.SCHEDULE_URL, UA2Bundle.getProcessData()));

            waitForProcessToReachACertainState(UA3ColoHelper, UA2Bundle, "RUNNING");
            Thread.sleep(20000);
            int coordCount = Util.getNumberOfWorkflowInstances(UA3ColoHelper, oldBundleId);

            Util.assertSucceeded(
                    UA3ColoHelper.getProcessHelper()
                            .suspend(URLS.SUSPEND_URL, UA2Bundle.getProcessData()));

            //now to update
            while (Util
                    .parseResponse(updateProcessConcurrency(UA2Bundle,
                            UA2Bundle.getProcessObject().getParallel() + 3))
                    .getStatus() != APIResult.Status.SUCCEEDED) {
                System.out.println("WARNING: update did not scceed, retyring ");
                Thread.sleep(20000);
            }


            String prismString = getResponse(prismHelper, UA2Bundle, true);
            Assert.assertEquals(Util.getProcessObject(prismString).getParallel(),
                    UA2Bundle.getProcessObject().getParallel() + 3);
            dualComparison(UA2Bundle, UA3ColoHelper);
            //ensure that the running process has new coordinators created; while the submitted
            // one is updated
            // correctly.
            verifyNewBundleCreation(UA3ColoHelper, oldBundleId, coordCount,
                    Util.readEntityName(UA2Bundle.getProcessData()), false);
            Util.verifyNoJobsFoundInOozie(
                    Util.getOozieJobStatus(UA2ColoHelper,
                            Util.readEntityName(UA2Bundle.getProcessData()), "RUNNING"));
            Util.assertSucceeded(UA3ColoHelper.getProcessHelper()
                    .resume(URLS.RESUME_URL, UA2Bundle.getProcessData()));
            Assert.assertTrue(
                    Util.getOozieJobStatus(UA3ColoHelper,
                            Util.readEntityName(UA2Bundle.getProcessData())).get(0)
                            .contains("RUNNING"));

            String status =
                    Util.getOozieJobStatus(UA3ColoHelper,
                            Util.readEntityName(UA2Bundle.getProcessData())).get(0);

            boolean doesExist = false;
            while (!status.equalsIgnoreCase("SUCCEEDED") || !status.equalsIgnoreCase("FAILED") ||
                    !status
                            .equalsIgnoreCase("DONEWITHERROR")) {
                if (InstanceUtil
                        .getInstanceCountWithStatus(UA3ColoHelper,
                                Util.readEntityName(UA2Bundle.getProcessData()),
                                org.apache.oozie.client.CoordinatorAction.Status.RUNNING,
                                "PROCESS") ==
                        UA2Bundle.getProcessObject().getParallel()) {
                    doesExist = true;
                    break;
                }
                status = Util.getOozieJobStatus(UA3ColoHelper,
                        Util.readEntityName(UA2Bundle.getProcessData())).get(0);
            }

            Assert.assertTrue(doesExist, "Er! The desired concurrency levels are never reached!!!");

            waitingForBundleFinish(UA3ColoHelper, oldBundleId);

            int finalNumberOfInstances =
                    InstanceUtil.getProcessInstanceListFromAllBundles(UA3ColoHelper,
                            Util.getProcessName(UA2Bundle.getProcessData()), "PROCESS").size();

            int expectedInstances =
                    getExpectedNumberOfWorkflowInstances(InstanceUtil.dateToOozieDate(
                            UA2Bundle.getProcessObject().getClusters().getCluster().get(0)
                                    .getValidity().getStart()),
                            InstanceUtil
                                    .dateToOozieDate(
                                            UA2Bundle.getProcessObject().getClusters().getCluster()
                                                    .get(0).getValidity()
                                                    .getEnd()));

            Assert.assertEquals(finalNumberOfInstances, expectedInstances,
                    "number of instances doesnt match :(");
        } finally {
            UA2Bundle.deleteBundle(prismHelper);
            UA3Bundle.deleteBundle(prismHelper);

        }
    }

    @Test(dataProvider = "DP", groups = {"multiCluster"}, dataProviderClass = Bundle.class,
            timeOut = 1200000)
    public void updateProcessConcurrencyInEachColoWithOneColoDown(Bundle bundle) throws Exception {
        Bundle b = new Bundle();
        b = (Bundle) Bundle.readBundle("src/test/resources/updateBundle")[0][0];
        b.generateUniqueBundle();
        Bundle UA2Bundle = new Bundle(b, UA2ColoHelper);
        Bundle UA3Bundle = new Bundle(b, UA3ColoHelper);
        try {
            UA2Bundle.addClusterToBundle(UA3Bundle.getClusters().get(0), ClusterType.TARGET);
            //UA2Bundle.setProcessWorkflow("/examples/apps/phailFs/workflow.xml");
            usualGrind(UA3ColoHelper, UA2Bundle);

            String startTime = InstanceUtil.getTimeWrtSystemTime(-1);
            String endTime = InstanceUtil.getTimeWrtSystemTime(5);
            UA2Bundle.setProcessValidity(startTime, endTime);

            //UA2Bundle.generateUniqueBundle();
            UA2Bundle.submitBundle(prismHelper);
            //now to schedule in 1 colo and let it remain in another

            System.out.println("process to be scheduled: " + UA2Bundle.getProcessData());

            Util.assertSucceeded(
                    UA3ColoHelper.getProcessHelper()
                            .schedule(URLS.SCHEDULE_URL, UA2Bundle.getProcessData()));
            String oldBundleId = InstanceUtil
                    .getLatestBundleID(UA3ColoHelper,
                            Util.readEntityName(UA2Bundle.getProcessData()), "PROCESS");
            //Util.assertSucceeded(UA3ColoHelper.getProcessHelper().schedule(URLS.SCHEDULE_URL,
            // UA2Bundle.getProcessData()));


            int coordCount = Util.getNumberOfWorkflowInstances(UA3ColoHelper, oldBundleId);

            //now to update
            Util.shutDownService(UA3ColoHelper.getClusterHelper());

            ServiceResponse response =
                    updateProcessConcurrency(UA2Bundle,
                            UA2Bundle.getProcessObject().getParallel() + 3);
            Util.assertPartialSucceeded(response);

            Util.startService(UA3ColoHelper.getClusterHelper());

            String prismString = getResponse(prismHelper, UA2Bundle, true);
            dualComparisonFailure(UA2Bundle, UA3ColoHelper);
            Assert.assertEquals(Util.getProcessObject(prismString).getParallel(),
                    UA2Bundle.getProcessObject().getParallel());

            //ensure that the running process has new coordinators created; while the submitted
            // one is updated
            // correctly.
            //verifyNewBundleCreation(UA3ColoHelper, oldBundleId, coordCount,
            // Util.readEntityName(UA2Bundle.getProcessData()), false);
            Util.verifyNoJobsFoundInOozie(
                    Util.getOozieJobStatus(UA2ColoHelper,
                            Util.readEntityName(UA2Bundle.getProcessData()), "RUNNING"));

            //waitForProcessToReachACertainState(UA3ColoHelper, UA2Bundle, "RUNNING");

            while (Util
                    .parseResponse(updateProcessConcurrency(UA2Bundle,
                            UA2Bundle.getProcessObject().getParallel() + 3))
                    .getStatus() != APIResult.Status.SUCCEEDED) {
                System.out.println("WARNING: update did not scceed, retyring ");
                Thread.sleep(20000);
            }


            prismString = dualComparison(UA2Bundle, UA3ColoHelper);
            dualComparison(UA2Bundle, UA2ColoHelper);

            String status = Util.getOozieJobStatus(UA3ColoHelper,
                    Util.readEntityName(UA2Bundle.getProcessData()))
                    .get(0);

            boolean doesExist = false;
            while (!status.equalsIgnoreCase("SUCCEEDED") || !status.equalsIgnoreCase("FAILED") ||
                    !status
                            .equalsIgnoreCase("DONEWITHERROR")) {
                if (InstanceUtil
                        .getInstanceCountWithStatus(UA3ColoHelper,
                                Util.readEntityName(UA2Bundle.getProcessData()),
                                org.apache.oozie.client.CoordinatorAction.Status.RUNNING,
                                "PROCESS") ==
                        UA2Bundle.getProcessObject().getParallel() + 3) {
                    doesExist = true;
                    break;
                }
                status = Util.getOozieJobStatus(UA3ColoHelper,
                        Util.readEntityName(UA2Bundle.getProcessData())).get(0);
                Thread.sleep(30000);
            }

            Assert.assertTrue(doesExist, "Er! The desired concurrency levels are never reached!!!");
            verifyNewBundleCreation(UA3ColoHelper, InstanceUtil
                    .getLatestBundleID(UA3ColoHelper,
                            Util.readEntityName(UA2Bundle.getProcessData()), "PROCESS"),
                    coordCount, Util.readEntityName(UA2Bundle.getProcessData()), false);

            waitingForBundleFinish(UA3ColoHelper, oldBundleId);

            int finalNumberOfInstances =
                    InstanceUtil.getProcessInstanceListFromAllBundles(UA3ColoHelper,
                            Util.getProcessName(UA2Bundle.getProcessData()), "PROCESS").size();

            int expectedInstances =
                    getExpectedNumberOfWorkflowInstances(InstanceUtil.dateToOozieDate(
                            UA2Bundle.getProcessObject().getClusters().getCluster().get(0)
                                    .getValidity().getStart()),
                            InstanceUtil
                                    .dateToOozieDate(
                                            UA2Bundle.getProcessObject().getClusters().getCluster()
                                                    .get(0).getValidity()
                                                    .getEnd()));

            Assert.assertEquals(finalNumberOfInstances, expectedInstances,
                    "number of instances doesnt match :(");
        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getMessage());
        } finally {

            Util.restartService(UA3ColoHelper.getProcessHelper());
            //UA1Bundle.deleteBundle(prismHelper);
            UA2Bundle.deleteBundle(prismHelper);
            UA3Bundle.deleteBundle(prismHelper);
        }
    }

    @Test(groups = {"multiCluster"}, timeOut = 1200000)
    public void updateProcessConcurrencyExecutionWorkflowInEachColoWithOneProcessRunning()
    throws Exception {
        Bundle b = new Bundle();
        b = (Bundle) Bundle.readBundle("src/test/resources/updateBundle")[0][0];
        b.generateUniqueBundle();
        Bundle UA1Bundle = new Bundle(b, UA1ColoHelper);
        Bundle UA2Bundle = new Bundle(b, UA2ColoHelper);
        Bundle UA3Bundle = new Bundle(b, UA3ColoHelper);
        try {
            UA2Bundle.addClusterToBundle(UA3Bundle.getClusters().get(0), ClusterType.TARGET);
            UA2Bundle.setProcessWorkflow("/examples/apps/phailFs/workflow.xml");
            usualGrind(UA3ColoHelper, UA2Bundle);

            String startTime = InstanceUtil.getTimeWrtSystemTime(-2);
            String endTime = InstanceUtil.getTimeWrtSystemTime(6);
            UA2Bundle.setProcessValidity(startTime, endTime);

            //UA2Bundle.generateUniqueBundle();
            UA2Bundle.submitBundle(prismHelper);
            //now to schedule in 1 colo and let it remain in another
            Util.assertSucceeded(
                    UA3ColoHelper.getProcessHelper()
                            .schedule(URLS.SCHEDULE_URL, UA2Bundle.getProcessData()));
            String oldBundleId = InstanceUtil
                    .getLatestBundleID(UA3ColoHelper,
                            Util.readEntityName(UA2Bundle.getProcessData()), "PROCESS");

            Thread.sleep(30000);
            waitForProcessToReachACertainState(UA3ColoHelper, UA2Bundle, "RUNNING");

            int coordCount = Util.getNumberOfWorkflowInstances(UA3ColoHelper, oldBundleId);

            int initialConcurrency = UA2Bundle.getProcessObject().getParallel();

            UA2Bundle.setProcessConcurrency(UA2Bundle.getProcessObject().getParallel() + 3);
            UA2Bundle.setProcessWorkflow("/examples/apps/aggregator1");
            UA2Bundle.getProcessObject().setOrder(getRandomExecutionType(UA2Bundle));

            //now to update

            String updateTime = new DateTime(DateTimeZone.UTC).plusMinutes(2).toString();

            System.out.println("updating @ " + updateTime);

            while (Util.parseResponse(
                    prismHelper.getProcessHelper().update((UA2Bundle.getProcessData()), UA2Bundle
                            .getProcessData())).getStatus() != APIResult.Status.SUCCEEDED) {
                Thread.sleep(10000);
            }

            String prismString = getResponse(prismHelper, UA2Bundle, true);
            Assert.assertEquals(Util.getProcessObject(prismString).getParallel(),
                    initialConcurrency + 3);
            Assert.assertEquals(Util.getProcessObject(prismString).getWorkflow().getPath(),
                    "/examples/apps/aggregator1");
            Assert.assertEquals(Util.getProcessObject(prismString).getOrder(),
                    UA2Bundle.getProcessObject().getOrder());
            dualComparison(UA2Bundle, UA3ColoHelper);
            //ensure that the running process has new coordinators created; while the submitted
            // one is updated
            // correctly.
            verifyNewBundleCreation(UA3ColoHelper, oldBundleId, coordCount,
                    Util.readEntityName(UA2Bundle.getProcessData()), true);
            Util.verifyNoJobsFoundInOozie(
                    Util.getOozieJobStatus(UA2ColoHelper,
                            Util.readEntityName(UA2Bundle.getProcessData()), "RUNNING"));
            waitingForBundleFinish(UA3ColoHelper, oldBundleId);
            int finalNumberOfInstances =
                    InstanceUtil.getProcessInstanceListFromAllBundles(UA3ColoHelper,
                            Util.getProcessName(UA2Bundle.getProcessData()), "PROCESS").size();

            int expectedInstances =
                    getExpectedNumberOfWorkflowInstances(InstanceUtil.dateToOozieDate(
                            UA2Bundle.getProcessObject().getClusters().getCluster().get(0)
                                    .getValidity().getStart()),
                            InstanceUtil
                                    .dateToOozieDate(
                                            UA2Bundle.getProcessObject().getClusters().getCluster()
                                                    .get(0).getValidity()
                                                    .getEnd()));

            Assert.assertEquals(finalNumberOfInstances, expectedInstances,
                    "number of instances doesnt match :(");
        } finally {
            UA1Bundle.deleteBundle(prismHelper);
            UA2Bundle.deleteBundle(prismHelper);
            UA3Bundle.deleteBundle(prismHelper);

        }
    }

    @Test(groups = {"multiCluster"}, timeOut = 1200000)
    public void updateProcessConcurrencyExecutionWorkflowInEachColoWithOneProcessSuspended()
    throws Exception {
        Bundle b = new Bundle();
        b = (Bundle) Bundle.readBundle("src/test/resources/updateBundle")[0][0];
        b.generateUniqueBundle();
        Bundle UA1Bundle = new Bundle(b, UA1ColoHelper);
        Bundle UA2Bundle = new Bundle(b, UA2ColoHelper);
        Bundle UA3Bundle = new Bundle(b, UA3ColoHelper);
        try {
            UA2Bundle.addClusterToBundle(UA3Bundle.getClusters().get(0), ClusterType.TARGET);
            UA2Bundle.setProcessWorkflow("/examples/apps/phailFs/workflow.xml");
            usualGrind(UA3ColoHelper, UA2Bundle);

            String startTime = InstanceUtil.getTimeWrtSystemTime(2);
            String endTime = InstanceUtil.getTimeWrtSystemTime(6);
            UA2Bundle.setProcessValidity(startTime, endTime);

            //UA2Bundle.generateUniqueBundle();
            UA2Bundle.submitBundle(prismHelper);
            //now to schedule in 1 colo and let it remain in another
            Util.assertSucceeded(
                    UA3ColoHelper.getProcessHelper()
                            .schedule(URLS.SCHEDULE_URL, UA2Bundle.getProcessData()));
            String oldBundleId = InstanceUtil
                    .getLatestBundleID(UA3ColoHelper,
                            Util.readEntityName(UA2Bundle.getProcessData()), "PROCESS");

            Thread.sleep(25000);
            waitForProcessToReachACertainState(UA3ColoHelper, UA2Bundle, "RUNNING");

            int coordCount = Util.getNumberOfWorkflowInstances(UA3ColoHelper, oldBundleId);

            int initialConcurrency = UA2Bundle.getProcessObject().getParallel();

            UA2Bundle.setProcessConcurrency(UA2Bundle.getProcessObject().getParallel() + 3);
            UA2Bundle.setProcessWorkflow("/examples/apps/aggregator1");
            UA2Bundle.getProcessObject().setOrder(getRandomExecutionType(UA2Bundle));
            //suspend
            Util.assertSucceeded(
                    UA3ColoHelper.getProcessHelper()
                            .suspend(URLS.SUSPEND_URL, UA2Bundle.getProcessData()));

            //now to update
            String updateTime = new DateTime(DateTimeZone.UTC).plusMinutes(2).toString();
            System.out.println("updating @ " + updateTime);
            while (Util.parseResponse(
                    prismHelper.getProcessHelper()
                            .update((UA2Bundle.getProcessData()), UA2Bundle.getProcessData()))
                    .getStatus() != APIResult.Status.SUCCEEDED) {
                //keep waiting
            }

            Util.assertSucceeded(UA3ColoHelper.getProcessHelper()
                    .resume(URLS.RESUME_URL, UA2Bundle.getProcessData()));

            String prismString = getResponse(prismHelper, UA2Bundle, true);
            Assert.assertEquals(Util.getProcessObject(prismString).getParallel(),
                    initialConcurrency + 3);
            Assert.assertEquals(Util.getProcessObject(prismString).getWorkflow().getPath(),
                    "/examples/apps/aggregator1");
            Assert.assertEquals(Util.getProcessObject(prismString).getOrder(),
                    UA2Bundle.getProcessObject().getOrder());
            dualComparison(UA2Bundle, UA3ColoHelper);
            //ensure that the running process has new coordinators created; while the submitted
            // one is updated
            // correctly.
            verifyNewBundleCreation(UA3ColoHelper, oldBundleId, coordCount,
                    Util.readEntityName(UA2Bundle.getProcessData()), true);
            Util.verifyNoJobsFoundInOozie(
                    Util.getOozieJobStatus(UA2ColoHelper,
                            Util.readEntityName(UA2Bundle.getProcessData()), "RUNNING"));
            waitingForBundleFinish(UA3ColoHelper, oldBundleId);
            int finalNumberOfInstances =
                    InstanceUtil.getProcessInstanceListFromAllBundles(UA3ColoHelper,
                            Util.getProcessName(UA2Bundle.getProcessData()), "PROCESS").size();

            int expectedInstances =
                    getExpectedNumberOfWorkflowInstances(InstanceUtil.dateToOozieDate(
                            UA2Bundle.getProcessObject().getClusters().getCluster().get(0)
                                    .getValidity().getStart()),
                            InstanceUtil
                                    .dateToOozieDate(
                                            UA2Bundle.getProcessObject().getClusters().getCluster()
                                                    .get(0).getValidity()
                                                    .getEnd()));

            Assert.assertEquals(finalNumberOfInstances, expectedInstances,
                    "number of instances doesnt match :(");
        } finally {
            UA1Bundle.deleteBundle(prismHelper);
            UA2Bundle.deleteBundle(prismHelper);
            UA3Bundle.deleteBundle(prismHelper);

        }
    }

    @Test(groups = {"multiCluster"}, timeOut = 1200000)
    public void updateProcessAddNewInputInEachColoWithOneProcessRunning() throws Exception {
        Bundle b = (Bundle) Bundle.readBundle("src/test/resources/updateBundle")[0][0];
        b.generateUniqueBundle();
        Bundle UA1Bundle = new Bundle(b, UA1ColoHelper);
        Bundle UA2Bundle = new Bundle(b, UA2ColoHelper);
        Bundle UA3Bundle = new Bundle(b, UA3ColoHelper);
        try {
            UA2Bundle.addClusterToBundle(UA3Bundle.getClusters().get(0), ClusterType.TARGET);
            UA2Bundle.setProcessWorkflow("/examples/apps/phailFs/workflow.xml");
            usualGrind(UA3ColoHelper, UA2Bundle);

            String startTime = InstanceUtil.getTimeWrtSystemTime(-2);
            String endTime = InstanceUtil.getTimeWrtSystemTime(6);
            UA2Bundle.setProcessValidity(startTime, endTime);

            //UA2Bundle.generateUniqueBundle();
            UA2Bundle.submitBundle(prismHelper);
            //now to schedule in 1 colo and let it remain in another
            Util.assertSucceeded(
                    UA3ColoHelper.getProcessHelper()
                            .schedule(URLS.SCHEDULE_URL, UA2Bundle.getProcessData()));
            String oldBundleId = InstanceUtil
                    .getLatestBundleID(UA3ColoHelper,
                            Util.readEntityName(UA2Bundle.getProcessData()), "PROCESS");

            Thread.sleep(20000);
            waitForProcessToReachACertainState(UA3ColoHelper, UA2Bundle, "RUNNING");
            int coordCount = Util.getNumberOfWorkflowInstances(UA3ColoHelper, oldBundleId);


            String newFeedName = Util.getInputFeedNameFromBundle(UA2Bundle) + "2";
            String inputFeed = Util.getInputFeedFromBundle(UA2Bundle);

            UA2Bundle.addProcessInput(newFeedName, "inputData2");
            inputFeed = Util.setFeedName(inputFeed, newFeedName);

            System.out.println(inputFeed);
            Util.assertSucceeded(
                    prismHelper.getFeedHelper().submitEntity(URLS.SUBMIT_URL, inputFeed));
            //Util.assertSucceeded(prismHelper.getProcessHelper().update(UA2Bundle.getProcessData(),
            // UA2Bundle.getProcessData()));

            while (Util.parseResponse(
                    prismHelper.getProcessHelper()
                            .update((UA2Bundle.getProcessData()), UA2Bundle.getProcessData()))
                    .getStatus() != APIResult.Status.SUCCEEDED) {
                Thread.sleep(20000);
            }

            String prismString = getResponse(prismHelper, UA2Bundle, true);

            UA2Bundle.verifyDependencyListing();

            dualComparison(UA2Bundle, UA3ColoHelper);
            //ensure that the running process has new coordinators created; while the submitted
            // one is updated
            // correctly.
            verifyNewBundleCreation(UA3ColoHelper, oldBundleId, coordCount,
                    Util.readEntityName(UA2Bundle.getProcessData()), true);
            Util.verifyNoJobsFoundInOozie(
                    Util.getOozieJobStatus(UA2ColoHelper,
                            Util.readEntityName(UA2Bundle.getProcessData()), "RUNNING"));

            waitingForBundleFinish(UA3ColoHelper, oldBundleId);

            int finalNumberOfInstances =
                    InstanceUtil.getProcessInstanceListFromAllBundles(UA3ColoHelper,
                            Util.getProcessName(UA2Bundle.getProcessData()), "PROCESS").size();

            int expectedInstances =
                    getExpectedNumberOfWorkflowInstances(InstanceUtil.dateToOozieDate(
                            UA2Bundle.getProcessObject().getClusters().getCluster().get(0)
                                    .getValidity().getStart()),
                            InstanceUtil
                                    .dateToOozieDate(
                                            UA2Bundle.getProcessObject().getClusters().getCluster()
                                                    .get(0).getValidity()
                                                    .getEnd()));

            Assert.assertEquals(finalNumberOfInstances, expectedInstances,
                    "number of instances doesnt match :(");
        } finally {
            UA1Bundle.deleteBundle(prismHelper);
            UA2Bundle.deleteBundle(prismHelper);
            UA3Bundle.deleteBundle(prismHelper);

        }
    }

    @Test(groups = {"multiCluster"}, timeOut = 1200000)
    public void updateProcessAddNewInputInEachColoWithOneProcessSuspended() throws Exception {
        Bundle b = new Bundle();
        b = (Bundle) Bundle.readBundle("src/test/resources/updateBundle")[0][0];
        b.generateUniqueBundle();
        Bundle UA1Bundle = new Bundle(b, UA1ColoHelper);
        Bundle UA2Bundle = new Bundle(b, UA2ColoHelper);
        Bundle UA3Bundle = new Bundle(b, UA3ColoHelper);
        try {
            UA2Bundle.addClusterToBundle(UA3Bundle.getClusters().get(0), ClusterType.TARGET);
            UA2Bundle.setProcessWorkflow("/examples/apps/phailFs/workflow.xml");
            usualGrind(UA3ColoHelper, UA2Bundle);

            String startTime = InstanceUtil.getTimeWrtSystemTime(1);
            String endTime = InstanceUtil.getTimeWrtSystemTime(6);
            UA2Bundle.setProcessValidity(startTime, endTime);

            //UA2Bundle.generateUniqueBundle();
            UA2Bundle.submitBundle(prismHelper);
            //now to schedule in 1 colo and let it remain in another
            Util.assertSucceeded(
                    UA3ColoHelper.getProcessHelper()
                            .schedule(URLS.SCHEDULE_URL, UA2Bundle.getProcessData()));
            String oldBundleId = InstanceUtil
                    .getLatestBundleID(UA3ColoHelper,
                            Util.readEntityName(UA2Bundle.getProcessData()), "PROCESS");

            Thread.sleep(10000);
            waitForProcessToReachACertainState(UA3ColoHelper, UA2Bundle, "RUNNING");
            int coordCount = Util.getNumberOfWorkflowInstances(UA3ColoHelper, oldBundleId);


            String newFeedName = Util.getInputFeedNameFromBundle(UA2Bundle) + "2";
            String inputFeed = Util.getInputFeedFromBundle(UA2Bundle);

            UA2Bundle.addProcessInput(newFeedName, "inputData2");
            inputFeed = Util.setFeedName(inputFeed, newFeedName);

            Util.assertSucceeded(
                    UA3ColoHelper.getProcessHelper()
                            .suspend(URLS.SUSPEND_URL, UA2Bundle.getProcessData()));
            Util.assertSucceeded(
                    prismHelper.getFeedHelper().submitEntity(URLS.SUBMIT_URL, inputFeed));

            while (Util.parseResponse(
                    prismHelper.getProcessHelper()
                            .update((UA2Bundle.getProcessData()), UA2Bundle.getProcessData()))
                    .getStatus() != APIResult.Status.SUCCEEDED) {
                //keep waiting
            }

            Util.assertSucceeded(UA3ColoHelper.getProcessHelper()
                    .resume(URLS.RESUME_URL, UA2Bundle.getProcessData()));


            String prismString = dualComparison(UA2Bundle, UA2ColoHelper);

            UA2Bundle.verifyDependencyListing();

            dualComparison(UA2Bundle, UA3ColoHelper);
            //ensure that the running process has new coordinators created; while the submitted
            // one is updated
            // correctly.
            verifyNewBundleCreation(UA3ColoHelper, oldBundleId, coordCount,
                    Util.readEntityName(UA2Bundle.getProcessData()), true);
            Util.verifyNoJobsFoundInOozie(
                    Util.getOozieJobStatus(UA2ColoHelper,
                            Util.readEntityName(UA2Bundle.getProcessData()), "RUNNING"));

            waitingForBundleFinish(UA3ColoHelper, oldBundleId);

            int finalNumberOfInstances =
                    InstanceUtil.getProcessInstanceListFromAllBundles(UA3ColoHelper,
                            Util.getProcessName(UA2Bundle.getProcessData()), "PROCESS").size();

            int expectedInstances =
                    getExpectedNumberOfWorkflowInstances(InstanceUtil.dateToOozieDate(
                            UA2Bundle.getProcessObject().getClusters().getCluster().get(0)
                                    .getValidity().getStart()),
                            InstanceUtil
                                    .dateToOozieDate(
                                            UA2Bundle.getProcessObject().getClusters().getCluster()
                                                    .get(0).getValidity()
                                                    .getEnd()));

            Assert.assertEquals(finalNumberOfInstances, expectedInstances,
                    "number of instances doesnt match :(");
        } finally {
            UA1Bundle.deleteBundle(prismHelper);
            UA2Bundle.deleteBundle(prismHelper);
            UA3Bundle.deleteBundle(prismHelper);

        }
    }

    @Test(groups = {"multiCluster"}, timeOut = 1200000)
    public void updateProcessAddNewInputInEachColoWithOneColoDown() throws Exception {
        Bundle b = (Bundle) Bundle.readBundle("src/test/resources/updateBundle")[0][0];
        b.generateUniqueBundle();
        Bundle UA1Bundle = new Bundle(b, UA1ColoHelper);
        Bundle UA2Bundle = new Bundle(b, UA2ColoHelper);
        Bundle UA3Bundle = new Bundle(b, UA3ColoHelper);
        try {
            UA2Bundle.addClusterToBundle(UA3Bundle.getClusters().get(0), ClusterType.TARGET);
            UA2Bundle.setProcessWorkflow("/examples/apps/phailFs/workflow.xml");
            usualGrind(UA3ColoHelper, UA2Bundle);

            String startTime = InstanceUtil.getTimeWrtSystemTime(-2);
            String endTime = InstanceUtil.getTimeWrtSystemTime(6);
            UA2Bundle.setProcessValidity(startTime, endTime);

            //UA2Bundle.generateUniqueBundle();
            UA2Bundle.submitBundle(prismHelper);
            //now to schedule in 1 colo and let it remain in another
            Util.assertSucceeded(
                    UA3ColoHelper.getProcessHelper()
                            .schedule(URLS.SCHEDULE_URL, UA2Bundle.getProcessData()));
            String oldBundleId = InstanceUtil
                    .getLatestBundleID(UA3ColoHelper,
                            Util.readEntityName(UA2Bundle.getProcessData()), "PROCESS");


            int coordCount = Util.getNumberOfWorkflowInstances(UA3ColoHelper, oldBundleId);


            String newFeedName = Util.getInputFeedNameFromBundle(UA2Bundle) + "2";
            String inputFeed = Util.getInputFeedFromBundle(UA2Bundle);

            UA2Bundle.addProcessInput(newFeedName, "inputData2");
            inputFeed = Util.setFeedName(inputFeed, newFeedName);


            Util.assertSucceeded(
                    prismHelper.getFeedHelper().submitEntity(URLS.SUBMIT_URL, inputFeed));

            Util.shutDownService(UA3ColoHelper.getProcessHelper());

            Util.assertPartialSucceeded(
                    prismHelper.getProcessHelper()
                            .update(UA2Bundle.getProcessData(), UA2Bundle.getProcessData()));

            Util.startService(UA3ColoHelper.getProcessHelper());

            String prismString = getResponse(prismHelper, UA2Bundle, true);

            UA2Bundle.verifyDependencyListing();

            dualComparisonFailure(UA2Bundle, UA3ColoHelper);
            //ensure that the running process has new coordinators created; while the submitted
            // one is updated
            // correctly.
            verifyNewBundleCreation(UA3ColoHelper, oldBundleId, coordCount,
                    Util.readEntityName(UA2Bundle.getProcessData()), false);
            Util.verifyNoJobsFoundInOozie(
                    Util.getOozieJobStatus(UA2ColoHelper,
                            Util.readEntityName(UA2Bundle.getProcessData()), "RUNNING"));

            waitForProcessToReachACertainState(UA3ColoHelper, UA2Bundle, "RUNNING");

            while (Util.parseResponse(
                    prismHelper.getProcessHelper()
                            .update((UA2Bundle.getProcessData()), UA2Bundle.getProcessData()))
                    .getStatus() != APIResult.Status.SUCCEEDED) {
                System.out.println("update didnt SUCCEED in last attempt");
                Thread.sleep(10000);
            }
            //Util.assertSucceeded(prismHelper.getProcessHelper().update((UA2Bundle
            // .getProcessData()),
            // UA2Bundle.getProcessData()));
            prismString = getResponse(prismHelper, UA2Bundle, true);
            dualComparisonFailure(UA2Bundle, UA3ColoHelper);
            UA2Bundle.verifyDependencyListing();
            verifyNewBundleCreation(UA3ColoHelper, oldBundleId, coordCount,
                    Util.readEntityName(UA2Bundle.getProcessData()), true);
            Util.verifyNoJobsFoundInOozie(
                    Util.getOozieJobStatus(UA2ColoHelper,
                            Util.readEntityName(UA2Bundle.getProcessData()), "RUNNING"));

            waitingForBundleFinish(UA3ColoHelper, oldBundleId);

            int finalNumberOfInstances =
                    InstanceUtil.getProcessInstanceListFromAllBundles(UA3ColoHelper,
                            Util.getProcessName(UA2Bundle.getProcessData()), "PROCESS").size();

            int expectedInstances =
                    getExpectedNumberOfWorkflowInstances(InstanceUtil.dateToOozieDate(
                            UA2Bundle.getProcessObject().getClusters().getCluster().get(0)
                                    .getValidity().getStart()),
                            InstanceUtil
                                    .dateToOozieDate(
                                            UA2Bundle.getProcessObject().getClusters().getCluster()
                                                    .get(0).getValidity()
                                                    .getEnd()));

            Assert.assertEquals(finalNumberOfInstances, expectedInstances,
                    "number of instances doesnt match :(");
        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getCause());
        } finally {

            Util.restartService(UA3ColoHelper.getProcessHelper());
            UA1Bundle.deleteBundle(prismHelper);
            UA2Bundle.deleteBundle(prismHelper);
            UA3Bundle.deleteBundle(prismHelper);
        }
    }

    @Test(groups = {"multiCluster"}, timeOut = 1200000)
    @SuppressWarnings("SleepWhileInLoop")
    public void updateProcessDecreaseValidityInEachColoWithOneProcessRunning() throws Exception {
        Bundle b = (Bundle) Bundle.readBundle("src/test/resources/updateBundle")[0][0];
        b.generateUniqueBundle();
        Bundle UA1Bundle = new Bundle(b, UA1ColoHelper);
        Bundle UA2Bundle = new Bundle(b, UA2ColoHelper);
        Bundle UA3Bundle = new Bundle(b, UA3ColoHelper);

        try {
            UA2Bundle.addClusterToBundle(UA3Bundle.getClusters().get(0), ClusterType.TARGET);
            UA2Bundle.setProcessWorkflow("/examples/apps/phailFs/workflow.xml");
            usualGrind(UA3ColoHelper, UA2Bundle);

            //UA2Bundle.generateUniqueBundle();
            UA2Bundle.submitBundle(prismHelper);
            //now to schedule in 1 colo and let it remain in another
            Util.assertSucceeded(
                    UA3ColoHelper.getProcessHelper()
                            .schedule(URLS.SCHEDULE_URL, UA2Bundle.getProcessData()));
            String oldBundleId = InstanceUtil
                    .getLatestBundleID(UA3ColoHelper,
                            Util.readEntityName(UA2Bundle.getProcessData()), "PROCESS");

            //Thread.sleep(10000);
            waitForProcessToReachACertainState(UA3ColoHelper, UA2Bundle, "RUNNING");

            int coordCount = Util.getNumberOfWorkflowInstances(UA3ColoHelper, oldBundleId);

            String newEndTime = InstanceUtil.addMinsToTime(InstanceUtil.dateToOozieDate(
                    UA2Bundle.getProcessObject().getClusters().getCluster().get(0).getValidity()
                            .getEnd()), -2);
            UA2Bundle.setProcessValidity(InstanceUtil.dateToOozieDate(
                    UA2Bundle.getProcessObject().getClusters().getCluster().get(0).getValidity()
                            .getStart()),
                    newEndTime);


            while (Util.parseResponse(
                    (prismHelper.getProcessHelper()
                            .update(UA2Bundle.getProcessData(), UA2Bundle.getProcessData())))
                    .getStatus() != APIResult.Status.SUCCEEDED) {
                System.out.println("update didnt SUCCEED in last attempt");
                Thread.sleep(10000);
            }

            verifyNewBundleCreation(UA3ColoHelper, oldBundleId, coordCount,
                    Util.readEntityName(UA2Bundle.getProcessData()), false);

            String prismString = getResponse(prismHelper, UA2Bundle, true);

            UA2Bundle.verifyDependencyListing();

            dualComparison(UA2Bundle, UA3ColoHelper);
            waitingForBundleFinish(UA3ColoHelper, oldBundleId);
            //ensure that the running process has new coordinators created; while the submitted
            // one is updated
            // correctly.
            int finalNumberOfInstances = InstanceUtil
                    .getProcessInstanceList(UA3ColoHelper,
                            Util.getProcessName(UA2Bundle.getProcessData()), "PROCESS")
                    .size();
            Assert.assertEquals(finalNumberOfInstances,
                    getExpectedNumberOfWorkflowInstances(UA2Bundle
                            .getProcessObject().getClusters().getCluster().get(0).getValidity()
                            .getStart(),
                            UA2Bundle.getProcessObject().getClusters().getCluster().get(0)
                                    .getValidity().getEnd()));
            Util.verifyNoJobsFoundInOozie(
                    Util.getOozieJobStatus(UA2ColoHelper, Util.readEntityName(UA2Bundle
                            .getProcessData()), "RUNNING"));

            int expectedNumberOfWorkflows =
                    getExpectedNumberOfWorkflowInstances(InstanceUtil.dateToOozieDate(
                            UA2Bundle.getProcessObject().getClusters().getCluster().get(0)
                                    .getValidity().getStart()),
                            newEndTime);
            Assert.assertEquals(Util.getNumberOfWorkflowInstances(UA3ColoHelper, oldBundleId),
                    expectedNumberOfWorkflows);
        } finally {
            UA1Bundle.deleteBundle(prismHelper);
            UA2Bundle.deleteBundle(prismHelper);
            UA3Bundle.deleteBundle(prismHelper);

        }
    }

    @Test(groups = {"multiCluster"}, timeOut = 1200000)
    public void updateProcessIncreaseValidityInEachColoWithOneProcessSuspended() throws Exception {
        Bundle b = (Bundle) Util.readELBundles()[0][0];
        b.generateUniqueBundle();
        Bundle UA1Bundle = new Bundle(b, UA1ColoHelper);
        Bundle UA2Bundle = new Bundle(b, UA2ColoHelper);
        Bundle UA3Bundle = new Bundle(b, UA3ColoHelper);
        try {
            UA2Bundle.addClusterToBundle(UA3Bundle.getClusters().get(0), ClusterType.TARGET);
            UA2Bundle.setProcessWorkflow("/examples/apps/phailFs/workflow.xml");
            usualGrind(UA3ColoHelper, UA2Bundle);

            String startTime = InstanceUtil.getTimeWrtSystemTime(-1);
            String endTime = InstanceUtil.getTimeWrtSystemTime(3);
            UA2Bundle.setProcessValidity(startTime, endTime);

            //UA2Bundle.generateUniqueBundle();
            UA2Bundle.submitBundle(prismHelper);
            //now to schedule in 1 colo and let it remain in another
            Util.assertSucceeded(
                    UA3ColoHelper.getProcessHelper()
                            .schedule(URLS.SCHEDULE_URL, UA2Bundle.getProcessData()));
            String oldBundleId = InstanceUtil
                    .getLatestBundleID(UA3ColoHelper,
                            Util.readEntityName(UA2Bundle.getProcessData()), "PROCESS");

            Thread.sleep(30000);
            waitForProcessToReachACertainState(UA3ColoHelper, UA2Bundle, "RUNNING");

            int coordCount = Util.getNumberOfWorkflowInstances(UA3ColoHelper, oldBundleId);

            String newEndTime = InstanceUtil.addMinsToTime(InstanceUtil.dateToOozieDate(
                    UA2Bundle.getProcessObject().getClusters().getCluster().get(0).getValidity()
                            .getEnd()), 4);
            UA2Bundle.setProcessValidity(InstanceUtil.dateToOozieDate(
                    UA2Bundle.getProcessObject().getClusters().getCluster().get(0).getValidity()
                            .getStart()),
                    newEndTime);


            Util.assertSucceeded(
                    UA3ColoHelper.getProcessHelper()
                            .suspend(URLS.SUSPEND_URL, UA2Bundle.getProcessData()));
            while (Util.parseResponse(
                    prismHelper.getProcessHelper()
                            .update((UA2Bundle.getProcessData()), UA2Bundle.getProcessData()))
                    .getStatus() != APIResult.Status.SUCCEEDED) {
                System.out.println("update didnt SUCCEED in last attempt");
                Thread.sleep(10000);
            }
            Util.assertSucceeded(UA3ColoHelper.getProcessHelper()
                    .resume(URLS.RESUME_URL, UA2Bundle.getProcessData()));

            //        		for(int wait = 0 ; wait <45 ; wait++)
            //			{
            //				if(instanceUtil.getDefaultCoordinatorStatus(UA3ColoHelper,
            // Util.readEntityName(UA2Bundle.getProcessData()),0).equals(Status.SUCCEEDED))
            //					break;
            //				Thread.sleep(60000);
            //			}
            //
            //        UA3ColoHelper.getProcessHelper().getProcessInstanceStatus(Util
            // .readEntityName(UA2Bundle
            // .getProcessData()),"?start="+UA2Bundle.getProcessObject().getClusters().getCluster
            // ().get(0)
            // .getValidity().getStart());
            //	UA3ColoHelper.getProcessHelper().getProcessInstanceStatus(Util.readEntityName
            // (UA2Bundle.getProcessData
            // ()),"?start="+UA2Bundle.getProcessObject().getClusters().getCluster().get(0)
            // .getValidity().getStart()
            // +"&end="+instanceUtil.addMinsToTime(newEndTime,11));
            //	UA3ColoHelper.getProcessHelper().getProcessInstanceStatus(Util.readEntityName
            // (UA2Bundle.getProcessData
            // ()),"?start="+UA2Bundle.getProcessObject().getClusters().getCluster().get(0)
            // .getValidity().getStart()
            // +"&end="+newEndTime);
            //
            String prismString = dualComparison(UA2Bundle, UA2ColoHelper);


            dualComparison(UA2Bundle, UA3ColoHelper);
            waitingForBundleFinish(UA3ColoHelper, oldBundleId);
            //ensure that the running process has new coordinators created; while the submitted
            // one is updated
            // correctly.
            int finalNumberOfInstances = InstanceUtil
                    .getProcessInstanceList(UA3ColoHelper,
                            Util.getProcessName(UA2Bundle.getProcessData()), "PROCESS")
                    .size();
            Assert.assertEquals(finalNumberOfInstances,
                    getExpectedNumberOfWorkflowInstances(UA2Bundle
                            .getProcessObject().getClusters().getCluster().get(0).getValidity()
                            .getStart(),
                            UA2Bundle.getProcessObject().getClusters().getCluster().get(0)
                                    .getValidity().getEnd()));
            Util.verifyNoJobsFoundInOozie(
                    Util.getOozieJobStatus(UA2ColoHelper,
                            Util.readEntityName(UA2Bundle.getProcessData()), "RUNNING"));
        } finally {
            UA1Bundle.deleteBundle(prismHelper);
            UA2Bundle.deleteBundle(prismHelper);
            UA3Bundle.deleteBundle(prismHelper);

        }
    }

    @Test(groups = {"multiCluster"}, timeOut = 1200000)
    public void updateProcessFrequencyInEachColoWithOneProcessRunning_Daily() throws Exception {
        Bundle b = (Bundle) Bundle.readBundle("src/test/resources/updateBundle")[0][0];
        b.generateUniqueBundle();
        Bundle UA1Bundle = new Bundle(b, UA1ColoHelper);
        Bundle UA2Bundle = new Bundle(b, UA2ColoHelper);
        Bundle UA3Bundle = new Bundle(b, UA3ColoHelper);
        try {
            UA2Bundle.addClusterToBundle(UA3Bundle.getClusters().get(0), ClusterType.TARGET);

            usualGrind(UA3ColoHelper, UA2Bundle);


            //set daily process

            final String START_TIME = InstanceUtil.getTimeWrtSystemTime(-20);
            String endTime = InstanceUtil.getTimeWrtSystemTime(4000);
            UA2Bundle.setProcessPeriodicity(1, TimeUnit.days);
            UA2Bundle.setOutputFeedPeriodicity(1, TimeUnit.days);
            UA2Bundle.setProcessValidity(START_TIME, endTime);


            //UA2Bundle.generateUniqueBundle();
            UA2Bundle.submitBundle(prismHelper);
            //now to schedule in 1 colo and let it remain in another
            Util.assertSucceeded(
                    UA3ColoHelper.getProcessHelper()
                            .schedule(URLS.SCHEDULE_URL, UA2Bundle.getProcessData()));
            String oldBundleId = InstanceUtil
                    .getLatestBundleID(UA3ColoHelper,
                            Util.readEntityName(UA2Bundle.getProcessData()), "PROCESS");


            waitForProcessToReachACertainState(UA3ColoHelper, UA2Bundle, "RUNNING");
            int coordCount = Util.getNumberOfWorkflowInstances(UA3ColoHelper, oldBundleId);

            Util.print("original process: " + UA2Bundle.getProcessData());

            String updatedProcess = InstanceUtil
                    .setProcessFrequency(UA2Bundle.getProcessData(),
                            new Frequency(5, TimeUnit.minutes));

            Util.print("updated process: " + updatedProcess);

            //now to update
            String updatedTime = new DateTime(DateTimeZone.UTC).plusMinutes(2).toString();


            ServiceResponse response =
                    prismHelper.getProcessHelper().update(updatedProcess, updatedProcess);
            Util.assertSucceeded(response);
            String prismString = dualComparison(UA2Bundle, UA2ColoHelper);
            Assert.assertEquals(Util.getProcessObject(prismString).getFrequency(),
                    new Frequency(5, TimeUnit.minutes));
            dualComparison(UA2Bundle, UA3ColoHelper);
            //ensure that the running process has new coordinators created; while the submitted
            // one is updated
            // correctly.
            verifyNewBundleCreation(UA3ColoHelper, oldBundleId, coordCount,
                    Util.readEntityName(UA2Bundle.getProcessData()), true);
            Util.verifyNoJobsFoundInOozie(
                    Util.getOozieJobStatus(UA2ColoHelper,
                            Util.readEntityName(UA2Bundle.getProcessData()), "RUNNING"));
        } finally {
            UA1Bundle.deleteBundle(prismHelper);
            UA2Bundle.deleteBundle(prismHelper);
            UA3Bundle.deleteBundle(prismHelper);

        }

    }


    @Test(groups = {"multiCluster"}, timeOut = 1200000)
    public void
    updateProcessFrequencyInEachColoWithOneProcessRunning_dailyToMonthly_withStartChange()
    throws Exception {
        Bundle b = (Bundle) Bundle.readBundle("src/test/resources/updateBundle")[0][0];
        b.generateUniqueBundle();
        Bundle UA1Bundle = new Bundle(b, UA1ColoHelper);
        Bundle UA2Bundle = new Bundle(b, UA2ColoHelper);
        Bundle UA3Bundle = new Bundle(b, UA3ColoHelper);
        try {
            UA2Bundle.addClusterToBundle(UA3Bundle.getClusters().get(0), ClusterType.TARGET);

            usualGrind(UA3ColoHelper, UA2Bundle);


            //set daily process

            final String START_TIME = InstanceUtil.getTimeWrtSystemTime(-20);
            String endTime = InstanceUtil.getTimeWrtSystemTime(4000 * 60);
            UA2Bundle.setProcessPeriodicity(1, TimeUnit.days);
            UA2Bundle.setOutputFeedPeriodicity(1, TimeUnit.days);
            UA2Bundle.setProcessValidity(START_TIME, endTime);


            //UA2Bundle.generateUniqueBundle();
            UA2Bundle.submitBundle(prismHelper);
            //now to schedule in 1 colo and let it remain in another
            Util.assertSucceeded(
                    UA3ColoHelper.getProcessHelper()
                            .schedule(URLS.SCHEDULE_URL, UA2Bundle.getProcessData()));
            String oldBundleId = InstanceUtil
                    .getLatestBundleID(UA3ColoHelper,
                            Util.readEntityName(UA2Bundle.getProcessData()), "PROCESS");


            waitForProcessToReachACertainState(UA3ColoHelper, UA2Bundle, "RUNNING");
            int coordCount = Util.getNumberOfWorkflowInstances(UA3ColoHelper, oldBundleId);

            Util.print("original process: " + UA2Bundle.getProcessData());

            String updatedProcess = InstanceUtil
                    .setProcessFrequency(UA2Bundle.getProcessData(),
                            new Frequency(1, TimeUnit.months));
            updatedProcess = InstanceUtil
                    .setProcessValidity(updatedProcess, InstanceUtil.getTimeWrtSystemTime(10),
                            endTime);

            Util.print("updated process: " + updatedProcess);

            //now to update
            String updatedTime = new DateTime(DateTimeZone.UTC).plusMinutes(2).toString();


            ServiceResponse response =
                    prismHelper.getProcessHelper().update(updatedProcess, updatedProcess);
            Util.assertSucceeded(response);
            String prismString = dualComparison(UA2Bundle, UA2ColoHelper);
            Assert.assertEquals(Util.getProcessObject(prismString).getFrequency(),
                    new Frequency(1, TimeUnit.months));
            dualComparison(UA2Bundle, UA3ColoHelper);
            //ensure that the running process has new coordinators created; while the submitted
            // one is updated
            // correctly.
            verifyNewBundleCreation(UA3ColoHelper, oldBundleId, coordCount,
                    Util.readEntityName(UA2Bundle.getProcessData()), true);
            Util.verifyNoJobsFoundInOozie(
                    Util.getOozieJobStatus(UA2ColoHelper,
                            Util.readEntityName(UA2Bundle.getProcessData()), "RUNNING"));
        } finally {
            UA1Bundle.deleteBundle(prismHelper);
            UA2Bundle.deleteBundle(prismHelper);
            UA3Bundle.deleteBundle(prismHelper);

        }

    }


    @Test(dataProvider = "DP", groups = {"multiCluster"}, dataProviderClass = Bundle.class,
            timeOut = 1200000)
    @SuppressWarnings("SleepWhileInLoop")
    public void updateProcessRollStartTimeBackwardsToPastInEachColoWithOneProcessRunning(
            Bundle bundle)
    throws Exception {
        Bundle UA1Bundle = new Bundle(bundle, UA1ColoHelper);
        Bundle UA2Bundle = new Bundle(bundle, UA2ColoHelper);
        Bundle UA3Bundle = new Bundle(bundle, UA3ColoHelper);

        try {
            UA2Bundle.addClusterToBundle(UA3Bundle.getClusters().get(0), ClusterType.TARGET);
            UA2Bundle.setProcessWorkflow("/examples/apps/phailFs/workflow.xml");
            usualGrind(UA3ColoHelper, UA2Bundle);

            //UA2Bundle.generateUniqueBundle();
            UA2Bundle.submitBundle(prismHelper);
            //now to schedule in 1 colo and let it remain in another
            Util.assertSucceeded(
                    UA3ColoHelper.getProcessHelper()
                            .schedule(URLS.SCHEDULE_URL, UA2Bundle.getProcessData()));
            String oldBundleId = InstanceUtil
                    .getLatestBundleID(UA3ColoHelper,
                            Util.readEntityName(UA2Bundle.getProcessData()), "PROCESS");

            Thread.sleep(10000);

            int coordCount = Util.getNumberOfWorkflowInstances(UA3ColoHelper, oldBundleId);

            String oldStartTime = InstanceUtil.dateToOozieDate(
                    UA2Bundle.getProcessObject().getClusters().getCluster().get(0).getValidity()
                            .getStart());
            String newStartTime = InstanceUtil.addMinsToTime(InstanceUtil.dateToOozieDate(
                    UA2Bundle.getProcessObject().getClusters().getCluster().get(0).getValidity()
                            .getStart()), -3);
            UA2Bundle.setProcessValidity(newStartTime, InstanceUtil.dateToOozieDate(
                    UA2Bundle.getProcessObject().getClusters().getCluster().get(0).getValidity()
                            .getEnd()));

            waitForProcessToReachACertainState(UA3ColoHelper, UA2Bundle, "RUNNING");

            Util.assertSucceeded(
                    prismHelper.getProcessHelper()
                            .update(UA2Bundle.getProcessData(), UA2Bundle.getProcessData()));

            verifyNewBundleCreation(UA3ColoHelper, oldBundleId, coordCount,
                    Util.readEntityName(UA2Bundle.getProcessData()), true);

            String prismString = dualComparison(UA2Bundle, UA2ColoHelper);

            UA2Bundle.verifyDependencyListing();

            dualComparison(UA2Bundle, UA3ColoHelper);
            //ensure that the running process has new coordinators created; while the submitted
            // one is updated
            // correctly.
            int finalNumberOfInstances =
                    InstanceUtil.getProcessInstanceListFromAllBundles(UA3ColoHelper,
                            Util.getProcessName(UA2Bundle.getProcessData()), "PROCESS").size();
            Assert.assertEquals(finalNumberOfInstances,
                    getExpectedNumberOfWorkflowInstances(oldStartTime,
                            UA2Bundle.getProcessObject().getClusters().getCluster().get(0)
                                    .getValidity().getEnd()));
            Util.verifyNoJobsFoundInOozie(
                    Util.getOozieJobStatus(UA2ColoHelper,
                            Util.readEntityName(UA2Bundle.getProcessData()), "RUNNING"));

            int expectedNumberOfWorkflows = getExpectedNumberOfWorkflowInstances(newStartTime,
                    UA2Bundle.getProcessObject().getClusters().getCluster().get(0).getValidity()
                            .getEnd());
            Assert.assertEquals(Util.getNumberOfWorkflowInstances(UA3ColoHelper, oldBundleId),
                    expectedNumberOfWorkflows);
        } finally {
            UA1Bundle.deleteBundle(prismHelper);
            UA2Bundle.deleteBundle(prismHelper);
            UA3Bundle.deleteBundle(prismHelper);

        }
    }

    @Test(dataProvider = "DP", groups = {"multiCluster"}, dataProviderClass = Bundle.class,
            timeOut = 1200000)
    public void updateProcessRollStartTimeForwardInEachColoWithOneProcessSuspended(Bundle bundle)
    throws Exception {
        Bundle UA1Bundle = new Bundle(bundle, UA1ColoHelper);
        Bundle UA2Bundle = new Bundle(bundle, UA2ColoHelper);
        Bundle UA3Bundle = new Bundle(bundle, UA3ColoHelper);
        try {
            UA2Bundle.addClusterToBundle(UA3Bundle.getClusters().get(0), ClusterType.TARGET);
            UA2Bundle.setProcessWorkflow("/examples/apps/phailFs/workflow.xml");
            usualGrind(UA3ColoHelper, UA2Bundle);

            //UA2Bundle.generateUniqueBundle();
            UA2Bundle.submitBundle(prismHelper);
            //now to schedule in 1 colo and let it remain in another
            Util.assertSucceeded(
                    UA3ColoHelper.getProcessHelper()
                            .schedule(URLS.SCHEDULE_URL, UA2Bundle.getProcessData()));
            String oldBundleId = InstanceUtil
                    .getLatestBundleID(UA3ColoHelper,
                            Util.readEntityName(UA2Bundle.getProcessData()), "PROCESS");

            Thread.sleep(30000);

            int coordCount = Util.getNumberOfWorkflowInstances(UA3ColoHelper, oldBundleId);
            String oldStartTime = InstanceUtil.dateToOozieDate(
                    UA2Bundle.getProcessObject().getClusters().getCluster().get(0).getValidity()
                            .getStart());
            String newStartTime = InstanceUtil.addMinsToTime(InstanceUtil.dateToOozieDate(
                    UA2Bundle.getProcessObject().getClusters().getCluster().get(0).getValidity()
                            .getStart()), 3);
            UA2Bundle.setProcessValidity(newStartTime, InstanceUtil.dateToOozieDate(
                    UA2Bundle.getProcessObject().getClusters().getCluster().get(0).getValidity()
                            .getEnd()));

            waitForProcessToReachACertainState(UA3ColoHelper, UA2Bundle, "RUNNING");

            Util.assertSucceeded(
                    UA3ColoHelper.getProcessHelper()
                            .suspend(URLS.SUSPEND_URL, UA2Bundle.getProcessData()));


            Util.assertSucceeded(
                    prismHelper.getProcessHelper()
                            .update(UA2Bundle.getProcessData(), UA2Bundle.getProcessData()));


            //            for (int wait = 0; wait < 45; wait++) {
            //                if (instanceUtil.getDefaultCoordinatorStatus(UA3ColoHelper,
            // Util.readEntityName(UA2Bundle.getProcessData()), 0).equals(Status.SUCCEEDED)) {
            //                    break;
            //                }
            //                Thread.sleep(60000);
            //            }
            //
            //            UA3ColoHelper.getProcessHelper().getProcessInstanceStatus(Util
            // .readEntityName(UA2Bundle
            // .getProcessData()), "?start=" + newStartTime);
            //            UA3ColoHelper.getProcessHelper().getProcessInstanceStatus(Util
            // .readEntityName(UA2Bundle
            // .getProcessData()), "?start=" + newStartTime + "&end=" + UA2Bundle
            // .getProcessObject().getClusters()
            // .getCluster().get(0).getValidity().getEnd());
            //            UA3ColoHelper.getProcessHelper().getProcessInstanceStatus(Util
            // .readEntityName(UA2Bundle
            // .getProcessData()), "?start=" + newStartTime + "&end=" + UA2Bundle
            // .getProcessObject().getClusters()
            // .getCluster().get(0).getValidity().getEnd());

            String prismString = dualComparison(UA2Bundle, UA2ColoHelper);

            UA2Bundle.verifyDependencyListing();

            dualComparison(UA2Bundle, UA3ColoHelper);
            //ensure that the running process has new coordinators created; while the submitted
            // one is updated
            // correctly.
            int finalNumberOfInstances =
                    InstanceUtil.getProcessInstanceListFromAllBundles(UA3ColoHelper,
                            Util.getProcessName(UA2Bundle.getProcessData()), "PROCESS").size();
            Assert.assertEquals(finalNumberOfInstances,
                    getExpectedNumberOfWorkflowInstances(oldStartTime,
                            UA2Bundle.getProcessObject().getClusters().getCluster().get(0)
                                    .getValidity().getEnd()));
            Assert.assertEquals(InstanceUtil
                    .getProcessInstanceList(UA3ColoHelper,
                            Util.readEntityName(UA2Bundle.getProcessData()), "PROCESS")
                    .size(), getExpectedNumberOfWorkflowInstances(newStartTime,
                    UA2Bundle.getProcessObject().getClusters().getCluster().get(0).getValidity()
                            .getEnd()));

            Util.verifyNoJobsFoundInOozie(
                    Util.getOozieJobStatus(UA2ColoHelper,
                            Util.readEntityName(UA2Bundle.getProcessData()), "RUNNING"));
        } finally {
            UA1Bundle.deleteBundle(prismHelper);
            UA2Bundle.deleteBundle(prismHelper);
            UA3Bundle.deleteBundle(prismHelper);

        }
    }

    @Test(dataProvider = "DP", groups = {"multiCluster"}, dataProviderClass = Bundle.class,
            timeOut = 1200000)
    public void updateProcessRollStartTimeBackwardsInEachColoWithOneProcessSuspended(Bundle bundle)
    throws Exception {
        Bundle b = (Bundle) Util.readELBundles()[0][0];
        b.generateUniqueBundle();
        Bundle UA1Bundle = new Bundle(b, UA1ColoHelper);
        Bundle UA2Bundle = new Bundle(b, UA2ColoHelper);
        Bundle UA3Bundle = new Bundle(b, UA3ColoHelper);
        try {
            UA2Bundle.addClusterToBundle(UA3Bundle.getClusters().get(0), ClusterType.TARGET);
            UA2Bundle.setProcessWorkflow("/examples/apps/phailFs/workflow.xml");
            usualGrind(UA3ColoHelper, UA2Bundle);

            //UA2Bundle.generateUniqueBundle();
            UA2Bundle.submitBundle(prismHelper);
            //now to schedule in 1 colo and let it remain in another
            Util.assertSucceeded(
                    UA3ColoHelper.getProcessHelper()
                            .schedule(URLS.SCHEDULE_URL, UA2Bundle.getProcessData()));
            String oldBundleId = InstanceUtil
                    .getLatestBundleID(UA3ColoHelper,
                            Util.readEntityName(UA2Bundle.getProcessData()), "PROCESS");

            Thread.sleep(30000);

            int coordCount = Util.getNumberOfWorkflowInstances(UA3ColoHelper, oldBundleId);
            String oldStartTime = InstanceUtil.dateToOozieDate(
                    UA2Bundle.getProcessObject().getClusters().getCluster().get(0).getValidity()
                            .getStart());
            String newStartTime = InstanceUtil.addMinsToTime(InstanceUtil.dateToOozieDate(
                    UA2Bundle.getProcessObject().getClusters().getCluster().get(0).getValidity()
                            .getStart()), -3);
            UA2Bundle.setProcessValidity(newStartTime, InstanceUtil.dateToOozieDate(
                    UA2Bundle.getProcessObject().getClusters().getCluster().get(0).getValidity()
                            .getEnd()));

            waitForProcessToReachACertainState(UA3ColoHelper, UA2Bundle, "RUNNING");

            Util.assertSucceeded(
                    UA3ColoHelper.getProcessHelper()
                            .suspend(URLS.SUSPEND_URL, UA2Bundle.getProcessData()));
            Util.assertSucceeded(
                    prismHelper.getProcessHelper()
                            .update(UA2Bundle.getProcessData(), UA2Bundle.getProcessData()));
            Util.assertSucceeded(UA3ColoHelper.getProcessHelper()
                    .resume(URLS.RESUME_URL, UA2Bundle.getProcessData()));
            verifyNewBundleCreation(UA3ColoHelper, oldBundleId, coordCount,
                    Util.readEntityName(UA2Bundle.getProcessData()), true);

            //            for (int wait = 0; wait < 45; wait++) {
            //                if (instanceUtil.getDefaultCoordinatorStatus(UA3ColoHelper,
            // Util.readEntityName(UA2Bundle.getProcessData()), 0).equals(Status.SUCCEEDED)) {
            //                    break;
            //                }
            //                Thread.sleep(60000);
            //            }
            //
            //            UA3ColoHelper.getProcessHelper().getProcessInstanceStatus(Util
            // .readEntityName(UA2Bundle
            // .getProcessData()), "?start=" + newStartTime);
            //            UA3ColoHelper.getProcessHelper().getProcessInstanceStatus(Util
            // .readEntityName(UA2Bundle
            // .getProcessData()), "?start=" + newStartTime + "&end=" + UA2Bundle
            // .getProcessObject().getClusters()
            // .getCluster().get(0).getValidity().getEnd());
            //            UA3ColoHelper.getProcessHelper().getProcessInstanceStatus(Util
            // .readEntityName(UA2Bundle
            // .getProcessData()), "?start=" + newStartTime + "&end=" + UA2Bundle
            // .getProcessObject().getClusters()
            // .getCluster().get(0).getValidity().getEnd());

            String prismString = dualComparison(UA2Bundle, UA2ColoHelper);

            UA2Bundle.verifyDependencyListing();

            dualComparison(UA2Bundle, UA3ColoHelper);
            waitingForBundleFinish(UA3ColoHelper, oldBundleId);
            //ensure that the running process has new coordinators created; while the submitted
            // one is updated
            // correctly.
            //int finalNumberOfInstances=instanceUtil.getProcessInstanceListFromAllBundles
            // (UA3ColoHelper,
            // Util.getProcessName(UA2Bundle.getProcessData()), "PROCESS").size();
            //Assert.assertEquals(finalNumberOfInstances,getExpectedNumberOfWorkflowInstances
            // (oldStartTime,
            // UA2Bundle.getProcessObject().getClusters().getCluster().get(0).getValidity()
            // .getEnd()));

            int finalNumberOfInstances =
                    InstanceUtil.getProcessInstanceListFromAllBundles(UA3ColoHelper,
                            Util.getProcessName(UA2Bundle.getProcessData()), "PROCESS").size();
            Assert.assertEquals(finalNumberOfInstances,
                    getExpectedNumberOfWorkflowInstances(oldStartTime,
                            UA2Bundle.getProcessObject().getClusters().getCluster().get(0)
                                    .getValidity().getEnd()));
            Util.verifyNoJobsFoundInOozie(
                    Util.getOozieJobStatus(UA2ColoHelper,
                            Util.readEntityName(UA2Bundle.getProcessData()), "RUNNING"));
        } finally {
            UA1Bundle.deleteBundle(prismHelper);
            UA2Bundle.deleteBundle(prismHelper);
            UA3Bundle.deleteBundle(prismHelper);

        }
    }


    private String setProcessTimeOut(String process, int mag, TimeUnit unit) throws Exception {

        Process p = InstanceUtil.getProcessElement(process);


        //TimeUnit b;

        Frequency f = new Frequency(mag, unit);

        p.setTimeout(f);
        return InstanceUtil.processToString(p);
    }


    private void submitClusters(Bundle bundle) throws Exception {
        for (String cluster : bundle.getClusters()) {
            System.out.println("will submit: " + cluster);
            Util.assertSucceeded(
                    prismHelper.getClusterHelper().submitEntity(Util.URLS.SUBMIT_URL, cluster));
        }
    }

    private void submitProcess(Bundle bundle) throws Exception {
        submitClusters(bundle);
        for (String feed : bundle.getDataSets()) {
            System.out.println(feed);
            Util.assertSucceeded(prismHelper.getFeedHelper().submitEntity(URLS.SUBMIT_URL, feed));
        }

        Util.assertSucceeded(
                prismHelper.getProcessHelper()
                        .submitEntity(Util.URLS.SUBMIT_URL, bundle.getProcessData()));
    }

    private void submitAndScheduleProcess(Bundle bundle) throws Exception {
        submitProcess(bundle);
        Util.assertSucceeded(prismHelper.getProcessHelper()
                .schedule(Util.URLS.SCHEDULE_URL, bundle.getProcessData()));
    }

    public ServiceResponse updateProcessConcurrency(Bundle bundle, int concurrency)
    throws Exception {
        String oldData = new String(bundle.getProcessData());
        Process updatedProcess = bundle.getProcessObject();
        updatedProcess.setParallel(concurrency);

        return prismHelper.getProcessHelper()
                .update(oldData, prismHelper.getProcessHelper().toString(updatedProcess));
    }

    private String dualComparison(Bundle bundle, ColoHelper coloHelper) throws Exception {
        String prismResponse = getResponse(prismHelper, bundle, true);
        String coloResponse = getResponse(coloHelper, bundle, true);
        AssertJUnit.assertEquals(prismResponse, coloResponse);

        //AssertJUnit.assertEquals(getResponse(prismHelper, bundle), getResponse(coloHelper, bundle));
        return getResponse(prismHelper, bundle, true);
    }

    private void dualComparisonFailure(Bundle bundle, ColoHelper coloHelper) throws Exception {
        AssertJUnit.assertNotSame(getResponse(prismHelper, bundle, true),
                getResponse(coloHelper, bundle, true));

    }

    private String getResponse(PrismHelper prismHelper, Bundle bundle, boolean bool)
    throws Exception {

        ServiceResponse response = prismHelper.getProcessHelper()
                .getEntityDefinition(Util.URLS.GET_ENTITY_DEFINITION, bundle.getProcessData());
        if (bool)
            Util.assertSucceeded(response);
        else
            Util.assertFailed(response);
        String result = response.getMessage();
        Assert.assertNotNull(result);

        return result;

    }

    private void verifyNewBundleCreation(ColoHelper coloHelper, String originalBundleId,
                                         int originalBundleCount,
                                         String processName, boolean shouldBeCreated)
    throws Exception {
        String newBundleId = InstanceUtil.getLatestBundleID(coloHelper, processName, "PROCESS");
        if (shouldBeCreated) {
            Assert.assertTrue(!newBundleId.equalsIgnoreCase(originalBundleId),
                    "eeks! new bundle is not getting created!!!!");
            System.out.println("old bundleId=" + originalBundleId);
            System.out.println("new bundleId=" + newBundleId);
            //Util.validateNumberOfWorkflowInstances(prismHelper,originalBundleCount, newBundleId, newBundleId);
        } else {
            AssertJUnit.assertEquals("eeks! new bundle is getting created!!!!", newBundleId,
                    originalBundleId);

        }

    }

    private void waitForProcessToReachACertainState(ColoHelper coloHelper, Bundle bundle,
                                                    String state)
    throws Exception {


        while (!Util.getOozieJobStatus(coloHelper, Util.readEntityName(bundle.getProcessData()))
                .get(0)
                .contains(state.toUpperCase()))
            ;
        {
            //keep waiting
            Thread.sleep(10000);

        }

        //now check if the coordinator is in desired state
        CoordinatorJob coord = getDefaultOozieCoord(coloHelper, InstanceUtil
                .getLatestBundleID(coloHelper, Util.readEntityName(bundle.getProcessData()),
                        "PROCESS"));

        while (!coord.getStatus().equals(Status.valueOf(state.toUpperCase()))) {
            Thread.sleep(10000);

            coord = getDefaultOozieCoord(coloHelper, InstanceUtil
                    .getLatestBundleID(coloHelper, Util.readEntityName(bundle.getProcessData()),
                            "PROCESS"));
        }
    }

    private Bundle usualGrind(PrismHelper prismHelper, Bundle b) throws Exception {
        System.setProperty("java.security.krb5.realm", "");
        System.setProperty("java.security.krb5.kdc", "");

        b.setInputFeedDataPath(
                "/samarthUpdate/input-data/rawLogs/oozieExample/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");

        String prefix = b.getFeedDataPathPrefix();
        Util.HDFSCleanup(prismHelper, prefix.substring(1));
        Util.lateDataReplenish(prismHelper, 60, 0, 1, prefix);
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

        return prismHelper.getProcessHelper()
                .update(oldData, prismHelper.getProcessHelper().toString(updatedProcess));
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
