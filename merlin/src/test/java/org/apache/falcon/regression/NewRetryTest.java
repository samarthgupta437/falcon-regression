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
import org.apache.falcon.regression.core.generated.dependencies.Frequency;
import org.apache.falcon.regression.core.generated.process.PolicyType;
import org.apache.falcon.regression.core.generated.process.Process;
import org.apache.falcon.regression.core.generated.process.Retry;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.response.ProcessInstancesResult;
import org.apache.falcon.regression.core.response.ServiceResponse;
import org.apache.falcon.regression.core.supportClasses.ENTITY_TYPE;
import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.core.util.Util.URLS;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.hadoop.fs.FileSystem;
import org.apache.oozie.client.BundleJob;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowJob;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.*;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Test(groups = "embedded")
public class NewRetryTest extends BaseTestClass {

    ColoHelper cluster = servers.get(0);
    FileSystem clusterFS = serverFS.get(0);
    OozieClient clusterOC = serverOC.get(0);
    String aggregateWorkflowDir = baseWorkflowDir + "/aggregator";

    DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy/MM/dd/HH/mm");
    private String latePath = "/lateDataTest/testFolders/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}";
    private String lateDir = "/lateDataTest/testFolders/";
    private DateTime startDate;
    private DateTime endDate;

    @BeforeClass
    public void uploadWorkflow() throws Exception {
        HadoopUtil.uploadDir(clusterFS, aggregateWorkflowDir, OSUtil.RESOURCES_OOZIE);
    }

    @BeforeMethod(alwaysRun = true)
    public void setUp(Method method) throws Exception {
        Util.print("test name: " + method.getName());
        bundles[0] = new Bundle(Util.getBundleData("RetryTests")[0], cluster);
        bundles[0].generateUniqueBundle();
        bundles[0].setProcessWorkflow(aggregateWorkflowDir);
        startDate = new DateTime(DateTimeZone.UTC).plusMinutes(1);
        endDate = new DateTime(DateTimeZone.UTC).plusMinutes(2);
        bundles[0].setProcessValidity(startDate, endDate);
    }

    @AfterMethod
    public void tearDown() throws Exception {
        removeBundles();
    }

    @Test(dataProvider = "DP", groups = {"0.2.2", "retry"}, enabled = true)
    public void testRetryInProcessZeroAttemptUpdate(String retryType, int delay, String delayUnits,
                                                    int retryAttempts) throws Exception {
        displayInputs("testRetryInProcessUpdate", delay, retryType, retryAttempts);

        String feed = Util.setFeedPathValue(Util.getInputFeedFromBundle(bundles[0]), latePath);
        feed = Util.insertLateFeedValue(feed, "8", "minutes");
        bundles[0].getDataSets().remove(Util.getInputFeedFromBundle(bundles[0]));
        bundles[0].getDataSets().add(feed);
        bundles[0].submitClusters(prism);

        for (String data : bundles[0].getDataSets()) {
            Util.assertSucceeded(prism.getFeedHelper().submitEntity(URLS.SUBMIT_URL, data));
        }

        bundles[0].setRetry(getRetry(bundles[0], delay, delayUnits, retryType, retryAttempts));

        //submit and schedule process
        ServiceResponse response = prism.getProcessHelper().submitEntity(URLS.SUBMIT_URL, bundles[0].getProcessData());

        if (retryAttempts <= 0 || delay <= 0) {
            Util.assertFailed(response);
        } else {
            Util.assertSucceeded(response);
            // lets create data now:
            HadoopUtil.deleteDirIfExists(lateDir, clusterFS);
            Util.lateDataReplenish(cluster, 20, 0);
            List<String> initialData = Util.getHadoopLateData(cluster, Util.getInputFeedFromBundle(bundles[0]));

            //schedule process
            Util.assertSucceeded(prism.getProcessHelper().schedule(URLS.SCHEDULE_URL, bundles[0].getProcessData()));

            //now wait till the process is over
            String bundleId = Util.getBundles(cluster.getFeedHelper().getOozieClient(),
                    Util.readEntityName(bundles[0].getProcessData()), ENTITY_TYPE.PROCESS).get(0);
            String status = Util.getBundleStatus(cluster, bundleId);

            waitTillCertainPercentageOfProcessHasStarted(cluster, bundleId, 25);

            Process oldProcessObject = bundles[0].getProcessObject();

            int defaultRetries = bundles[0].getProcessObject().getRetry().getAttempts();
            Retry retry = bundles[0].getProcessObject().getRetry();
            retry.setAttempts((0));

            bundles[0].setRetry(retry);

            System.out.println("going to update process at:" + DateTime.now(DateTimeZone.UTC));
            prism.getProcessHelper().update((bundles[0].getProcessData()), bundles[0].getProcessData());
            String newBundleId = InstanceUtil.getLatestBundleID(cluster,
                    Util.readEntityName(bundles[0].getProcessData()), ENTITY_TYPE.PROCESS);

            Assert.assertEquals(bundleId, newBundleId, "its creating a new bundle!!!");

            //now to validate all failed instances to check if they were retried or not.
            validateRetry(cluster, bundleId, defaultRetries);
            checkIfRetriesWereTriggeredCorrectly(cluster, retryType, delay, bundleId);
        }
    }


    @Test(dataProvider = "DP", groups = {"0.2.2", "retry"}, enabled = true)
    public void testRetryInProcessLowerAttemptUpdate(String retryType, int delay, String delayUnits,
                                                     int retryAttempts) throws Exception {
        displayInputs("testRetryInProcessUpdate", delay, retryType, retryAttempts);

        String feed = Util.setFeedPathValue(Util.getInputFeedFromBundle(bundles[0]), latePath);
        feed = Util.insertLateFeedValue(feed, "8", "minutes");
        bundles[0].getDataSets().remove(Util.getInputFeedFromBundle(bundles[0]));
        bundles[0].getDataSets().add(feed);
        bundles[0].submitClusters(prism);

        for (String data : bundles[0].getDataSets()) {
            Util.assertSucceeded(prism.getFeedHelper().submitEntity(URLS.SUBMIT_URL, data));
        }
        bundles[0].setRetry(getRetry(bundles[0], delay, delayUnits, retryType, retryAttempts));

        //submit and schedule process
        ServiceResponse response = prism.getProcessHelper().submitEntity(URLS.SUBMIT_URL, bundles[0].getProcessData());

        if (retryAttempts <= 0 || delay <= 0) {
            Util.assertFailed(response);
        } else {
            Util.assertSucceeded(response);
            HadoopUtil.deleteDirIfExists(lateDir, clusterFS);
            Util.lateDataReplenish(cluster, 20, 0);
            List<String> initialData = Util.getHadoopLateData(cluster, Util.getInputFeedFromBundle(bundles[0]));
            //now wait till the process is over
            Util.assertSucceeded(prism.getProcessHelper().schedule(URLS.SCHEDULE_URL, bundles[0].getProcessData()));
            String bundleId = Util.getBundles(cluster.getFeedHelper().getOozieClient(),
                    Util.readEntityName(bundles[0].getProcessData()), ENTITY_TYPE.PROCESS).get(0);
            String status = Util.getBundleStatus(cluster, bundleId);

            boolean validation = false;
            for (int attempt = 0; attempt < 60; ++attempt) {
                validation =
                        validateFailureRetries(cluster, getDefaultOozieCoord(cluster, bundleId), 1);
                if (validation)
                    break;
                Thread.sleep(10000);
            }
            Assert.assertTrue(validation, "Failure Retry validation failed");

            Process oldProcessObject = bundles[0].getProcessObject();

            Retry retry = bundles[0].getProcessObject().getRetry();
            retry.setAttempts((retryAttempts - 2));

            bundles[0].setRetry(retry);

            System.out.println("going to update process at:" + DateTime.now(DateTimeZone.UTC));

            if ((retryAttempts - 2) > 0) {
                Assert.assertTrue(prism.getProcessHelper().update((bundles[0].getProcessData()), bundles[0].getProcessData())
                                .getMessage().contains("updated successfully"), "process was not updated successfully");
                String newBundleId = InstanceUtil.getLatestBundleID(cluster,
                                Util.readEntityName(bundles[0].getProcessData()), ENTITY_TYPE.PROCESS);

                Assert.assertEquals(bundleId, newBundleId, "its creating a new bundle!!!");

                //now to validate all failed instances to check if they were retried or not.
                validateRetry(cluster, bundleId, retryAttempts - 2);
                if (bundles[0].getProcessObject().getRetry().getAttempts() > 0) {
                    checkIfRetriesWereTriggeredCorrectly(cluster, retryType, delay, bundleId);
                }
            }
        }
    }


    @Test(dataProvider = "DP", groups = {"0.2.2", "retry"}, enabled = false)
    public void testRetryInProcessLowerManageableAttemptUpdate(String retryType, int delay,
                                                               String delayUnits, int retryAttempts) throws Exception {
        displayInputs("testRetryInProcessUpdate", delay, retryType, retryAttempts);

        String feed = Util.setFeedPathValue(Util.getInputFeedFromBundle(bundles[0]), latePath);
        feed = Util.insertLateFeedValue(feed, "8", "minutes");
        bundles[0].getDataSets().remove(Util.getInputFeedFromBundle(bundles[0]));
        bundles[0].getDataSets().add(feed);

        bundles[0].submitClusters(prism);

        for (String data : bundles[0].getDataSets()) {
            Util.assertSucceeded(prism.getFeedHelper().submitEntity(URLS.SUBMIT_URL, data));
        }
        bundles[0].setRetry(getRetry(bundles[0], delay, delayUnits, retryType, retryAttempts));

        //submit and schedule process
        ServiceResponse response = prism.getProcessHelper().submitEntity(URLS.SUBMIT_URL, bundles[0].getProcessData());

        if (retryAttempts <= 0 || delay <= 0) {
            Util.assertFailed(response);
        } else {
            Util.assertSucceeded(response);
            HadoopUtil.deleteDirIfExists(lateDir, clusterFS);
            Util.lateDataReplenish(cluster, 20, 0);
            List<String> initialData = Util.getHadoopLateData(cluster, Util.getInputFeedFromBundle(bundles[0]));

            Util.assertSucceeded(prism.getProcessHelper().schedule(URLS.SCHEDULE_URL, bundles[0].getProcessData()));
            //now wait till the process is over
            String bundleId = Util.getBundles(clusterOC,
                    Util.readEntityName(bundles[0].getProcessData()), ENTITY_TYPE.PROCESS).get(0);;
            String status = Util.getBundleStatus(cluster, bundleId);

            while (!validateFailureRetries(cluster, getDefaultOozieCoord(cluster, bundleId), 1)) {
                //wait
            }

            Process oldProcessObject = bundles[0].getProcessObject();

            Retry retry = bundles[0].getProcessObject().getRetry();
            retry.setAttempts((retryAttempts - 1));

            bundles[0].setRetry(retry);

            System.out.println("going to update process at:" + DateTime.now(DateTimeZone.UTC));
            Assert.assertTrue(prism.getProcessHelper()
                            .update((bundles[0].getProcessData()), bundles[0].getProcessData())
                            .getMessage().contains("updated successfully"),
                    "process was not updated successfully");
            String newBundleId = InstanceUtil.getLatestBundleID(cluster,
                   Util.readEntityName(bundles[0].getProcessData()), ENTITY_TYPE.PROCESS);

            Assert.assertEquals(bundleId, newBundleId, "its creating a new bundle!!!");

            //now to validate all failed instances to check if they were retried or not.
            validateRetry(cluster, bundleId, retryAttempts - 1);
            if (bundles[0].getProcessObject().getRetry().getAttempts() > 0) {
                checkIfRetriesWereTriggeredCorrectly(cluster, retryType, delay, bundleId);
            }
        }
    }

    @Test(dataProvider = "DP", groups = {"0.2.2", "retry"}, enabled = false)
    public void testRetryInProcessLowerBoundaryAttemptUpdate(String retryType, int delay,
                                                             String delayUnits, int retryAttempts) throws Exception {
        displayInputs("testRetryInProcessUpdate", delay, retryType, retryAttempts);

        String feed = Util.setFeedPathValue(Util.getInputFeedFromBundle(bundles[0]), latePath);
        feed = Util.insertLateFeedValue(feed, "8", "minutes");
        bundles[0].getDataSets().remove(Util.getInputFeedFromBundle(bundles[0]));
        bundles[0].getDataSets().add(feed);
        bundles[0].submitClusters(prism);

        for (String data : bundles[0].getDataSets()) {
            Util.assertSucceeded(prism.getFeedHelper().submitEntity(URLS.SUBMIT_URL, data));
        }
        bundles[0].setRetry(getRetry(bundles[0], delay, delayUnits, retryType, retryAttempts));

        //submit and schedule process
        ServiceResponse response = prism.getProcessHelper().submitEntity(URLS.SUBMIT_URL, bundles[0].getProcessData());

        if (retryAttempts <= 0 || delay <= 0) {
            Util.assertFailed(response);
        } else {
            Util.assertSucceeded(response);
            HadoopUtil.deleteDirIfExists(lateDir, clusterFS);
            Util.lateDataReplenish(cluster, 20, 0);
            List<String> initialData = Util.getHadoopLateData(cluster, Util.getInputFeedFromBundle(bundles[0]));
            Util.assertSucceeded(prism.getProcessHelper().schedule(URLS.SCHEDULE_URL, bundles[0].getProcessData()));

            //now wait till the process is over
            String bundleId = Util.getBundles(clusterOC,
                    Util.readEntityName(bundles[0].getProcessData()), ENTITY_TYPE.PROCESS).get(0);
            String status = Util.getBundleStatus(cluster, bundleId);

            while (!validateFailureRetries(cluster, getDefaultOozieCoord(cluster, bundleId), 2)) {
                //wait
            }

            Process oldProcessObject = bundles[0].getProcessObject();

            Retry retry = bundles[0].getProcessObject().getRetry();
            retry.setAttempts((2));

            bundles[0].setRetry(retry);

            System.out.println("going to update process at:" + DateTime.now(DateTimeZone.UTC));
            Assert.assertTrue(
                    prism.getProcessHelper()
                            .update((bundles[0].getProcessData()), bundles[0].getProcessData())
                            .getMessage().contains("updated successfully"),
                    "process was not updated successfully");
            String newBundleId = InstanceUtil.getLatestBundleID(cluster,
                    Util.readEntityName(bundles[0].getProcessData()), ENTITY_TYPE.PROCESS);

            Assert.assertEquals(bundleId, newBundleId, "its creating a new bundle!!!");

            //now to validate all failed instances to check if they were retried or not.
            validateRetry(cluster, bundleId, 2);
            if (bundles[0].getProcessObject().getRetry().getAttempts() > 0) {
                checkIfRetriesWereTriggeredCorrectly(cluster, retryType, delay, bundleId);
            }
        }
    }

    @Test(dataProvider = "DP", groups = {"0.2.2", "retry"}, enabled = false)
    public void testRetryInProcessUpdate(String retryType, int delay, String delayUnits,
                                         int retryAttempts) throws Exception {
        displayInputs("testRetryInProcessUpdate", delay, retryType, retryAttempts);

        String feed = Util.setFeedPathValue(Util.getInputFeedFromBundle(bundles[0]), latePath);
        feed = Util.insertLateFeedValue(feed, "8", "minutes");
        bundles[0].getDataSets().remove(Util.getInputFeedFromBundle(bundles[0]));
        bundles[0].getDataSets().add(feed);
        bundles[0].submitClusters(prism);

        for (String data : bundles[0].getDataSets()) {
            Util.assertSucceeded(prism.getFeedHelper().submitEntity(URLS.SUBMIT_URL, data));
        }
        bundles[0].setRetry(getRetry(bundles[0], delay, delayUnits, retryType, retryAttempts));

        //submit and schedule process
        ServiceResponse response = prism.getProcessHelper().submitEntity(URLS.SUBMIT_URL, bundles[0].getProcessData());

        if (retryAttempts <= 0 || delay <= 0) {
            Util.assertFailed(response);
        } else {
            Util.assertSucceeded(response);
            HadoopUtil.deleteDirIfExists(lateDir, clusterFS);
            Util.lateDataReplenish(cluster, 20, 0);
            List<String> initialData = Util.getHadoopLateData(cluster, Util.getInputFeedFromBundle(bundles[0]));
            Util.assertSucceeded(prism.getProcessHelper().schedule(URLS.SCHEDULE_URL, bundles[0].getProcessData()));
            //now wait till the process is over
            String bundleId = Util.getBundles(clusterOC,
                    Util.readEntityName(bundles[0].getProcessData()), ENTITY_TYPE.PROCESS).get(0);
            String status = Util.getBundleStatus(cluster, bundleId);

            waitTillCertainPercentageOfProcessHasStarted(cluster, bundleId, 25);

            Process oldProcessObject = bundles[0].getProcessObject();

            Retry retry = bundles[0].getProcessObject().getRetry();
            retry.setAttempts((4));

            bundles[0].setRetry(retry);

            System.out.println("going to update process at:" + DateTime.now(DateTimeZone.UTC));
            Assert.assertTrue(prism.getProcessHelper()
                    .update(Util.readEntityName(bundles[0].getProcessData()),
                            bundles[0].getProcessFilePath()).getMessage()
                    .contains("updated successfully"), "process was not updated successfully");
            String newBundleId = InstanceUtil.getLatestBundleID(cluster,
                    Util.readEntityName(bundles[0].getProcessData()), ENTITY_TYPE.PROCESS);

            Assert.assertEquals(bundleId, newBundleId, "its creating a new bundle!!!");

            //now to validate all failed instances to check if they were retried or not.
            validateRetry(cluster, bundleId, 4);
            if (bundles[0].getProcessObject().getRetry().getAttempts() > 0) {
                checkIfRetriesWereTriggeredCorrectly(cluster, retryType, delay, bundleId);
            }
        }
    }


    @Test(dataProvider = "DP", groups = {"0.2.2", "retry"}, enabled = false)
    public void testRetryInProcessHigherDelayUpdate(String retryType, int delay, String delayUnits,
                                                    int retryAttempts) throws Exception {
        displayInputs("testRetryInProcessUpdate", delay, retryType, retryAttempts);

        String feed = Util.setFeedPathValue(Util.getInputFeedFromBundle(bundles[0]), latePath);
        feed = Util.insertLateFeedValue(feed, "8", "minutes");
        bundles[0].getDataSets().remove(Util.getInputFeedFromBundle(bundles[0]));
        bundles[0].getDataSets().add(feed);
        bundles[0].submitClusters(prism);

        for (String data : bundles[0].getDataSets()) {
            Util.assertSucceeded(prism.getFeedHelper().submitEntity(URLS.SUBMIT_URL, data));
        }
        bundles[0].setRetry(getRetry(bundles[0], delay, delayUnits, retryType, retryAttempts));

        //submit and schedule process
        ServiceResponse response = prism.getProcessHelper().submitEntity(URLS.SUBMIT_URL, bundles[0].getProcessData());
        if (retryAttempts <= 0 || delay <= 0) {
            Util.assertFailed(response);
        } else {
            Util.assertSucceeded(response);
            HadoopUtil.deleteDirIfExists(lateDir, clusterFS);
            Util.lateDataReplenish(cluster, 20, 0);
            List<String> initialData = Util.getHadoopLateData(cluster, Util.getInputFeedFromBundle(bundles[0]));
            Util.assertSucceeded(prism.getProcessHelper().schedule(URLS.SCHEDULE_URL, bundles[0].getProcessData()));
            //now wait till the process is over
            String bundleId = Util.getBundles(clusterOC,
                    Util.readEntityName(bundles[0].getProcessData()), ENTITY_TYPE.PROCESS).get(0);
            String status = Util.getBundleStatus(cluster, bundleId);

            waitTillCertainPercentageOfProcessHasStarted(cluster, bundleId, 25);

            Process oldProcessObject = bundles[0].getProcessObject();

            Retry retry = bundles[0].getProcessObject().getRetry();
            retry.setDelay(new Frequency("minutes(" + (retry.getDelay().getFrequency() + 1) + ")"));

            bundles[0].setRetry(retry);

            System.out.println("going to update process at:" + DateTime.now(DateTimeZone.UTC));
            Assert.assertTrue(prism.getProcessHelper().update(Util.readEntityName(bundles[0].getProcessData()),
                            bundles[0].getProcessData()).getMessage()
                    .contains("updated successfully"), "process was not updated successfully");
            String newBundleId = InstanceUtil.getLatestBundleID(cluster,
                    Util.readEntityName(bundles[0].getProcessData()), ENTITY_TYPE.PROCESS);

            Assert.assertEquals(bundleId, newBundleId, "its creating a new bundle!!!");

            //now to validate all failed instances to check if they were retried or not.
            validateRetry(cluster, bundleId, bundles[0].getProcessObject().getRetry().getAttempts());
            if (bundles[0].getProcessObject().getRetry().getAttempts() > 0) {
                checkIfRetriesWereTriggeredCorrectly(cluster, retryType, delay, bundleId);
            }
        }
    }


    @Test(dataProvider = "DP", groups = {"0.2.2", "retry"}, enabled = false)
    public void testRetryInProcessLowerDelayUpdate(String retryType, int delay, String delayUnits,
                                                   int retryAttempts) throws Exception {
        displayInputs("testRetryInProcessUpdate", delay, retryType, retryAttempts);

        String feed = Util.setFeedPathValue(Util.getInputFeedFromBundle(bundles[0]), latePath);
        feed = Util.insertLateFeedValue(feed, "8", "minutes");
        bundles[0].getDataSets().remove(Util.getInputFeedFromBundle(bundles[0]));
        bundles[0].getDataSets().add(feed);
        bundles[0].submitClusters(prism);

        for (String data : bundles[0].getDataSets()) {
            Util.assertSucceeded(
                    prism.getFeedHelper().submitEntity(URLS.SUBMIT_URL, data));
        }

        bundles[0].setRetry(getRetry(bundles[0], delay, delayUnits, retryType, retryAttempts));

        //submit and schedule process
        ServiceResponse response = prism.getProcessHelper().submitEntity(URLS.SUBMIT_URL, bundles[0].getProcessData());

        if (retryAttempts <= 0 || delay <= 0) {
            Util.assertFailed(response);
        } else {
            Util.assertSucceeded(response);
            HadoopUtil.deleteDirIfExists(lateDir, clusterFS);
            Util.lateDataReplenish(cluster, 20, 0);
            List<String> initialData = Util.getHadoopLateData(cluster, Util.getInputFeedFromBundle(bundles[0]));
            Util.assertSucceeded(prism.getProcessHelper().schedule(URLS.SCHEDULE_URL, bundles[0].getProcessData()));
            //now wait till the process is over
            String bundleId = Util.getBundles(clusterOC,
                    Util.readEntityName(bundles[0].getProcessData()), ENTITY_TYPE.PROCESS).get(0);
            String status = Util.getBundleStatus(cluster, bundleId);

            waitTillCertainPercentageOfProcessHasStarted(cluster, bundleId, 25);

            Process oldProcessObject = bundles[0].getProcessObject();

            Retry retry = bundles[0].getProcessObject().getRetry();
            retry.setDelay(new Frequency("minutes(" + (retry.getDelay().getFrequency() - 1) + ")"));

            bundles[0].setRetry(retry);

            System.out.println("going to update process at:" + DateTime.now(DateTimeZone.UTC));
            Assert.assertTrue(prism.getProcessHelper()
                    .update(Util.readEntityName(bundles[0].getProcessData()),
                            bundles[0].getProcessData()).getMessage()
                    .contains("updated successfully"),
                    "process was not updated successfully");
            String newBundleId = InstanceUtil
                    .getLatestBundleID(cluster, Util.readEntityName(bundles[0].getProcessData()), ENTITY_TYPE.PROCESS);

            Assert.assertEquals(bundleId, newBundleId, "its creating a new bundle!!!");

            //now to validate all failed instances to check if they were retried or not.
            validateRetry(cluster, bundleId, bundles[0].getProcessObject().getRetry().getAttempts());
            if (bundles[0].getProcessObject().getRetry().getAttempts() > 0) {
                checkIfRetriesWereTriggeredCorrectly(cluster, retryType, delay, bundleId);
            }
        }
    }


    @Test(dataProvider = "DP", groups = {"0.2.2", "retry"}, enabled = false)
    public void testRetryInProcessZeroDelayUpdate(String retryType, int delay, String delayUnits,
                                                  int retryAttempts) throws Exception {
        displayInputs("testRetryInProcessUpdate", delay, retryType, retryAttempts);

        String feed = Util.setFeedPathValue(Util.getInputFeedFromBundle(bundles[0]), latePath);
        feed = Util.insertLateFeedValue(feed, "8", "minutes");
        bundles[0].getDataSets().remove(Util.getInputFeedFromBundle(bundles[0]));
        bundles[0].getDataSets().add(feed);
        bundles[0].submitClusters(prism);

        for (String data : bundles[0].getDataSets()) {
            Util.assertSucceeded(prism.getFeedHelper().submitEntity(URLS.SUBMIT_URL, data));
        }

        bundles[0].setRetry(getRetry(bundles[0], delay, delayUnits, retryType, retryAttempts));

        //submit and schedule process
        ServiceResponse response = prism.getProcessHelper().submitEntity(URLS.SUBMIT_URL, bundles[0].getProcessData());

        if (retryAttempts <= 0 || delay <= 0) {
            Util.assertFailed(response);
        } else {
            Util.assertSucceeded(response);
            HadoopUtil.deleteDirIfExists(lateDir, clusterFS);
            Util.lateDataReplenish(cluster, 20, 0);
            List<String> initialData = Util.getHadoopLateData(cluster, Util.getInputFeedFromBundle(bundles[0]));
            Util.assertSucceeded(prism.getProcessHelper().schedule(URLS.SCHEDULE_URL, bundles[0].getProcessData()));
            //now wait till the process is over
            String bundleId = Util.getBundles(clusterOC,
                    Util.readEntityName(bundles[0].getProcessData()), ENTITY_TYPE.PROCESS).get(0);
            String status = Util.getBundleStatus(cluster, bundleId);

            waitTillCertainPercentageOfProcessHasStarted(cluster, bundleId, 25);
            Process oldProcessObject = bundles[0].getProcessObject();

            Retry retry = bundles[0].getProcessObject().getRetry();
            retry.setDelay(new Frequency("minutes(0)"));

            bundles[0].setRetry(retry);

            System.out.println("going to update process at:" + DateTime.now(DateTimeZone.UTC));
            Assert.assertFalse(prism.getProcessHelper().update(Util.readEntityName(bundles[0].getProcessData())
                            , bundles[0].getProcessData()).getMessage().contains("updated successfully"),
                    "process was updated successfully!!!");
            String newBundleId = InstanceUtil.getLatestBundleID(cluster,
                    Util.readEntityName(bundles[0].getProcessData()), ENTITY_TYPE.PROCESS);

            Assert.assertEquals(bundleId, newBundleId, "its creating a new bundle!!!");

            //now to validate all failed instances to check if they were retried or not.
            validateRetry(cluster, bundleId, bundles[0].getProcessObject().getRetry().getAttempts());
            if (bundles[0].getProcessObject().getRetry().getAttempts() > 0) {
                checkIfRetriesWereTriggeredCorrectly(cluster, retryType, delay, bundleId);
            }
        }
    }


    @Test(dataProvider = "DP", groups = {"0.2.2", "retry"}, enabled = false)
    public void testRetryInSimpleFailureCase(String retryType, int delay, String delayUnits,
                                             int retryAttempts) throws Exception {
        displayInputs("testRetryInSimpleFailureCase", delay, retryType, retryAttempts);

        String feed = Util.setFeedPathValue(Util.getInputFeedFromBundle(bundles[0]), latePath);
        feed = Util.insertLateFeedValue(feed, "8", "minutes");
        bundles[0].getDataSets().remove(Util.getInputFeedFromBundle(bundles[0]));
        bundles[0].getDataSets().add(feed);
        bundles[0].setRetry(getRetry(bundles[0], delay, delayUnits, retryType, retryAttempts));

        bundles[0].submitClusters(prism);

        for (String data : bundles[0].getDataSets()) {
            Util.assertSucceeded(prism.getFeedHelper().submitEntity(URLS.SUBMIT_URL, data));
        }

        bundles[0].setProcessLatePolicy(null);
        //submit and schedule process
        ServiceResponse response = prism.getProcessHelper().submitEntity(URLS.SUBMIT_URL, bundles[0].getProcessData());

        if (retryAttempts <= 0 || delay <= 0) {
            Util.assertFailed(response);
        } else {
            Util.assertSucceeded(response);
            HadoopUtil.deleteDirIfExists(lateDir, clusterFS);
            Util.lateDataReplenish(cluster, 20, 0);
            List<String> initialData = Util.getHadoopLateData(cluster, Util.getInputFeedFromBundle(bundles[0]));
            Util.assertSucceeded(prism.getProcessHelper().schedule(URLS.SCHEDULE_URL, bundles[0].getProcessData()));
            //now wait till the process is over
            String bundleId = Util.getBundles(clusterOC,
                    Util.readEntityName(bundles[0].getProcessData()), ENTITY_TYPE.PROCESS).get(0);
            String status = Util.getBundleStatus(cluster, bundleId);

            //now to validate all failed instances to check if they were retried or not.
            validateRetry(cluster, bundleId, bundles[0].getProcessObject().getRetry().getAttempts());
            if (bundles[0].getProcessObject().getRetry().getAttempts() > 0) {
                checkIfRetriesWereTriggeredCorrectly(cluster, retryType, delay, bundleId);
            }
        }
    }


    @Test(dataProvider = "DP", groups = {"0.2.2", "retry"}, enabled = false)
    public void testUserRetryWhileAutomaticRetriesHappen(String retryType, int delay, String delayUnits,
                                                         int retryAttempts) throws Exception {
        displayInputs("testUserRetryWhileAutomaticRetriesHappen", delay, retryType, retryAttempts);

        DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd/hh:mm");

        String feed = Util.setFeedPathValue(Util.getInputFeedFromBundle(bundles[0]), latePath);
        feed = Util.insertLateFeedValue(feed, "8", "minutes");
        bundles[0].getDataSets().remove(Util.getInputFeedFromBundle(bundles[0]));
        bundles[0].getDataSets().add(feed);
        bundles[0].setRetry(getRetry(bundles[0], delay, delayUnits, retryType, retryAttempts));

        bundles[0].submitClusters(prism);

        for (String data : bundles[0].getDataSets()) {
            Util.assertSucceeded(prism.getFeedHelper().submitEntity(URLS.SUBMIT_URL, data));
        }

        System.out.println("process dates: " + startDate + "," + endDate);

        //submit and schedule process
        ServiceResponse response = prism.getProcessHelper().submitEntity(URLS.SUBMIT_URL, bundles[0].getProcessData());

        if (retryAttempts <= 0 || delay <= 0) {
            Util.assertFailed(response);
        } else {
            Util.assertSucceeded(response);
            HadoopUtil.deleteDirIfExists(lateDir, clusterFS);
            Util.lateDataReplenish(cluster, 20, 0);
            List<String> initialData = Util.getHadoopLateData(cluster, Util.getInputFeedFromBundle(bundles[0]));
            Util.assertSucceeded(prism.getProcessHelper().schedule(URLS.SCHEDULE_URL, bundles[0].getProcessData()));

            //now wait till the process is over
            String bundleId = Util.getBundles(clusterOC,
                    Util.readEntityName(bundles[0].getProcessData()), ENTITY_TYPE.PROCESS).get(0);
            String status = Util.getBundleStatus(cluster, bundleId);

            while (!validateFailureRetries(cluster, getDefaultOozieCoord(cluster, bundleId), 1)) {
                //keep waiting
            }

            //now start firing random retries
            System.out.println("now firing user reruns:");
            for (int i = 0; i < 1; i++) {
                prism.getProcessHelper()
                        .getProcessInstanceRerun(Util.readEntityName(bundles[0].getProcessData()),
                                "?start=" + formatter.print(startDate).replace("/", "T") + "Z" +
                                        "&end=" + formatter.print(endDate).replace("/", "T") + "Z");
            }
            //now to validate all failed instances to check if they were retried or not.
            validateRetry(cluster, bundleId, bundles[0].getProcessObject().getRetry().getAttempts());
            if (bundles[0].getProcessObject().getRetry().getAttempts() > 0) {
                checkIfRetriesWereTriggeredCorrectly(cluster, retryType, delay, bundleId);
            }
        }
    }

    @Test(dataProvider = "DP", groups = {"0.2.2", "retry"}, enabled = false)
    public void testUserRetryAfterAutomaticRetriesHappen(String retryType, int delay, String delayUnits,
                                                         int retryAttempts) throws Exception {
        displayInputs("testUserRetryAfterAutomaticRetriesHappen", delay, retryType, retryAttempts);

        DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd/hh:mm");

        String feed = Util.setFeedPathValue(Util.getInputFeedFromBundle(bundles[0]), latePath);
        feed = Util.insertLateFeedValue(feed, "8", "minutes");
        bundles[0].getDataSets().remove(Util.getInputFeedFromBundle(bundles[0]));
        bundles[0].getDataSets().add(feed);
        bundles[0].setRetry(getRetry(bundles[0], delay, delayUnits, retryType, retryAttempts));

        bundles[0].submitClusters(prism);

        for (String data : bundles[0].getDataSets()) {
            Util.assertSucceeded(prism.getFeedHelper().submitEntity(URLS.SUBMIT_URL, data));
        }

        System.out.println("process dates: " + startDate + "," + endDate);

        //submit and schedule process
        ServiceResponse response = prism.getProcessHelper().submitEntity(URLS.SUBMIT_URL, bundles[0].getProcessData());

        if (retryAttempts <= 0 || delay <= 0) {
            Util.assertFailed(response);
        } else {
            Util.assertSucceeded(response);
            HadoopUtil.deleteDirIfExists(lateDir, clusterFS);
            Util.lateDataReplenish(cluster, 20, 0);
            List<String> initialData = Util.getHadoopLateData(cluster, Util.getInputFeedFromBundle(bundles[0]));
            Util.assertSucceeded(prism.getProcessHelper().schedule(URLS.SCHEDULE_URL, bundles[0].getProcessData()));
            //now wait till the process is over
            String bundleId = Util.getBundles(clusterOC,
                    Util.readEntityName(bundles[0].getProcessData()), ENTITY_TYPE.PROCESS).get(0);
            String status = Util.getBundleStatus(cluster, bundleId);

            //now to validate all failed instances to check if they were retried or not.
            validateRetry(cluster, bundleId, bundles[0].getProcessObject().getRetry().getAttempts());

            System.out.println("now firing user reruns:");

            DateTime[] dateBoundaries = getFailureTimeBoundaries(cluster, bundleId);
            ProcessInstancesResult piResult = prism.getProcessHelper()
                    .getProcessInstanceRerun(Util.readEntityName(bundles[0].getProcessData()),
                            "?start=" + formatter.print(dateBoundaries[0]).replace("/", "T") +
                                    "Z" + "&end=" + formatter.print(dateBoundaries[dateBoundaries.length - 1])
                                            .replace("/", "T") + "Z");

            Assert.assertEquals(piResult.getStatusCode(), 0, "rerun failed miserably! you fool!");

            System.out.println();
            validateRetry(cluster, bundleId, bundles[0].getProcessObject().getRetry().getAttempts() + 1);

            if (bundles[0].getProcessObject().getRetry().getAttempts() > 0) {
                checkIfRetriesWereTriggeredCorrectly(cluster, retryType, delay, bundleId);
            }
        }
    }

    @Test(dataProvider = "DP", groups = {"0.2.2", "retry"}, enabled = false)
    public void testRetryInSuspendedAndResumeCaseWithLateData(String retryType, int delay,
                                                              String delayUnits, int retryAttempts) throws Exception {
        displayInputs("testRetryInSuspendedAndResumeCaseWithLateData", delay, retryType, retryAttempts);

        String feed = Util.setFeedPathValue(Util.getInputFeedFromBundle(bundles[0]), latePath);
        feed = Util.insertLateFeedValue(feed, "10", "minutes");
        bundles[0].getDataSets().remove(Util.getInputFeedFromBundle(bundles[0]));
        bundles[0].getDataSets().add(feed);
        bundles[0].setRetry(getRetry(bundles[0], delay, delayUnits, retryType, retryAttempts));

        bundles[0].submitClusters(prism);

        for (String data : bundles[0].getDataSets()) {
            Util.assertSucceeded(prism.getFeedHelper().submitEntity(URLS.SUBMIT_URL, data));
        }

        //submit and schedule process
        ServiceResponse response = prism.getProcessHelper().submitEntity(URLS.SUBMIT_URL, bundles[0].getProcessData());

        if (retryAttempts <= 0 || delay <= 0) {
            Util.assertFailed(response);
        } else {
            Util.assertSucceeded(response);
            HadoopUtil.deleteDirIfExists(lateDir, clusterFS);
            Util.lateDataReplenish(cluster, 20, 0);
            List<String> initialData = Util.getHadoopLateData(cluster, Util.getInputFeedFromBundle(bundles[0]));
            Util.assertSucceeded(prism.getProcessHelper().schedule(URLS.SCHEDULE_URL, bundles[0].getProcessData()));
            String bundleId = Util.getBundles(clusterOC,
                    Util.readEntityName(bundles[0].getProcessData()), ENTITY_TYPE.PROCESS).get(0);
            List<DateTime> dates = null;

            do {
                dates = Util.getStartTimeForRunningCoordinators(cluster, bundleId);
            } while (dates == null);

            System.out.println("Start time: " + formatter.print(startDate));
            System.out.println("End time: " + formatter.print(endDate));
            System.out.println("candidate nominal time:" + formatter.print(dates.get(0)));
            DateTime now = dates.get(0);

            if (formatter.print(startDate).compareToIgnoreCase(formatter.print(dates.get(0))) > 0) {
                now = startDate;
            }

            //now wait till the process is over
            String status = Util.getBundleStatus(cluster, bundleId);

            while (!validateFailureRetries(cluster, getDefaultOozieCoord(cluster, bundleId), 1)) {
                //keep waiting
            }

            System.out.println("now suspending the process altogether....");

            Util.assertSucceeded(cluster.getProcessHelper().suspend(URLS.SUSPEND_URL, bundles[0].getProcessData()));

            HashMap<String, Integer> initialMap = getFailureRetriesForEachWorkflow(cluster,
                    getDefaultOozieCoord(cluster, bundleId));
            System.out.println("saved state of workflow retries");

            for (String key : initialMap.keySet()) {
                System.out.println(key + "," + initialMap.get(key));
            }

            Thread.sleep(600000);


            HashMap<String, Integer> finalMap = getFailureRetriesForEachWorkflow(cluster,
                    getDefaultOozieCoord(cluster, bundleId));
            System.out.println("final state of process looks like:");

            for (String key : finalMap.keySet()) {
                System.out.println(key + "," + finalMap.get(key));
            }

            Assert.assertEquals(initialMap.size(), finalMap.size(), "a new workflow retried while process was suspended!!!!");

            for (String key : initialMap.keySet()) {
                Assert.assertEquals(initialMap.get(key), finalMap.get(key), "values are different for workflow: " + key);
            }

            System.out.println("now resuming the process...");
            Util.assertSucceeded(cluster.getProcessHelper().resume(URLS.RESUME_URL, bundles[0].getProcessData()));

            //now to validate all failed instances to check if they were retried or not.
            validateRetry(cluster, bundleId, bundles[0].getProcessObject().getRetry().getAttempts());
            if (bundles[0].getProcessObject().getRetry().getAttempts() > 0) {
                checkIfRetriesWereTriggeredCorrectly(cluster, retryType, delay, bundleId);
            }
        }
    }


    @Test(dataProvider = "DP", groups = {"0.2.2", "retry"}, enabled = false)
    public void testRetryInLateDataCase(String retryType, int delay,
                                        String delayUnits, int retryAttempts) throws Exception {
        displayInputs("testRetryInLateDataCase", delay, retryType, retryAttempts);

        String feed = Util.setFeedPathValue(Util.getInputFeedFromBundle(bundles[0]), latePath);

        feed = Util.insertLateFeedValue(feed, getDelay(delay, retryAttempts, retryType).toString(), delayUnits);

        bundles[0].getDataSets().remove(Util.getInputFeedFromBundle(bundles[0]));
        bundles[0].getDataSets().add(feed);

        bundles[0].setRetry(getRetry(bundles[0], delay, delayUnits, retryType, retryAttempts));

        bundles[0].submitClusters(prism);

        for (String data : bundles[0].getDataSets()) {
            Util.assertSucceeded(prism.getFeedHelper().submitEntity(URLS.SUBMIT_URL, data));
        }

        //submit and schedule process
        ServiceResponse response = prism.getProcessHelper().submitEntity(URLS.SUBMIT_URL, bundles[0].getProcessData());

        if (retryAttempts <= 0 || delay <= 0) {
            Util.assertFailed(response);
        } else {
            Util.assertSucceeded(response);
            HadoopUtil.deleteDirIfExists(lateDir, clusterFS);
            Util.lateDataReplenish(cluster, 20, 0);
            List<String> initialData = Util.getHadoopLateData(cluster, Util.getInputFeedFromBundle(bundles[0]));
            Util.assertSucceeded(prism.getProcessHelper().schedule(URLS.SCHEDULE_URL, bundles[0].getProcessData()));
            String bundleId = Util.getBundles(clusterOC,
                    Util.readEntityName(bundles[0].getProcessData()), ENTITY_TYPE.PROCESS).get(0);
            List<DateTime> dates = null;

            do {
                dates = Util.getStartTimeForRunningCoordinators(cluster, bundleId);
            } while (dates == null);

            System.out.println("Start time: " + formatter.print(startDate));
            System.out.println("End time: " + formatter.print(endDate));
            System.out.println("candidate nominal time:" + formatter.print(dates.get(0)));
            DateTime now = dates.get(0);

            if (formatter.print(startDate).compareToIgnoreCase(formatter.print(dates.get(0))) > 0) {
                now = startDate;
            }

            //now wait till the process is over
            String status = Util.getBundleStatus(cluster, bundleId);

            boolean inserted = false;

            int tryingToInsertData = 0;

            while (true) {
                //keep dancing
                String insertionFolder = Util.findFolderBetweenGivenTimeStamps(now, now.plusMinutes(5), initialData);

                if (!inserted && validateFailureRetries(cluster, getDefaultOozieCoord(cluster, bundleId),
                                bundles[0].getProcessObject().getRetry().getAttempts())) {
                    System.out.println("inserting data in folder " + insertionFolder + " at " + DateTime.now());
                    Util.injectMoreData(cluster, insertionFolder, OSUtil.OOZIE_EXAMPLE_INPUT_DATA + "lateData");
                    inserted = true;
                    break;
                }

                Thread.sleep(1000);
                tryingToInsertData++;
                status = Util.getBundleStatus(cluster, bundleId);
            }

            //now to validate all failed instances to check if they were retried or not.
            validateRetry(cluster, bundleId,
                    bundles[0].getProcessObject().getRetry().getAttempts());

            if (bundles[0].getProcessObject().getRetry().getAttempts() > 0) {
                checkIfRetriesWereTriggeredCorrectly(cluster, retryType, delay, bundleId);
            }
        }
    }


    @Test(dataProvider = "DP", groups = {"0.2.2", "retry"}, enabled = false)
    public void testRetryInDeleteAfterPartialRetryCase(String retryType, int delay, String delayUnits,
                                                       int retryAttempts) throws Exception {
        displayInputs("testRetryInDeleteAfterPartialRetryCase", delay, retryType, retryAttempts);

        String feed = Util.setFeedPathValue(Util.getInputFeedFromBundle(bundles[0]), latePath);
        feed = Util.insertLateFeedValue(feed, "1", "minutes");
        bundles[0].getDataSets().remove(Util.getInputFeedFromBundle(bundles[0]));
        bundles[0].getDataSets().add(feed);

        bundles[0].setRetry(getRetry(bundles[0], delay, delayUnits, retryType, retryAttempts));
        bundles[0].submitClusters(prism);

        for (String data : bundles[0].getDataSets()) {
            Util.assertSucceeded(prism.getFeedHelper().submitEntity(URLS.SUBMIT_URL, data));
        }

        

        //submit and schedule process
        ServiceResponse response = prism.getProcessHelper().submitEntity(URLS.SUBMIT_URL, bundles[0].getProcessData());
        if (retryAttempts <= 0 || delay <= 0) {
            Util.assertFailed(response);
        } else {
            Util.assertSucceeded(response);
            HadoopUtil.deleteDirIfExists(lateDir, clusterFS);
            Util.lateDataReplenish(cluster, 20, 0);
            List<String> initialData = Util.getHadoopLateData(cluster, Util.getInputFeedFromBundle(bundles[0]));
            Util.assertSucceeded(prism.getProcessHelper().schedule(URLS.SCHEDULE_URL, bundles[0].getProcessData()));
            //now wait till the process is over
            String bundleId = Util.getBundles(clusterOC,
                    Util.readEntityName(bundles[0].getProcessData()), ENTITY_TYPE.PROCESS).get(0);
            String status = Util.getBundleStatus(cluster, bundleId);

            validateRetry(cluster, bundleId, (bundles[0].getProcessObject().getRetry().getAttempts()) / 2);

            Util.assertSucceeded(prism.getProcessHelper().delete(URLS.DELETE_URL, (bundles[0].getProcessData())));

            if (retryType.equals("exp-backoff")) {
                TimeUnit.MINUTES.sleep(delay * ((retryAttempts -
                        (bundles[0].getProcessObject().getRetry().getAttempts()) / 2) ^ 2));
            } else {
                TimeUnit.MINUTES.sleep (delay * ((bundles[0].getProcessObject().getRetry().getAttempts()) -
                                (bundles[0].getProcessObject().getRetry().getAttempts()) / 2));
            }

            //now to validate all failed instances to check if they were retried or not.
            validateRetry(cluster, bundleId, (bundles[0].getProcessObject().getRetry().getAttempts()) / 2);

            if (bundles[0].getProcessObject().getRetry().getAttempts() > 0) {
                checkIfRetriesWereTriggeredCorrectly(cluster, retryType, delay, bundleId);
            }
        }
    }


    private void validateRetry(ColoHelper coloHelper, String bundleId, int maxNumberOfRetries)
    throws Exception {
        //validate that all failed processes were retried the specified number of times.
        int attempt = 0;
        boolean result = false;
        while (true) {
            result = ensureAllFailedInstancesHaveRetried(coloHelper, bundleId, maxNumberOfRetries);

            if (result || attempt > 60) {
                break;
            } else {
                Thread.sleep(1000);
                System.out.println("desired state not reached.This was attempt number: " + attempt);
                attempt++;
                Thread.sleep(5000);
            }
        }
        Assert.assertTrue(result, "all retries were not attempted correctly!");

    }


    private boolean validateFailureRetries(ColoHelper coloHelper, CoordinatorJob coordinator,
                                           int maxNumberOfRetries) throws Exception {

        if (maxNumberOfRetries < 0) {
            maxNumberOfRetries = 0;
        }

        HashMap<String, Boolean> workflowMap = new HashMap<String, Boolean>();

        OozieClient client =coloHelper.getFeedHelper().getOozieClient();

        if (coordinator.getActions().size() == 0) {
            return false;
        }

        for (CoordinatorAction action : coordinator.getActions()) {

            if (null == action.getExternalId()) {
                return false;
            }


            WorkflowJob actionInfo = client.getJobInfo(action.getExternalId());


            if (!(actionInfo.getStatus().equals(WorkflowJob.Status.SUCCEEDED) ||
                    actionInfo.getStatus().equals(WorkflowJob.Status.RUNNING))) {

                System.out.println("workflow " + actionInfo.getId() + " has action number: " +
                        actionInfo.getRun());
                if (actionInfo.getRun() == maxNumberOfRetries) {
                    workflowMap.put(actionInfo.getId(), true);
                } else {
                    Assert.assertTrue(actionInfo.getRun() < maxNumberOfRetries,
                            "The workflow exceeded the max number of retries specified for it!!!!");
                    workflowMap.put(actionInfo.getId(), false);
                }

            } else if (actionInfo.getStatus().equals(WorkflowJob.Status.SUCCEEDED)) {
                workflowMap.put(actionInfo.getId(), true);
            }
        }

        //first make sure that the map has all the entries for the coordinator:
        if (workflowMap.size() != coordinator.getActions().size()) {
            return false;
        } else {
            boolean result = true;

            for (String key : workflowMap.keySet()) {
                result &= workflowMap.get(key);
            }

            return result;
        }
    }

    private CoordinatorJob getDefaultOozieCoord(ColoHelper coloHelper, String bundleId) throws Exception {
        OozieClient client = coloHelper.getFeedHelper().getOozieClient();
        BundleJob bundlejob = client.getBundleJobInfo(bundleId);

        for (CoordinatorJob coord : bundlejob.getCoordinators()) {
            if (coord.getAppName().contains("DEFAULT")) {
                return client.getCoordJobInfo(coord.getId());
            }
        }
        return null;
    }

    @DataProvider(name = "DP")
    public Object[][] getData() throws Exception {

        String[] retryTypes = new String[]{"periodic", "exp-backoff"};//,"exp-backoff"
        int[] delays = new int[]{2, 0};//removing -1 since this should be checked at validation level while setting
        String[] delayUnits = new String[]{"minutes"};
        Integer[] retryAttempts = new Integer[]{2, 0, 3};//0,-1,2

        Object[][] testData = new Object[retryTypes.length * delays.length * delayUnits.length * retryAttempts.length][4];

        int i = 0;

        for (String retryType : retryTypes) {
            for (int delay : delays) {
                for (String delayUnit : delayUnits) {
                    for (int retry : retryAttempts) {
                        testData[i][0] = retryType;
                        testData[i][1] = delay;
                        testData[i][2] = delayUnit;
                        testData[i][3] = retry;
                        i++;
                    }
                }
            }
        }

        return testData;
    }


    private boolean allRelevantWorkflowsAreOver(ColoHelper coloHelper, String bundleId,
                                                String insertionFolder) throws Exception {
        boolean finished = true;
        OozieClient oozieClient = coloHelper.getProcessHelper().getOozieClient();
        BundleJob bundleJob = oozieClient.getBundleJobInfo(bundleId);
        for (CoordinatorJob job : bundleJob.getCoordinators()) {
            if (job.getAppName().contains("DEFAULT")) {

                CoordinatorJob coordJob = oozieClient.getCoordJobInfo(job.getId());

                for (CoordinatorAction action : coordJob.getActions()) {
                    CoordinatorAction actionMan = oozieClient.getCoordActionInfo(action.getId());

                    if (actionMan.getRunConf().contains(insertionFolder)) {
                        if ((actionMan.getStatus().equals(CoordinatorAction.Status.SUCCEEDED)) ||
                                actionMan.getStatus().equals(CoordinatorAction.Status.KILLED) ||
                                actionMan.getStatus().equals(CoordinatorAction.Status.FAILED)) {
                            System.out.println(
                                    "related workflow " + actionMan.getId() + " is over....");
                            finished &= true;
                        } else {
                            finished &= false;
                        }
                    }
                }
            }
        }

        return finished;
    }


    private boolean ensureAllFailedInstancesHaveRetried(ColoHelper coloHelper, String bundleId,
                                                        int maxNumberOfRetries) throws Exception {
        boolean retried = false;

        CoordinatorJob defaultCoordinator = getDefaultOozieCoord(coloHelper, bundleId);
        boolean retriedAllDefault = validateFailureRetries(coloHelper, defaultCoordinator, maxNumberOfRetries);
        if (retriedAllDefault) {
            return true;
        }
        return retried;
    }


    private void waitTillCertainPercentageOfProcessHasStarted(ColoHelper coloHelper,
                                                              String bundleId, int percentage) throws Exception {
        CoordinatorJob defaultCoordinator = getDefaultOozieCoord(coloHelper, bundleId);

        while (defaultCoordinator.getStatus().equals(CoordinatorJob.Status.PREP)) {
            defaultCoordinator = getDefaultOozieCoord(coloHelper, bundleId);
        }

        int totalCount = defaultCoordinator.getActions().size();

        int percentageConversion = (percentage * totalCount) / 100;

        while (percentageConversion > 0) {
            int doneBynow = 0;
            for (CoordinatorAction action : defaultCoordinator.getActions()) {
                CoordinatorAction actionInfo = getOozieActionInfo(coloHelper, action.getId());
                if (actionInfo.getStatus().equals(CoordinatorAction.Status.RUNNING)) {
                    doneBynow++;
                    if (doneBynow == percentageConversion) {
                        return;
                    }
                }
            }
        }
    }

    private void waitTillCertainPercentageOfProcessAreKilled(ColoHelper coloHelper, String bundleId,
                                                             int percentage) throws Exception {
        CoordinatorJob defaultCoordinator = getDefaultOozieCoord(coloHelper, bundleId);

        while (defaultCoordinator.getStatus().equals(CoordinatorJob.Status.PREP)) {
            defaultCoordinator = getDefaultOozieCoord(coloHelper, bundleId);
        }

        int totalCount = defaultCoordinator.getActions().size();

        int percentageConversion = (percentage * totalCount) / 100;

        while (true && percentageConversion > 0) {
            int doneBynow = 0;
            for (CoordinatorAction action : defaultCoordinator.getActions()) {
                CoordinatorAction actionInfo = getOozieActionInfo(coloHelper, action.getId());
                if (actionInfo.getStatus().equals(CoordinatorAction.Status.KILLED) ||
                        actionInfo.getStatus().equals(CoordinatorAction.Status.FAILED)) {
                    doneBynow++;
                    if (doneBynow == percentageConversion) {
                        return;
                    }
                }
            }
        }
    }


    private CoordinatorAction getOozieActionInfo(ColoHelper colohelper, String actionId) throws Exception {
        OozieClient client =colohelper.getProcessHelper().getOozieClient();
        return client.getCoordActionInfo(actionId);
    }


    private HashMap<String, Integer> getFailureRetriesForEachWorkflow(ColoHelper coloHelper, CoordinatorJob coordinator)
    throws Exception {
        OozieClient client = coloHelper.getClusterHelper().getOozieClient();
        HashMap<String, Integer> workflowRetryMap = new HashMap<String, Integer>();
        for (CoordinatorAction action : coordinator.getActions()) {

            if (null == action.getExternalId()) {
                continue;
            }

            WorkflowJob actionInfo = client.getJobInfo(action.getExternalId());
            System.out.println("adding workflow " + actionInfo.getId() + " to the map");
            workflowRetryMap.put(actionInfo.getId(), actionInfo.getRun());
        }
        return workflowRetryMap;
    }

    private DateTime[] getFailureTimeBoundaries(ColoHelper coloHelper, String bundleId) throws Exception {
        OozieClient client = coloHelper.getProcessHelper().getOozieClient();
        List<DateTime> dateList = new ArrayList<DateTime>();

        CoordinatorJob coordinator = getDefaultOozieCoord(coloHelper, bundleId);

        for (CoordinatorAction action : coordinator.getActions()) {
            if (action.getExternalId() != null) {

                WorkflowJob jobInfo = client.getJobInfo(action.getExternalId());
                if (jobInfo.getRun() > 0) {
                    dateList.add(new DateTime(jobInfo.getStartTime(), DateTimeZone.UTC));
                }
            }
        }
        Collections.sort(dateList);
        return dateList.toArray(new DateTime[dateList.size()]);
    }

    private void checkIfRetriesWereTriggeredCorrectly(ColoHelper coloHelper, String retryType,
                                                      int delay, String bundleId) throws Exception {
        //it is presumed that this delay here will be expressed in minutes. Hourly/daily is unfeasible to check :)
        checkRetryTriggerForCoordinator(coloHelper, retryType, delay, getDefaultOozieCoord(coloHelper, bundleId));
    }

    private void checkRetryTriggerForCoordinator(ColoHelper coloHelper, String retryType, int delay,
                                                 CoordinatorJob coordinator) throws Exception {

        DateTimeFormatter formatter = DateTimeFormat.forPattern("HH:mm:ss");

        for (CoordinatorAction action : coordinator.getActions()) {

            CoordinatorAction coordAction = getOozieActionInfo(coloHelper, action.getExternalId());
            if (!coordAction.getStatus().equals(CoordinatorAction.Status.SUCCEEDED)) {
                int expectedDelay = delay;
                //first get data from logs:
                List<String> instanceRetryTimes = Util.getInstanceRetryTimes(coloHelper, action.getExternalId());
                List<String> instanceFinishTimes = Util.getInstanceFinishTimes(coloHelper, action.getExternalId());

                System.out.println("finish times look like:");
                for (String line : instanceFinishTimes) {
                    System.out.println(line);
                }

                System.out.println("retry times look like:");
                for (String line : instanceRetryTimes) {
                    System.out.println(line);
                }

                System.out.println("checking timelines for retry type " + retryType + " for delay " +
                                expectedDelay + " for workflow id: " + action.getExternalId());

                if (retryType.equalsIgnoreCase("periodic")) {
                    //in this case the delay unit will always be a constant time diff
                    for (int i = 0; i < instanceFinishTimes.size() - 1; i++) {
                        DateTime temp = formatter.parseDateTime(instanceFinishTimes.get(i));

                        Assert.assertEquals(temp.plusMinutes(expectedDelay).getMillis(),
                                formatter.parseDateTime(instanceRetryTimes.get(i)).getMillis(),
                                5000, "oops! this is out of expected delay range for workflow id  " +
                                        action.getExternalId());
                    }
                } else {
                    //check for exponential
                    for (int i = 0; i < instanceFinishTimes.size() - 1; i++) {
                        DateTime temp = formatter.parseDateTime(instanceFinishTimes.get(i));
                        Assert.assertEquals(temp.plusMinutes(expectedDelay).getMillis(),
                                formatter.parseDateTime(instanceRetryTimes.get(i)).getMillis(),
                                5000,
                                "oops! this is out of expected delay range for workflow id " +
                                        action.getExternalId());
                        expectedDelay *= 2;
                    }
                }
            }
        }

    }

    private Retry getRetry(Bundle bundle, int delay, String delayUnits, String retryType,
                           int retryAttempts)
    throws Exception {
        Retry retry = new Retry();
        retry.setAttempts(retryAttempts);
        retry.setDelay(new Frequency(delayUnits + "(" + delay + ")"));
        retry.setPolicy(PolicyType.fromValue(retryType));
        return retry;
    }

    private void displayInputs(String m, int delay, String policy, int retryAttempts)
    throws Exception {
        System.out.println("******************");
        System.out.println("This test case is being executed with:");
        System.out.println("test case=" + m);
        System.out.println("delay=" + delay);
        System.out.println("policy=" + policy);
        System.out.println("retries=" + retryAttempts);
        System.out.println("******************");
    }

    private Integer getDelay(int delay, int attempts, String attemptType) throws Exception {
        if (delay == 0) {
            delay = 1;
        }
        if (attempts == 0) {
            attempts = 1;
        }

        if (attemptType.equals("exp-backoff")) {
            return (Math.abs(delay)) * (2 ^ (Math.abs(attempts)));
        } else {
            return Math.abs(delay * attempts);
        }

    }

}

