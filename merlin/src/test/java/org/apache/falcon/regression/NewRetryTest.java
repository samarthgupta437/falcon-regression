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
import org.apache.falcon.regression.core.enumsAndConstants.ENTITY_TYPE;
import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.core.util.Util.URLS;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Test(groups = "embedded")
public class NewRetryTest extends BaseTestClass {

    private Logger logger = Logger.getLogger(NewRetryTest.class);
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

        String feed = Util.setFeedPathValue(Util.getInputFeedFromBundle(bundles[0]), latePath);
        feed = Util.insertLateFeedValue(feed, "8", "minutes");
        bundles[0].getDataSets().remove(Util.getInputFeedFromBundle(bundles[0]));
        bundles[0].getDataSets().add(feed);
        bundles[0].submitClusters(prism);

        for (String data : bundles[0].getDataSets()) {
            Util.assertSucceeded(prism.getFeedHelper().submitEntity(URLS.SUBMIT_URL, data));
        }

        bundles[0].setRetry(getRetry(delay, delayUnits, retryType, retryAttempts));

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
            String bundleId = Util.getBundles(clusterOC,
                    Util.readEntityName(bundles[0].getProcessData()), ENTITY_TYPE.PROCESS).get(0);
            String status = Util.getBundleStatus(cluster, bundleId);

            waitTillCertainPercentageOfProcessHasStarted(clusterOC, bundleId, 25);

            Process oldProcessObject = bundles[0].getProcessObject();

            int defaultRetries = bundles[0].getProcessObject().getRetry().getAttempts();
            Retry retry = bundles[0].getProcessObject().getRetry();
            retry.setAttempts((0));

            bundles[0].setRetry(retry);

            logger.info("going to update process at:" + DateTime.now(DateTimeZone.UTC));
            prism.getProcessHelper().update((bundles[0].getProcessData()), bundles[0].getProcessData());
            String newBundleId = InstanceUtil.getLatestBundleID(cluster,
                    Util.readEntityName(bundles[0].getProcessData()), ENTITY_TYPE.PROCESS);

            Assert.assertEquals(bundleId, newBundleId, "its creating a new bundle!!!");

            //now to validate all failed instances to check if they were retried or not.
            validateRetry(clusterOC, bundleId, defaultRetries);
            checkIfRetriesWereTriggeredCorrectly(cluster, retryType, delay, bundleId);
        }
    }


    @Test(dataProvider = "DP", groups = {"0.2.2", "retry"}, enabled = true)
    public void testRetryInProcessLowerAttemptUpdate(String retryType, int delay, String delayUnits,
                                                     int retryAttempts) throws Exception {

        String feed = Util.setFeedPathValue(Util.getInputFeedFromBundle(bundles[0]), latePath);
        feed = Util.insertLateFeedValue(feed, "8", "minutes");
        bundles[0].getDataSets().remove(Util.getInputFeedFromBundle(bundles[0]));
        bundles[0].getDataSets().add(feed);
        bundles[0].submitClusters(prism);

        for (String data : bundles[0].getDataSets()) {
            Util.assertSucceeded(prism.getFeedHelper().submitEntity(URLS.SUBMIT_URL, data));
        }
        bundles[0].setRetry(getRetry(delay, delayUnits, retryType, retryAttempts));

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
            String bundleId = Util.getBundles(clusterOC,
                    Util.readEntityName(bundles[0].getProcessData()), ENTITY_TYPE.PROCESS).get(0);
            String status = Util.getBundleStatus(cluster, bundleId);

            boolean validation = false;
            for (int attempt = 0; attempt < 60; ++attempt) {
                validation =
                        validateFailureRetries(clusterOC, getDefaultOozieCoordinator(clusterOC, bundleId), 1);
                if (validation)
                    break;
                Thread.sleep(10000);
            }
            Assert.assertTrue(validation, "Failure Retry validation failed");

            Process oldProcessObject = bundles[0].getProcessObject();

            Retry retry = bundles[0].getProcessObject().getRetry();
            retry.setAttempts((retryAttempts - 2));

            bundles[0].setRetry(retry);

            logger.info("going to update process at:" + DateTime.now(DateTimeZone.UTC));

            if ((retryAttempts - 2) > 0) {
                Assert.assertTrue(prism.getProcessHelper().update((bundles[0].getProcessData()), bundles[0].getProcessData())
                                .getMessage().contains("updated successfully"), "process was not updated successfully");
                String newBundleId = InstanceUtil.getLatestBundleID(cluster,
                                Util.readEntityName(bundles[0].getProcessData()), ENTITY_TYPE.PROCESS);

                Assert.assertEquals(bundleId, newBundleId, "its creating a new bundle!!!");

                //now to validate all failed instances to check if they were retried or not.
                validateRetry(clusterOC, bundleId, retryAttempts - 2);
                if (bundles[0].getProcessObject().getRetry().getAttempts() > 0) {
                    checkIfRetriesWereTriggeredCorrectly(cluster, retryType, delay, bundleId);
                }
            }
        }
    }


    @Test(dataProvider = "DP", groups = {"0.2.2", "retry"}, enabled = false)
    public void testRetryInProcessLowerManageableAttemptUpdate(String retryType, int delay,
                                                               String delayUnits, int retryAttempts) throws Exception {

        String feed = Util.setFeedPathValue(Util.getInputFeedFromBundle(bundles[0]), latePath);
        feed = Util.insertLateFeedValue(feed, "8", "minutes");
        bundles[0].getDataSets().remove(Util.getInputFeedFromBundle(bundles[0]));
        bundles[0].getDataSets().add(feed);

        bundles[0].submitClusters(prism);

        for (String data : bundles[0].getDataSets()) {
            Util.assertSucceeded(prism.getFeedHelper().submitEntity(URLS.SUBMIT_URL, data));
        }
        bundles[0].setRetry(getRetry(delay, delayUnits, retryType, retryAttempts));

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

            while (!validateFailureRetries(clusterOC, getDefaultOozieCoordinator(clusterOC, bundleId), 1)) {
                //wait
            }

            Process oldProcessObject = bundles[0].getProcessObject();

            Retry retry = bundles[0].getProcessObject().getRetry();
            retry.setAttempts((retryAttempts - 1));

            bundles[0].setRetry(retry);

            logger.info("going to update process at:" + DateTime.now(DateTimeZone.UTC));
            Assert.assertTrue(prism.getProcessHelper()
                            .update((bundles[0].getProcessData()), bundles[0].getProcessData())
                            .getMessage().contains("updated successfully"),
                    "process was not updated successfully");
            String newBundleId = InstanceUtil.getLatestBundleID(cluster,
                   Util.readEntityName(bundles[0].getProcessData()), ENTITY_TYPE.PROCESS);

            Assert.assertEquals(bundleId, newBundleId, "its creating a new bundle!!!");

            //now to validate all failed instances to check if they were retried or not.
            validateRetry(clusterOC, bundleId, retryAttempts - 1);
            if (bundles[0].getProcessObject().getRetry().getAttempts() > 0) {
                checkIfRetriesWereTriggeredCorrectly(cluster, retryType, delay, bundleId);
            }
        }
    }

    @Test(dataProvider = "DP", groups = {"0.2.2", "retry"}, enabled = false)
    public void testRetryInProcessLowerBoundaryAttemptUpdate(String retryType, int delay,
                                                             String delayUnits, int retryAttempts) throws Exception {

        String feed = Util.setFeedPathValue(Util.getInputFeedFromBundle(bundles[0]), latePath);
        feed = Util.insertLateFeedValue(feed, "8", "minutes");
        bundles[0].getDataSets().remove(Util.getInputFeedFromBundle(bundles[0]));
        bundles[0].getDataSets().add(feed);
        bundles[0].submitClusters(prism);

        for (String data : bundles[0].getDataSets()) {
            Util.assertSucceeded(prism.getFeedHelper().submitEntity(URLS.SUBMIT_URL, data));
        }
        bundles[0].setRetry(getRetry(delay, delayUnits, retryType, retryAttempts));

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

            while (!validateFailureRetries(clusterOC, getDefaultOozieCoordinator(clusterOC, bundleId), 2)) {
                //wait
            }

            Process oldProcessObject = bundles[0].getProcessObject();

            Retry retry = bundles[0].getProcessObject().getRetry();
            retry.setAttempts((2));

            bundles[0].setRetry(retry);

            logger.info("going to update process at:" + DateTime.now(DateTimeZone.UTC));
            Assert.assertTrue(
                    prism.getProcessHelper()
                            .update((bundles[0].getProcessData()), bundles[0].getProcessData())
                            .getMessage().contains("updated successfully"),
                    "process was not updated successfully");
            String newBundleId = InstanceUtil.getLatestBundleID(cluster,
                    Util.readEntityName(bundles[0].getProcessData()), ENTITY_TYPE.PROCESS);

            Assert.assertEquals(bundleId, newBundleId, "its creating a new bundle!!!");

            //now to validate all failed instances to check if they were retried or not.
            validateRetry(clusterOC, bundleId, 2);
            if (bundles[0].getProcessObject().getRetry().getAttempts() > 0) {
                checkIfRetriesWereTriggeredCorrectly(cluster, retryType, delay, bundleId);
            }
        }
    }

    @Test(dataProvider = "DP", groups = {"0.2.2", "retry"}, enabled = false)
    public void testRetryInProcessUpdate(String retryType, int delay, String delayUnits,
                                         int retryAttempts) throws Exception {

        String feed = Util.setFeedPathValue(Util.getInputFeedFromBundle(bundles[0]), latePath);
        feed = Util.insertLateFeedValue(feed, "8", "minutes");
        bundles[0].getDataSets().remove(Util.getInputFeedFromBundle(bundles[0]));
        bundles[0].getDataSets().add(feed);
        bundles[0].submitClusters(prism);

        for (String data : bundles[0].getDataSets()) {
            Util.assertSucceeded(prism.getFeedHelper().submitEntity(URLS.SUBMIT_URL, data));
        }
        bundles[0].setRetry(getRetry(delay, delayUnits, retryType, retryAttempts));

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

            waitTillCertainPercentageOfProcessHasStarted(clusterOC, bundleId, 25);

            Process oldProcessObject = bundles[0].getProcessObject();

            Retry retry = bundles[0].getProcessObject().getRetry();
            retry.setAttempts((4));

            bundles[0].setRetry(retry);

            logger.info("going to update process at:" + DateTime.now(DateTimeZone.UTC));
            Assert.assertTrue(prism.getProcessHelper()
                    .update(Util.readEntityName(bundles[0].getProcessData()),
                            bundles[0].getProcessFilePath()).getMessage()
                    .contains("updated successfully"), "process was not updated successfully");
            String newBundleId = InstanceUtil.getLatestBundleID(cluster,
                    Util.readEntityName(bundles[0].getProcessData()), ENTITY_TYPE.PROCESS);

            Assert.assertEquals(bundleId, newBundleId, "its creating a new bundle!!!");

            //now to validate all failed instances to check if they were retried or not.
            validateRetry(clusterOC, bundleId, 4);
            if (bundles[0].getProcessObject().getRetry().getAttempts() > 0) {
                checkIfRetriesWereTriggeredCorrectly(cluster, retryType, delay, bundleId);
            }
        }
    }


    @Test(dataProvider = "DP", groups = {"0.2.2", "retry"}, enabled = false)
    public void testRetryInProcessHigherDelayUpdate(String retryType, int delay, String delayUnits,
                                                    int retryAttempts) throws Exception {

        String feed = Util.setFeedPathValue(Util.getInputFeedFromBundle(bundles[0]), latePath);
        feed = Util.insertLateFeedValue(feed, "8", "minutes");
        bundles[0].getDataSets().remove(Util.getInputFeedFromBundle(bundles[0]));
        bundles[0].getDataSets().add(feed);
        bundles[0].submitClusters(prism);

        for (String data : bundles[0].getDataSets()) {
            Util.assertSucceeded(prism.getFeedHelper().submitEntity(URLS.SUBMIT_URL, data));
        }
        bundles[0].setRetry(getRetry(delay, delayUnits, retryType, retryAttempts));

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

            waitTillCertainPercentageOfProcessHasStarted(clusterOC, bundleId, 25);

            Process oldProcessObject = bundles[0].getProcessObject();

            Retry retry = bundles[0].getProcessObject().getRetry();
            retry.setDelay(new Frequency("minutes(" + (retry.getDelay().getFrequency() + 1) + ")"));

            bundles[0].setRetry(retry);

            logger.info("going to update process at:" + DateTime.now(DateTimeZone.UTC));
            Assert.assertTrue(prism.getProcessHelper().update(Util.readEntityName(bundles[0].getProcessData()),
                            bundles[0].getProcessData()).getMessage()
                    .contains("updated successfully"), "process was not updated successfully");
            String newBundleId = InstanceUtil.getLatestBundleID(cluster,
                    Util.readEntityName(bundles[0].getProcessData()), ENTITY_TYPE.PROCESS);

            Assert.assertEquals(bundleId, newBundleId, "its creating a new bundle!!!");

            //now to validate all failed instances to check if they were retried or not.
            validateRetry(clusterOC, bundleId, bundles[0].getProcessObject().getRetry().getAttempts());
            if (bundles[0].getProcessObject().getRetry().getAttempts() > 0) {
                checkIfRetriesWereTriggeredCorrectly(cluster, retryType, delay, bundleId);
            }
        }
    }


    @Test(dataProvider = "DP", groups = {"0.2.2", "retry"}, enabled = false)
    public void testRetryInProcessLowerDelayUpdate(String retryType, int delay, String delayUnits,
                                                   int retryAttempts) throws Exception {

        String feed = Util.setFeedPathValue(Util.getInputFeedFromBundle(bundles[0]), latePath);
        feed = Util.insertLateFeedValue(feed, "8", "minutes");
        bundles[0].getDataSets().remove(Util.getInputFeedFromBundle(bundles[0]));
        bundles[0].getDataSets().add(feed);
        bundles[0].submitClusters(prism);

        for (String data : bundles[0].getDataSets()) {
            Util.assertSucceeded(
                    prism.getFeedHelper().submitEntity(URLS.SUBMIT_URL, data));
        }

        bundles[0].setRetry(getRetry(delay, delayUnits, retryType, retryAttempts));

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

            waitTillCertainPercentageOfProcessHasStarted(clusterOC, bundleId, 25);

            Process oldProcessObject = bundles[0].getProcessObject();

            Retry retry = bundles[0].getProcessObject().getRetry();
            retry.setDelay(new Frequency("minutes(" + (retry.getDelay().getFrequency() - 1) + ")"));

            bundles[0].setRetry(retry);

            logger.info("going to update process at:" + DateTime.now(DateTimeZone.UTC));
            Assert.assertTrue(prism.getProcessHelper()
                    .update(Util.readEntityName(bundles[0].getProcessData()),
                            bundles[0].getProcessData()).getMessage()
                    .contains("updated successfully"),
                    "process was not updated successfully");
            String newBundleId = InstanceUtil
                    .getLatestBundleID(cluster, Util.readEntityName(bundles[0].getProcessData()), ENTITY_TYPE.PROCESS);

            Assert.assertEquals(bundleId, newBundleId, "its creating a new bundle!!!");

            //now to validate all failed instances to check if they were retried or not.
            validateRetry(clusterOC, bundleId, bundles[0].getProcessObject().getRetry().getAttempts());
            if (bundles[0].getProcessObject().getRetry().getAttempts() > 0) {
                checkIfRetriesWereTriggeredCorrectly(cluster, retryType, delay, bundleId);
            }
        }
    }


    @Test(dataProvider = "DP", groups = {"0.2.2", "retry"}, enabled = false)
    public void testRetryInProcessZeroDelayUpdate(String retryType, int delay, String delayUnits,
                                                  int retryAttempts) throws Exception {

        String feed = Util.setFeedPathValue(Util.getInputFeedFromBundle(bundles[0]), latePath);
        feed = Util.insertLateFeedValue(feed, "8", "minutes");
        bundles[0].getDataSets().remove(Util.getInputFeedFromBundle(bundles[0]));
        bundles[0].getDataSets().add(feed);
        bundles[0].submitClusters(prism);

        for (String data : bundles[0].getDataSets()) {
            Util.assertSucceeded(prism.getFeedHelper().submitEntity(URLS.SUBMIT_URL, data));
        }

        bundles[0].setRetry(getRetry(delay, delayUnits, retryType, retryAttempts));

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

            waitTillCertainPercentageOfProcessHasStarted(clusterOC, bundleId, 25);
            Process oldProcessObject = bundles[0].getProcessObject();

            Retry retry = bundles[0].getProcessObject().getRetry();
            retry.setDelay(new Frequency("minutes(0)"));

            bundles[0].setRetry(retry);

            logger.info("going to update process at:" + DateTime.now(DateTimeZone.UTC));
            Assert.assertFalse(prism.getProcessHelper().update(Util.readEntityName(bundles[0].getProcessData())
                            , bundles[0].getProcessData()).getMessage().contains("updated successfully"),
                    "process was updated successfully!!!");
            String newBundleId = InstanceUtil.getLatestBundleID(cluster,
                    Util.readEntityName(bundles[0].getProcessData()), ENTITY_TYPE.PROCESS);

            Assert.assertEquals(bundleId, newBundleId, "its creating a new bundle!!!");

            //now to validate all failed instances to check if they were retried or not.
            validateRetry(clusterOC, bundleId, bundles[0].getProcessObject().getRetry().getAttempts());
            if (bundles[0].getProcessObject().getRetry().getAttempts() > 0) {
                checkIfRetriesWereTriggeredCorrectly(cluster, retryType, delay, bundleId);
            }
        }
    }


    @Test(dataProvider = "DP", groups = {"0.2.2", "retry"}, enabled = false)
    public void testRetryInSimpleFailureCase(String retryType, int delay, String delayUnits,
                                             int retryAttempts) throws Exception {

        String feed = Util.setFeedPathValue(Util.getInputFeedFromBundle(bundles[0]), latePath);
        feed = Util.insertLateFeedValue(feed, "8", "minutes");
        bundles[0].getDataSets().remove(Util.getInputFeedFromBundle(bundles[0]));
        bundles[0].getDataSets().add(feed);
        bundles[0].setRetry(getRetry(delay, delayUnits, retryType, retryAttempts));

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
            validateRetry(clusterOC, bundleId, bundles[0].getProcessObject().getRetry().getAttempts());
            if (bundles[0].getProcessObject().getRetry().getAttempts() > 0) {
                checkIfRetriesWereTriggeredCorrectly(cluster, retryType, delay, bundleId);
            }
        }
    }


    @Test(dataProvider = "DP", groups = {"0.2.2", "retry"}, enabled = false)
    public void testUserRetryWhileAutomaticRetriesHappen(String retryType, int delay, String delayUnits,
                                                         int retryAttempts) throws Exception {

        DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd/hh:mm");

        String feed = Util.setFeedPathValue(Util.getInputFeedFromBundle(bundles[0]), latePath);
        feed = Util.insertLateFeedValue(feed, "8", "minutes");
        bundles[0].getDataSets().remove(Util.getInputFeedFromBundle(bundles[0]));
        bundles[0].getDataSets().add(feed);
        bundles[0].setRetry(getRetry(delay, delayUnits, retryType, retryAttempts));

        bundles[0].submitClusters(prism);

        for (String data : bundles[0].getDataSets()) {
            Util.assertSucceeded(prism.getFeedHelper().submitEntity(URLS.SUBMIT_URL, data));
        }

        logger.info("process dates: " + startDate + "," + endDate);

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

            while (!validateFailureRetries(clusterOC, getDefaultOozieCoordinator(clusterOC, bundleId), 1)) {
                //keep waiting
            }

            //now start firing random retries
            logger.info("now firing user reruns:");
            for (int i = 0; i < 1; i++) {
                prism.getProcessHelper()
                        .getProcessInstanceRerun(Util.readEntityName(bundles[0].getProcessData()),
                                "?start=" + formatter.print(startDate).replace("/", "T") + "Z" +
                                        "&end=" + formatter.print(endDate).replace("/", "T") + "Z");
            }
            //now to validate all failed instances to check if they were retried or not.
            validateRetry(clusterOC, bundleId, bundles[0].getProcessObject().getRetry().getAttempts());
            if (bundles[0].getProcessObject().getRetry().getAttempts() > 0) {
                checkIfRetriesWereTriggeredCorrectly(cluster, retryType, delay, bundleId);
            }
        }
    }

    @Test(dataProvider = "DP", groups = {"0.2.2", "retry"}, enabled = false)
    public void testUserRetryAfterAutomaticRetriesHappen(String retryType, int delay, String delayUnits,
                                                         int retryAttempts) throws Exception {

        DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd/hh:mm");

        String feed = Util.setFeedPathValue(Util.getInputFeedFromBundle(bundles[0]), latePath);
        feed = Util.insertLateFeedValue(feed, "8", "minutes");
        bundles[0].getDataSets().remove(Util.getInputFeedFromBundle(bundles[0]));
        bundles[0].getDataSets().add(feed);
        bundles[0].setRetry(getRetry(delay, delayUnits, retryType, retryAttempts));

        bundles[0].submitClusters(prism);

        for (String data : bundles[0].getDataSets()) {
            Util.assertSucceeded(prism.getFeedHelper().submitEntity(URLS.SUBMIT_URL, data));
        }

        logger.info("process dates: " + startDate + "," + endDate);

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
            Util.readEntityName(bundles[0].getProcessData()),
              ENTITY_TYPE.PROCESS).get(0);
            String status = Util.getBundleStatus(cluster, bundleId);

            //now to validate all failed instances to check if they were retried or not.
            validateRetry(clusterOC, bundleId, bundles[0].getProcessObject().getRetry().getAttempts());

            logger.info("now firing user reruns:");

            DateTime[] dateBoundaries = getFailureTimeBoundaries(clusterOC, bundleId);
            ProcessInstancesResult piResult = prism.getProcessHelper()
                    .getProcessInstanceRerun(Util.readEntityName(bundles[0].getProcessData()),
                            "?start=" + formatter.print(dateBoundaries[0]).replace("/", "T") +
                                    "Z" + "&end=" + formatter.print(dateBoundaries[dateBoundaries.length - 1])
                                            .replace("/", "T") + "Z");

            Assert.assertEquals(piResult.getStatusCode(), 0, "rerun failed miserably! you fool!");

            validateRetry(clusterOC, bundleId, bundles[0].getProcessObject().getRetry().getAttempts() + 1);

            if (bundles[0].getProcessObject().getRetry().getAttempts() > 0) {
                checkIfRetriesWereTriggeredCorrectly(cluster, retryType, delay, bundleId);
            }
        }
    }

    @Test(dataProvider = "DP", groups = {"0.2.2", "retry"}, enabled = false)
    public void testRetryInSuspendedAndResumeCaseWithLateData(String retryType, int delay,
                                                              String delayUnits, int retryAttempts) throws Exception {

        String feed = Util.setFeedPathValue(Util.getInputFeedFromBundle(bundles[0]), latePath);
        feed = Util.insertLateFeedValue(feed, "10", "minutes");
        bundles[0].getDataSets().remove(Util.getInputFeedFromBundle(bundles[0]));
        bundles[0].getDataSets().add(feed);
        bundles[0].setRetry(getRetry(delay, delayUnits, retryType, retryAttempts));

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

            logger.info("Start time: " + formatter.print(startDate));
            logger.info("End time: " + formatter.print(endDate));
            logger.info("candidate nominal time:" + formatter.print(dates.get(0)));
            DateTime now = dates.get(0);

            if (formatter.print(startDate).compareToIgnoreCase(formatter.print(dates.get(0))) > 0) {
                now = startDate;
            }

            //now wait till the process is over
            String status = Util.getBundleStatus(cluster, bundleId);

            while (!validateFailureRetries(clusterOC, getDefaultOozieCoordinator(clusterOC, bundleId), 1)) {
                //keep waiting
            }

            logger.info("now suspending the process altogether....");

            Util.assertSucceeded(cluster.getProcessHelper().suspend(URLS.SUSPEND_URL, bundles[0].getProcessData()));

            HashMap<String, Integer> initialMap = getFailureRetriesForEachWorkflow(
                    clusterOC, getDefaultOozieCoordinator(clusterOC, bundleId));
            logger.info("saved state of workflow retries");

            for (String key : initialMap.keySet()) {
                logger.info(key + "," + initialMap.get(key));
            }

            Thread.sleep(600000);


            HashMap<String, Integer> finalMap = getFailureRetriesForEachWorkflow(
                    clusterOC, getDefaultOozieCoordinator(clusterOC, bundleId));
            logger.info("final state of process looks like:");

            for (String key : finalMap.keySet()) {
                logger.info(key + "," + finalMap.get(key));
            }

            Assert.assertEquals(initialMap.size(), finalMap.size(), "a new workflow retried while process was suspended!!!!");

            for (String key : initialMap.keySet()) {
                Assert.assertEquals(initialMap.get(key), finalMap.get(key), "values are different for workflow: " + key);
            }

            logger.info("now resuming the process...");
            Util.assertSucceeded(cluster.getProcessHelper().resume(URLS.RESUME_URL, bundles[0].getProcessData()));

            //now to validate all failed instances to check if they were retried or not.
            validateRetry(clusterOC, bundleId, bundles[0].getProcessObject().getRetry().getAttempts());
            if (bundles[0].getProcessObject().getRetry().getAttempts() > 0) {
                checkIfRetriesWereTriggeredCorrectly(cluster, retryType, delay, bundleId);
            }
        }
    }


    @Test(dataProvider = "DP", groups = {"0.2.2", "retry"}, enabled = false)
    public void testRetryInLateDataCase(String retryType, int delay,
                                        String delayUnits, int retryAttempts) throws Exception {

        String feed = Util.setFeedPathValue(Util.getInputFeedFromBundle(bundles[0]), latePath);

        feed = Util.insertLateFeedValue(feed, getDelay(delay, retryAttempts, retryType).toString(), delayUnits);

        bundles[0].getDataSets().remove(Util.getInputFeedFromBundle(bundles[0]));
        bundles[0].getDataSets().add(feed);

        bundles[0].setRetry(getRetry(delay, delayUnits, retryType, retryAttempts));

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

            logger.info("Start time: " + formatter.print(startDate));
            logger.info("End time: " + formatter.print(endDate));
            logger.info("candidate nominal time:" + formatter.print(dates.get(0)));
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

                if (!inserted && validateFailureRetries(clusterOC, getDefaultOozieCoordinator(clusterOC, bundleId),
                                bundles[0].getProcessObject().getRetry().getAttempts())) {
                    logger.info("inserting data in folder " + insertionFolder + " at " + DateTime.now());
                    Util.injectMoreData(cluster, insertionFolder, OSUtil.OOZIE_EXAMPLE_INPUT_DATA + "lateData");
                    inserted = true;
                    break;
                }

                Thread.sleep(1000);
                tryingToInsertData++;
                status = Util.getBundleStatus(cluster, bundleId);
            }

            //now to validate all failed instances to check if they were retried or not.
            validateRetry(clusterOC, bundleId,
                    bundles[0].getProcessObject().getRetry().getAttempts());

            if (bundles[0].getProcessObject().getRetry().getAttempts() > 0) {
                checkIfRetriesWereTriggeredCorrectly(cluster, retryType, delay, bundleId);
            }
        }
    }


    @Test(dataProvider = "DP", groups = {"0.2.2", "retry"}, enabled = false)
    public void testRetryInDeleteAfterPartialRetryCase(String retryType, int delay, String delayUnits,
                                                       int retryAttempts) throws Exception {

        String feed = Util.setFeedPathValue(Util.getInputFeedFromBundle(bundles[0]), latePath);
        feed = Util.insertLateFeedValue(feed, "1", "minutes");
        bundles[0].getDataSets().remove(Util.getInputFeedFromBundle(bundles[0]));
        bundles[0].getDataSets().add(feed);

        bundles[0].setRetry(getRetry(delay, delayUnits, retryType, retryAttempts));
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

            validateRetry(clusterOC, bundleId, (bundles[0].getProcessObject().getRetry().getAttempts()) / 2);

            Util.assertSucceeded(prism.getProcessHelper().delete(URLS.DELETE_URL, (bundles[0].getProcessData())));

            if (retryType.equals("exp-backoff")) {
                TimeUnit.MINUTES.sleep(delay * ((retryAttempts -
                        (bundles[0].getProcessObject().getRetry().getAttempts()) / 2) ^ 2));
            } else {
                TimeUnit.MINUTES.sleep (delay * ((bundles[0].getProcessObject().getRetry().getAttempts()) -
                                (bundles[0].getProcessObject().getRetry().getAttempts()) / 2));
            }

            //now to validate all failed instances to check if they were retried or not.
            validateRetry(clusterOC, bundleId, (bundles[0].getProcessObject().getRetry().getAttempts()) / 2);

            if (bundles[0].getProcessObject().getRetry().getAttempts() > 0) {
                checkIfRetriesWereTriggeredCorrectly(cluster, retryType, delay, bundleId);
            }
        }
    }


    private void validateRetry(OozieClient oozieClient, String bundleId, int maxNumberOfRetries)
    throws Exception {
        //validate that all failed processes were retried the specified number of times.
        for(int i = 0; i < 60 && getDefaultOozieCoordinator(oozieClient, bundleId) == null; ++i ) {
            TimeUnit.SECONDS.sleep(10);
        }
        final CoordinatorJob defaultCoordinator = getDefaultOozieCoordinator(oozieClient, bundleId);
        Assert.assertNotNull(defaultCoordinator, "Unexpected value of defaultCoordinator");

        for(int i = 0; i < 60 && validateFailureRetries(oozieClient, defaultCoordinator, maxNumberOfRetries); ++i) {
            logger.info("desired state not reached, attempt number: " + i);
            TimeUnit.SECONDS.sleep(10);
        }
        Assert.fail("all retries were not attempted correctly!");
    }


    private boolean validateFailureRetries(OozieClient oozieClient, CoordinatorJob coordinator,
                                           int maxNumberOfRetries) throws Exception {

        if (maxNumberOfRetries < 0) {
            maxNumberOfRetries = 0;
        }

        HashMap<String, Boolean> workflowMap = new HashMap<String, Boolean>();

        if (coordinator.getActions().size() == 0) {
            return false;
        }

        for (CoordinatorAction action : coordinator.getActions()) {

            if (null == action.getExternalId()) {
                return false;
            }


            WorkflowJob actionInfo = oozieClient.getJobInfo(action.getExternalId());


            if (!(actionInfo.getStatus().equals(WorkflowJob.Status.SUCCEEDED) ||
                    actionInfo.getStatus().equals(WorkflowJob.Status.RUNNING))) {

                logger.info("workflow " + actionInfo.getId() + " has action number: " +
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

    public CoordinatorJob getDefaultOozieCoordinator(OozieClient oozieClient, String bundleId) throws Exception {
        BundleJob bundlejob = oozieClient.getBundleJobInfo(bundleId);

        for (CoordinatorJob coord : bundlejob.getCoordinators()) {
            if (coord.getAppName().contains("DEFAULT")) {
                return oozieClient.getCoordJobInfo(coord.getId());
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

    private void waitTillCertainPercentageOfProcessHasStarted(OozieClient oozieClient, String bundleId, int percentage) throws Exception {
        CoordinatorJob defaultCoordinator = getDefaultOozieCoordinator(oozieClient, bundleId);

        for (int i = 0; i < 120 && defaultCoordinator.getStatus() == CoordinatorJob.Status.PREP; ++i) {
            TimeUnit.SECONDS.sleep(10);
        }
        Assert.assertNotEquals(defaultCoordinator.getStatus(), CoordinatorJob.Status.PREP,
                "Unexpected state for coordinator job: " + defaultCoordinator.getId());
        int totalCount = defaultCoordinator.getActions().size();

        int percentageConversion = (percentage * totalCount) / 100;

        while (percentageConversion > 0) {
            int doneBynow = 0;
            for (CoordinatorAction action : defaultCoordinator.getActions()) {
                CoordinatorAction actionInfo = oozieClient.getCoordActionInfo(action.getId());
                if (actionInfo.getStatus().equals(CoordinatorAction.Status.RUNNING)) {
                    doneBynow++;
                    if (doneBynow == percentageConversion) {
                        return;
                    }
                }
            }
        }
    }


    private HashMap<String, Integer> getFailureRetriesForEachWorkflow(OozieClient oozieClient, CoordinatorJob coordinator)
    throws Exception {
        HashMap<String, Integer> workflowRetryMap = new HashMap<String, Integer>();
        for (CoordinatorAction action : coordinator.getActions()) {

            if (null == action.getExternalId()) {
                continue;
            }

            WorkflowJob actionInfo = oozieClient.getJobInfo(action.getExternalId());
            logger.info("adding workflow " + actionInfo.getId() + " to the map");
            workflowRetryMap.put(actionInfo.getId(), actionInfo.getRun());
        }
        return workflowRetryMap;
    }

    private DateTime[] getFailureTimeBoundaries(OozieClient oozieClient, String bundleId) throws Exception {
        List<DateTime> dateList = new ArrayList<DateTime>();

        CoordinatorJob coordinator = getDefaultOozieCoordinator(oozieClient, bundleId);

        for (CoordinatorAction action : coordinator.getActions()) {
            if (action.getExternalId() != null) {

                WorkflowJob jobInfo = oozieClient.getJobInfo(action.getExternalId());
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

        final DateTimeFormatter formatter = DateTimeFormat.forPattern("HH:mm:ss");

        final OozieClient oozieClient = coloHelper.getFeedHelper().getOozieClient();
        final CoordinatorJob coordinator = getDefaultOozieCoordinator(oozieClient, bundleId);

        for (CoordinatorAction action : coordinator.getActions()) {
            CoordinatorAction coordAction = oozieClient.getCoordActionInfo(action.getExternalId());
            if (!coordAction.getStatus().equals(CoordinatorAction.Status.SUCCEEDED)) {
                int expectedDelay = delay;
                //first get data from logs:
                List<String> instanceRetryTimes = Util.getInstanceRetryTimes(coloHelper, action.getExternalId());
                List<String> instanceFinishTimes = Util.getInstanceFinishTimes(coloHelper, action.getExternalId());

                logger.info("finish times look like:");
                for (String line : instanceFinishTimes) {
                    logger.info(line);
                }

                logger.info("retry times look like:");
                for (String line : instanceRetryTimes) {
                    logger.info(line);
                }

                logger.info("checking timelines for retry type " + retryType + " for delay " +
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

    private Retry getRetry(int delay, String delayUnits, String retryType,
                           int retryAttempts)
    throws Exception {
        Retry retry = new Retry();
        retry.setAttempts(retryAttempts);
        retry.setDelay(new Frequency(delayUnits + "(" + delay + ")"));
        retry.setPolicy(PolicyType.fromValue(retryType));
        return retry;
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

