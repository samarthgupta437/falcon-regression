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
package org.apache.falcon.regression;


import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.generated.dependencies.Frequency;
import org.apache.falcon.regression.core.generated.process.PolicyType;
import org.apache.falcon.regression.core.generated.process.Process;
import org.apache.falcon.regression.core.generated.process.Retry;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.helpers.PrismHelper;
import org.apache.falcon.regression.core.response.ProcessInstancesResult;
import org.apache.falcon.regression.core.response.ServiceResponse;
import org.apache.falcon.regression.core.supportClasses.ENTITY_TYPE;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.core.util.Util.URLS;
import org.apache.oozie.client.BundleJob;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.client.XOozieClient;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.testng.Assert;
import org.testng.TestNGException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

public class NewRetryTest {

    PrismHelper prismHelper = new PrismHelper("prism.properties");
    ColoHelper UA1ColoHelper = new ColoHelper("mk-qa.config.properties");
    ColoHelper UA2ColoHelper = new ColoHelper("ivoryqa-1.config.properties");
    ColoHelper UA3ColoHelper = new ColoHelper("gs1001.config.properties");

    DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy/MM/dd/HH/mm");

    @BeforeMethod(alwaysRun = true)
    public void testName(Method method) throws Exception {
        Util.print("test name: " + method.getName());

    }


    @Test(dataProvider = "DP", groups = {"0.2.2", "retry"}, enabled = true)
    public void testRetryInProcessZeroAttemptUpdate(Bundle bundle, String retryType, int delay,
                                                    String delayUnits,
                                                    int retryAttempts)
    throws Exception {

        bundle = new Bundle(bundle, UA3ColoHelper);
        displayInputs("testRetryInProcessUpdate", delay, retryType, retryAttempts);
        try {

            System.setProperty("java.security.krb5.realm", "");
            System.setProperty("java.security.krb5.kdc", "");

            String feed = Util.setFeedPathValue(Util.getInputFeedFromBundle(bundle),
                    "/lateDataTest/testFolders/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
            feed = Util.insertLateFeedValue(feed, "8", "minutes");
            bundle.getDataSets().remove(Util.getInputFeedFromBundle(bundle));
            bundle.getDataSets().add(feed);

            //inject data into folders for processing
//        Util.HDFSCleanup(UA3ColoHelper,"lateDataTest/testFolders/");
//        Util.lateDataReplenish(UA3ColoHelper,20,0,0);
//        List<String> initialData=Util.getHadoopLateData(UA3ColoHelper,
// Util.getInputFeedFromBundle(bundle));

            bundle.generateUniqueBundle();

            submitClusters(bundle);

            for (String data : bundle.getDataSets()) {
                Util.assertSucceeded(
                        prismHelper.getFeedHelper().submitEntity(URLS.SUBMIT_URL, data));
            }

            //set process start and end date
            DateTime startDate = new DateTime(DateTimeZone.UTC).plusMinutes(1);
            DateTime endDate = new DateTime(DateTimeZone.UTC).plusMinutes(2);

            bundle.setProcessValidity(startDate, endDate);

            bundle.setRetry(getRetry(bundle, delay, delayUnits, retryType, retryAttempts));

            //submit and schedule process
            //ServiceResponse response=prismHelper.getProcessHelper().submitAndSchedule(URLS
            // .SUBMIT_AND_SCHEDULE_URL,
            // bundle.getProcessData());
            ServiceResponse response =
                    prismHelper.getProcessHelper()
                            .submitEntity(URLS.SUBMIT_URL, bundle.getProcessData());

            if (retryAttempts <= 0 || delay <= 0) {
                Util.assertFailed(response);
            } else {

                Util.assertSucceeded(response);
                // lets create data now:
                Util.HDFSCleanup(UA3ColoHelper, "lateDataTest/testFolders/");
                Util.lateDataReplenish(UA3ColoHelper, 20, 0);
                List<String> initialData =
                        Util.getHadoopLateData(UA3ColoHelper, Util.getInputFeedFromBundle(bundle));

                //schedule process
                Util.assertSucceeded(
                        prismHelper.getProcessHelper()
                                .schedule(URLS.SCHEDULE_URL, bundle.getProcessData()));

                //now wait till the process is over

                String bundleId = Util.getBundles(UA3ColoHelper.getFeedHelper().getOozieClient(),
                        Util.readEntityName(bundle.getProcessData()), ENTITY_TYPE.PROCESS).get(0);
                String status = Util.getBundleStatus(UA3ColoHelper, bundleId);

                waitTillCertainPercentageOfProcessHasStarted(UA3ColoHelper, bundleId, 25);

                org.apache.falcon.regression.core.generated.process.Process oldProcessObject =
                        bundle.getProcessObject();

                int defaultRetries = bundle.getProcessObject().getRetry().getAttempts();
                Retry retry = new Retry();
                retry = bundle.getProcessObject().getRetry();
                retry.setAttempts((0));

                bundle.setRetry(retry);
                //bundle.writeBundleToFiles();

                System.out.println("going to update process at:" + DateTime.now(DateTimeZone.UTC));
                prismHelper.getProcessHelper()
                        .update((bundle.getProcessData()), bundle.getProcessData());
                //Assert.assertFalse(prismHelper.getProcessHelper().update(Util.readEntityName
                // (bundle.getProcessData
                // ()),bundle.getProcessFilePath()).contains("updated successfully"),
                // "process was not updated successfully");
                String newBundleId = InstanceUtil
                        .getLatestBundleID(UA3ColoHelper,
                                Util.readEntityName(bundle.getProcessData()), ENTITY_TYPE.PROCESS);

                Assert.assertEquals(bundleId, newBundleId, "its creating a new bundle!!!");

                //now to validate all failed instances to check if they were retried or not.
                validateRetry(UA3ColoHelper, bundleId, defaultRetries);
                //validateRetry(newBundleId,bundle.getProcessObject().getRetry().getAttempts());

                checkIfRetriesWereTriggeredCorrectly(UA3ColoHelper, retryType, delay, bundleId);

            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getCause());
        } finally {
            bundle.deleteBundle(prismHelper);
        }
    }


    @Test(dataProvider = "DP", groups = {"0.2.2", "retry"}, enabled = true)
    public void testRetryInProcessLowerAttemptUpdate(Bundle bundle, String retryType, int delay,
                                                     String delayUnits,
                                                     int retryAttempts)
    throws Exception {
        bundle = new Bundle(bundle, UA3ColoHelper);
        displayInputs("testRetryInProcessUpdate", delay, retryType, retryAttempts);
        try {

            System.setProperty("java.security.krb5.realm", "");
            System.setProperty("java.security.krb5.kdc", "");

            String feed = Util.setFeedPathValue(Util.getInputFeedFromBundle(bundle),
                    "/lateDataTest/testFolders/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
            feed = Util.insertLateFeedValue(feed, "8", "minutes");
            bundle.getDataSets().remove(Util.getInputFeedFromBundle(bundle));
            bundle.getDataSets().add(feed);

            //inject data into folders for processing
//        Util.HDFSCleanup(UA3ColoHelper,"lateDataTest/testFolders/");
//        Util.lateDataReplenish(UA3ColoHelper,20,0,0);
//        List<String> initialData=Util.getHadoopLateData(UA3ColoHelper,
// Util.getInputFeedFromBundle(bundle));

            bundle.generateUniqueBundle();

            submitClusters(bundle);

            for (String data : bundle.getDataSets()) {
                Util.assertSucceeded(
                        prismHelper.getFeedHelper().submitEntity(URLS.SUBMIT_URL, data));
            }

            //set process start and end date
            DateTime startDate = new DateTime(DateTimeZone.UTC).plusMinutes(1);
            DateTime endDate = new DateTime(DateTimeZone.UTC).plusMinutes(2);

            bundle.setProcessValidity(startDate, endDate);

            bundle.setRetry(getRetry(bundle, delay, delayUnits, retryType, retryAttempts));

            //submit and schedule process
            ServiceResponse response =
                    prismHelper.getProcessHelper()
                            .submitEntity(URLS.SUBMIT_URL, bundle.getProcessData());

            if (retryAttempts <= 0 || delay <= 0) {
                Util.assertFailed(response);
            } else {

                Util.assertSucceeded(response);
                Util.HDFSCleanup(UA3ColoHelper, "lateDataTest/testFolders/");
                Util.lateDataReplenish(UA3ColoHelper, 20, 0);
                List<String> initialData =
                        Util.getHadoopLateData(UA3ColoHelper, Util.getInputFeedFromBundle(bundle));
                //now wait till the process is over

                Util.assertSucceeded(
                        prismHelper.getProcessHelper()
                                .schedule(URLS.SCHEDULE_URL, bundle.getProcessData()));
                String bundleId = Util.getBundles(UA3ColoHelper.getFeedHelper().getOozieClient(),
                        Util.readEntityName(bundle.getProcessData()), ENTITY_TYPE.PROCESS).get(0);
                String status = Util.getBundleStatus(UA3ColoHelper, bundleId);

                //waitTillCertainPercentageOfProcessHasStarted(bundleId,25);

                while (!validateFailureRetries(UA3ColoHelper,
                        getDefaultOozieCoord(UA3ColoHelper, bundleId), 1)) {
                    //wait
                }

                org.apache.falcon.regression.core.generated.process.Process oldProcessObject =
                        bundle.getProcessObject();

                Retry retry = new Retry();
                retry = bundle.getProcessObject().getRetry();
                retry.setAttempts((retryAttempts - 2));

                bundle.setRetry(retry);
                //bundle.writeBundleToFiles();

                System.out.println("going to update process at:" + DateTime.now(DateTimeZone.UTC));

                if ((retryAttempts - 2) > 0) {

                    Assert.assertTrue(
                            prismHelper.getProcessHelper()
                                    .update((bundle.getProcessData()), bundle.getProcessData())
                                    .getMessage().contains("updated successfully"),
                            "process was not updated successfully");
                    String newBundleId = InstanceUtil
                            .getLatestBundleID(UA3ColoHelper,
                                    Util.readEntityName(bundle.getProcessData()), ENTITY_TYPE.PROCESS);

                    Assert.assertEquals(bundleId, newBundleId, "its creating a new bundle!!!");

                    //now to validate all failed instances to check if they were retried or not.
                    validateRetry(UA3ColoHelper, bundleId, retryAttempts - 2);
                    //validateRetry(newBundleId,bundle.getProcessObject().getRetry().getAttempts());
                    if (bundle.getProcessObject().getRetry().getAttempts() > 0) {
                        checkIfRetriesWereTriggeredCorrectly(UA3ColoHelper, retryType, delay,
                                bundleId);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getCause());
        } finally {
            bundle.deleteBundle(prismHelper);
        }
    }


    @Test(dataProvider = "DP", groups = {"0.2.2", "retry"}, enabled = false)
    public void testRetryInProcessLowerManageableAttemptUpdate(Bundle bundle, String retryType,
                                                               int delay,
                                                               String delayUnits, int retryAttempts)
    throws Exception {
        bundle = new Bundle(bundle, UA3ColoHelper);
        displayInputs("testRetryInProcessUpdate", delay, retryType, retryAttempts);
        try {

            System.setProperty("java.security.krb5.realm", "");
            System.setProperty("java.security.krb5.kdc", "");

            String feed = Util.setFeedPathValue(Util.getInputFeedFromBundle(bundle),
                    "/lateDataTest/testFolders/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
            feed = Util.insertLateFeedValue(feed, "8", "minutes");
            bundle.getDataSets().remove(Util.getInputFeedFromBundle(bundle));
            bundle.getDataSets().add(feed);

            //inject data into folders for processing
//        Util.HDFSCleanup(UA3ColoHelper,"lateDataTest/testFolders/");
//        Util.lateDataReplenish(UA3ColoHelper,20,0,0);
//        List<String> initialData=Util.getHadoopLateData(UA3ColoHelper,
// Util.getInputFeedFromBundle(bundle));

            bundle.generateUniqueBundle();

            submitClusters(bundle);

            for (String data : bundle.getDataSets()) {
                Util.assertSucceeded(
                        prismHelper.getFeedHelper().submitEntity(URLS.SUBMIT_URL, data));
            }

            //set process start and end date
            DateTime startDate = new DateTime(DateTimeZone.UTC).plusMinutes(1);
            DateTime endDate = new DateTime(DateTimeZone.UTC).plusMinutes(2);

            bundle.setProcessValidity(startDate, endDate);

            bundle.setRetry(getRetry(bundle, delay, delayUnits, retryType, retryAttempts));

            //submit and schedule process
            ServiceResponse response =
                    prismHelper.getProcessHelper()
                            .submitEntity(URLS.SUBMIT_URL, bundle.getProcessData());

            if (retryAttempts <= 0 || delay <= 0) {
                Util.assertFailed(response);
            } else {

                Util.assertSucceeded(response);
                Util.HDFSCleanup(UA3ColoHelper, "lateDataTest/testFolders/");
                Util.lateDataReplenish(UA3ColoHelper, 20, 0);
                List<String> initialData =
                        Util.getHadoopLateData(UA3ColoHelper, Util.getInputFeedFromBundle(bundle));

                Util.assertSucceeded(
                        prismHelper.getProcessHelper()
                                .schedule(URLS.SCHEDULE_URL, bundle.getProcessData()));
                //now wait till the process is over
                String bundleId = Util.getBundles(UA3ColoHelper.getFeedHelper().getOozieClient(),
                        Util.readEntityName(bundle.getProcessData()), ENTITY_TYPE.PROCESS).get(0);;
                String status = Util.getBundleStatus(UA3ColoHelper, bundleId);

                //waitTillCertainPercentageOfProcessHasStarted(bundleId,25);

                while (!validateFailureRetries(UA3ColoHelper,
                        getDefaultOozieCoord(UA3ColoHelper, bundleId), 1)) {
                    //wait
                }

                Process oldProcessObject = bundle.getProcessObject();

                Retry retry = new Retry();
                retry = bundle.getProcessObject().getRetry();
                retry.setAttempts((retryAttempts - 1));

                bundle.setRetry(retry);
                //bundle.writeBundleToFiles();

                System.out.println("going to update process at:" + DateTime.now(DateTimeZone.UTC));
                Assert.assertTrue(
                        prismHelper.getProcessHelper()
                                .update((bundle.getProcessData()), bundle.getProcessData())
                                .getMessage().contains("updated successfully"),
                        "process was not updated successfully");
                String newBundleId = InstanceUtil
                        .getLatestBundleID(UA3ColoHelper,
                                Util.readEntityName(bundle.getProcessData()), ENTITY_TYPE.PROCESS);

                Assert.assertEquals(bundleId, newBundleId, "its creating a new bundle!!!");

                //now to validate all failed instances to check if they were retried or not.
                validateRetry(UA3ColoHelper, bundleId, retryAttempts - 1);
                //validateRetry(newBundleId,bundle.getProcessObject().getRetry().getAttempts());
                if (bundle.getProcessObject().getRetry().getAttempts() > 0) {
                    checkIfRetriesWereTriggeredCorrectly(UA3ColoHelper, retryType, delay, bundleId);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getCause());
        } finally {
            bundle.deleteBundle(prismHelper);
        }
    }

    @Test(dataProvider = "DP", groups = {"0.2.2", "retry"}, enabled = false)
    public void testRetryInProcessLowerBoundaryAttemptUpdate(Bundle bundle, String retryType,
                                                             int delay,
                                                             String delayUnits, int retryAttempts)
    throws Exception {
        bundle = new Bundle(bundle, UA3ColoHelper);
        displayInputs("testRetryInProcessUpdate", delay, retryType, retryAttempts);
        try {

            System.setProperty("java.security.krb5.realm", "");
            System.setProperty("java.security.krb5.kdc", "");

            String feed = Util.setFeedPathValue(Util.getInputFeedFromBundle(bundle),
                    "/lateDataTest/testFolders/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
            feed = Util.insertLateFeedValue(feed, "8", "minutes");
            bundle.getDataSets().remove(Util.getInputFeedFromBundle(bundle));
            bundle.getDataSets().add(feed);

            //inject data into folders for processing
//        Util.HDFSCleanup(UA3ColoHelper,"lateDataTest/testFolders/");
//        Util.lateDataReplenish(UA3ColoHelper,20,0,0);
//        List<String> initialData=Util.getHadoopLateData(UA3ColoHelper,
// Util.getInputFeedFromBundle(bundle));

            bundle.generateUniqueBundle();

            submitClusters(bundle);

            for (String data : bundle.getDataSets()) {
                Util.assertSucceeded(
                        prismHelper.getFeedHelper().submitEntity(URLS.SUBMIT_URL, data));
            }

            //set process start and end date
            DateTime startDate = new DateTime(DateTimeZone.UTC).plusMinutes(1);
            DateTime endDate = new DateTime(DateTimeZone.UTC).plusMinutes(2);

            bundle.setProcessValidity(startDate, endDate);

            bundle.setRetry(getRetry(bundle, delay, delayUnits, retryType, retryAttempts));

            //submit and schedule process
            ServiceResponse response =
                    prismHelper.getProcessHelper()
                            .submitEntity(URLS.SUBMIT_URL, bundle.getProcessData());

            if (retryAttempts <= 0 || delay <= 0) {
                Util.assertFailed(response);
            } else {

                Util.assertSucceeded(response);
                Util.HDFSCleanup(UA3ColoHelper, "lateDataTest/testFolders/");
                Util.lateDataReplenish(UA3ColoHelper, 20, 0);
                List<String> initialData =
                        Util.getHadoopLateData(UA3ColoHelper, Util.getInputFeedFromBundle(bundle));
                Util.assertSucceeded(
                        prismHelper.getProcessHelper()
                                .schedule(URLS.SCHEDULE_URL, bundle.getProcessData()));

                //now wait till the process is over
                String bundleId = Util.getBundles(UA3ColoHelper.getFeedHelper().getOozieClient(),
                        Util.readEntityName(bundle.getProcessData()), ENTITY_TYPE.PROCESS).get(0);
                String status = Util.getBundleStatus(UA3ColoHelper, bundleId);

                //waitTillCertainPercentageOfProcessHasStarted(bundleId,25);

                while (!validateFailureRetries(UA3ColoHelper,
                        getDefaultOozieCoord(UA3ColoHelper, bundleId), 2)) {
                    //wait
                }

                Process oldProcessObject = bundle.getProcessObject();

                Retry retry = new Retry();
                retry = bundle.getProcessObject().getRetry();
                retry.setAttempts((2));

                bundle.setRetry(retry);
                //bundle.writeBundleToFiles();

                System.out.println("going to update process at:" + DateTime.now(DateTimeZone.UTC));
                Assert.assertTrue(
                        prismHelper.getProcessHelper()
                                .update((bundle.getProcessData()), bundle.getProcessData())
                                .getMessage().contains("updated successfully"),
                        "process was not updated successfully");
                String newBundleId = InstanceUtil
                        .getLatestBundleID(UA3ColoHelper,
                                Util.readEntityName(bundle.getProcessData()), ENTITY_TYPE.PROCESS);

                Assert.assertEquals(bundleId, newBundleId, "its creating a new bundle!!!");

                //now to validate all failed instances to check if they were retried or not.
                validateRetry(UA3ColoHelper, bundleId, 2);
                //validateRetry(newBundleId,bundle.getProcessObject().getRetry().getAttempts());
                if (bundle.getProcessObject().getRetry().getAttempts() > 0) {
                    checkIfRetriesWereTriggeredCorrectly(UA3ColoHelper, retryType, delay, bundleId);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getCause());
        } finally {
            bundle.deleteBundle(prismHelper);
        }
    }

    @Test(dataProvider = "DP", groups = {"0.2.2", "retry"}, enabled = false)
    public void testRetryInProcessUpdate(Bundle bundle, String retryType, int delay,
                                         String delayUnits,
                                         int retryAttempts)
    throws Exception {
        bundle = new Bundle(bundle, UA3ColoHelper);
        displayInputs("testRetryInProcessUpdate", delay, retryType, retryAttempts);
        try {

            System.setProperty("java.security.krb5.realm", "");
            System.setProperty("java.security.krb5.kdc", "");

            String feed = Util.setFeedPathValue(Util.getInputFeedFromBundle(bundle),
                    "/lateDataTest/testFolders/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
            feed = Util.insertLateFeedValue(feed, "8", "minutes");
            bundle.getDataSets().remove(Util.getInputFeedFromBundle(bundle));
            bundle.getDataSets().add(feed);

            //inject data into folders for processing
//        Util.HDFSCleanup(UA3ColoHelper,"lateDataTest/testFolders/");
//        Util.lateDataReplenish(UA3ColoHelper,20,0,0);
//        List<String> initialData=Util.getHadoopLateData(UA3ColoHelper,
// Util.getInputFeedFromBundle(bundle));

            bundle.generateUniqueBundle();

            submitClusters(bundle);

            for (String data : bundle.getDataSets()) {
                Util.assertSucceeded(
                        prismHelper.getFeedHelper().submitEntity(URLS.SUBMIT_URL, data));
            }

            //set process start and end date
            DateTime startDate = new DateTime(DateTimeZone.UTC).plusMinutes(1);
            DateTime endDate = new DateTime(DateTimeZone.UTC).plusMinutes(2);

            bundle.setProcessValidity(startDate, endDate);

            bundle.setRetry(getRetry(bundle, delay, delayUnits, retryType, retryAttempts));

            //submit and schedule process
            ServiceResponse response =
                    prismHelper.getProcessHelper()
                            .submitEntity(URLS.SUBMIT_URL, bundle.getProcessData());

            if (retryAttempts <= 0 || delay <= 0) {
                Util.assertFailed(response);
            } else {

                Util.assertSucceeded(response);
                Util.HDFSCleanup(UA3ColoHelper, "lateDataTest/testFolders/");
                Util.lateDataReplenish(UA3ColoHelper, 20, 0);
                List<String> initialData =
                        Util.getHadoopLateData(UA3ColoHelper, Util.getInputFeedFromBundle(bundle));
                Util.assertSucceeded(
                        prismHelper.getProcessHelper()
                                .schedule(URLS.SCHEDULE_URL, bundle.getProcessData()));
                //now wait till the process is over
                String bundleId = Util.getBundles(UA3ColoHelper.getFeedHelper().getOozieClient(),
                        Util.readEntityName(bundle.getProcessData()), ENTITY_TYPE.PROCESS).get(0);
                String status = Util.getBundleStatus(UA3ColoHelper, bundleId);

                waitTillCertainPercentageOfProcessHasStarted(UA3ColoHelper, bundleId, 25);

                Process oldProcessObject = bundle.getProcessObject();

                Retry retry = new Retry();
                retry = bundle.getProcessObject().getRetry();
                retry.setAttempts((4));

                bundle.setRetry(retry);
                ////bundle.writeBundleToFiles();

                System.out.println("going to update process at:" + DateTime.now(DateTimeZone.UTC));
                Assert.assertTrue(prismHelper.getProcessHelper()
                        .update(Util.readEntityName(bundle.getProcessData()),
                                bundle.getProcessFilePath()).getMessage()
                        .contains("updated successfully"), "process was not updated successfully");
                String newBundleId = InstanceUtil
                        .getLatestBundleID(UA3ColoHelper,
                                Util.readEntityName(bundle.getProcessData()), ENTITY_TYPE.PROCESS);

                Assert.assertEquals(bundleId, newBundleId, "its creating a new bundle!!!");

                //now to validate all failed instances to check if they were retried or not.
                validateRetry(UA3ColoHelper, bundleId, 4);
                //validateRetry(newBundleId,bundle.getProcessObject().getRetry().getAttempts());
                if (bundle.getProcessObject().getRetry().getAttempts() > 0) {
                    checkIfRetriesWereTriggeredCorrectly(UA3ColoHelper, retryType, delay, bundleId);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getCause());
        } finally {
            bundle.deleteBundle(prismHelper);
        }
    }


    @Test(dataProvider = "DP", groups = {"0.2.2", "retry"}, enabled = false)
    public void testRetryInProcessHigherDelayUpdate(Bundle bundle, String retryType, int delay,
                                                    String delayUnits,
                                                    int retryAttempts)
    throws Exception {
        bundle = new Bundle(bundle, UA3ColoHelper);
        displayInputs("testRetryInProcessUpdate", delay, retryType, retryAttempts);
        try {

            System.setProperty("java.security.krb5.realm", "");
            System.setProperty("java.security.krb5.kdc", "");

            String feed = Util.setFeedPathValue(Util.getInputFeedFromBundle(bundle),
                    "/lateDataTest/testFolders/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
            feed = Util.insertLateFeedValue(feed, "8", "minutes");
            bundle.getDataSets().remove(Util.getInputFeedFromBundle(bundle));
            bundle.getDataSets().add(feed);

            //inject data into folders for processing
//        Util.HDFSCleanup(UA3ColoHelper,"lateDataTest/testFolders/");
//        Util.lateDataReplenish(UA3ColoHelper,20,0,0);
//        List<String> initialData=Util.getHadoopLateData(UA3ColoHelper,
// Util.getInputFeedFromBundle(bundle));

            bundle.generateUniqueBundle();

            submitClusters(bundle);

            for (String data : bundle.getDataSets()) {
                Util.assertSucceeded(
                        prismHelper.getFeedHelper().submitEntity(URLS.SUBMIT_URL, data));
            }

            //set process start and end date
            DateTime startDate = new DateTime(DateTimeZone.UTC).plusMinutes(1);
            DateTime endDate = new DateTime(DateTimeZone.UTC).plusMinutes(2);

            bundle.setProcessValidity(startDate, endDate);

            bundle.setRetry(getRetry(bundle, delay, delayUnits, retryType, retryAttempts));

            //submit and schedule process
            ServiceResponse response =
                    prismHelper.getProcessHelper()
                            .submitEntity(URLS.SUBMIT_URL, bundle.getProcessData());

            if (retryAttempts <= 0 || delay <= 0) {
                Util.assertFailed(response);
            } else {

                Util.assertSucceeded(response);
                Util.HDFSCleanup(UA3ColoHelper, "lateDataTest/testFolders/");
                Util.lateDataReplenish(UA3ColoHelper, 20, 0);
                List<String> initialData =
                        Util.getHadoopLateData(UA3ColoHelper, Util.getInputFeedFromBundle(bundle));
                Util.assertSucceeded(
                        prismHelper.getProcessHelper()
                                .schedule(URLS.SCHEDULE_URL, bundle.getProcessData()));
                //now wait till the process is over
                String bundleId = Util.getBundles(UA3ColoHelper.getFeedHelper().getOozieClient(),
                        Util.readEntityName(bundle.getProcessData()), ENTITY_TYPE.PROCESS).get(0);
                String status = Util.getBundleStatus(UA3ColoHelper, bundleId);

                waitTillCertainPercentageOfProcessHasStarted(UA3ColoHelper, bundleId, 25);

                Process oldProcessObject = bundle.getProcessObject();

                Retry retry = new Retry();
                retry = bundle.getProcessObject().getRetry();
                retry.setDelay(
                        new Frequency("minutes(" + (retry.getDelay().getFrequency() + 1) + ")"));

                bundle.setRetry(retry);
                //bundle.writeBundleToFiles();

                System.out.println("going to update process at:" + DateTime.now(DateTimeZone.UTC));
                Assert.assertTrue(prismHelper.getProcessHelper()
                        .update(Util.readEntityName(bundle.getProcessData()),
                                bundle.getProcessData()).getMessage()
                        .contains("updated successfully"), "process was not updated successfully");
                String newBundleId = InstanceUtil
                        .getLatestBundleID(UA3ColoHelper,
                                Util.readEntityName(bundle.getProcessData()), ENTITY_TYPE.PROCESS);

                Assert.assertEquals(bundleId, newBundleId, "its creating a new bundle!!!");

                //now to validate all failed instances to check if they were retried or not.
                validateRetry(UA3ColoHelper, bundleId,
                        bundle.getProcessObject().getRetry().getAttempts());
                //validateRetry(newBundleId,bundle.getProcessObject().getRetry().getAttempts());
                if (bundle.getProcessObject().getRetry().getAttempts() > 0) {
                    checkIfRetriesWereTriggeredCorrectly(UA3ColoHelper, retryType, delay, bundleId);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getCause());
        } finally {
            bundle.deleteBundle(prismHelper);
        }
    }


    @Test(dataProvider = "DP", groups = {"0.2.2", "retry"}, enabled = false)
    public void testRetryInProcessLowerDelayUpdate(Bundle bundle, String retryType, int delay,
                                                   String delayUnits,
                                                   int retryAttempts)
    throws Exception {
        bundle = new Bundle(bundle, UA3ColoHelper);
        displayInputs("testRetryInProcessUpdate", delay, retryType, retryAttempts);
        try {

            System.setProperty("java.security.krb5.realm", "");
            System.setProperty("java.security.krb5.kdc", "");

            String feed = Util.setFeedPathValue(Util.getInputFeedFromBundle(bundle),
                    "/lateDataTest/testFolders/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
            feed = Util.insertLateFeedValue(feed, "8", "minutes");
            bundle.getDataSets().remove(Util.getInputFeedFromBundle(bundle));
            bundle.getDataSets().add(feed);

            //inject data into folders for processing
//        Util.HDFSCleanup(UA3ColoHelper,"lateDataTest/testFolders/");
//        Util.lateDataReplenish(UA3ColoHelper,20,0,0);
//        List<String> initialData=Util.getHadoopLateData(UA3ColoHelper,
// Util.getInputFeedFromBundle(bundle));

            bundle.generateUniqueBundle();

            submitClusters(bundle);

            for (String data : bundle.getDataSets()) {
                Util.assertSucceeded(
                        prismHelper.getFeedHelper().submitEntity(URLS.SUBMIT_URL, data));
            }

            //set process start and end date
            DateTime startDate = new DateTime(DateTimeZone.UTC).plusMinutes(1);
            DateTime endDate = new DateTime(DateTimeZone.UTC).plusMinutes(2);

            bundle.setProcessValidity(startDate, endDate);

            bundle.setRetry(getRetry(bundle, delay, delayUnits, retryType, retryAttempts));

            //submit and schedule process
            ServiceResponse response =
                    prismHelper.getProcessHelper()
                            .submitEntity(URLS.SUBMIT_URL, bundle.getProcessData());

            if (retryAttempts <= 0 || delay <= 0) {
                Util.assertFailed(response);
            } else {

                Util.assertSucceeded(response);
                Util.HDFSCleanup(UA3ColoHelper, "lateDataTest/testFolders/");
                Util.lateDataReplenish(UA3ColoHelper, 20, 0);
                List<String> initialData =
                        Util.getHadoopLateData(UA3ColoHelper, Util.getInputFeedFromBundle(bundle));
                Util.assertSucceeded(
                        prismHelper.getProcessHelper()
                                .schedule(URLS.SCHEDULE_URL, bundle.getProcessData()));
                //now wait till the process is over
                String bundleId = Util.getBundles(UA3ColoHelper.getFeedHelper().getOozieClient(),
                        Util.readEntityName(bundle.getProcessData()), ENTITY_TYPE.PROCESS).get(0);
                String status = Util.getBundleStatus(UA3ColoHelper, bundleId);

                waitTillCertainPercentageOfProcessHasStarted(UA3ColoHelper, bundleId, 25);

                Process oldProcessObject = bundle.getProcessObject();

                Retry retry = new Retry();
                retry = bundle.getProcessObject().getRetry();
                retry.setDelay(
                        new Frequency("minutes(" + (retry.getDelay().getFrequency() - 1) + ")"));

                bundle.setRetry(retry);
                //bundle.writeBundleToFiles();

                System.out.println("going to update process at:" + DateTime.now(DateTimeZone.UTC));
                Assert.assertTrue(prismHelper.getProcessHelper()
                        .update(Util.readEntityName(bundle.getProcessData()),
                                bundle.getProcessData()).getMessage()
                        .contains("updated successfully"),
                        "process was not updated successfully");
                String newBundleId = InstanceUtil
                        .getLatestBundleID(UA3ColoHelper,
                                Util.readEntityName(bundle.getProcessData()), ENTITY_TYPE.PROCESS);

                Assert.assertEquals(bundleId, newBundleId, "its creating a new bundle!!!");

                //now to validate all failed instances to check if they were retried or not.
                validateRetry(UA3ColoHelper, bundleId,
                        bundle.getProcessObject().getRetry().getAttempts());
                //validateRetry(newBundleId,bundle.getProcessObject().getRetry().getAttempts());
                if (bundle.getProcessObject().getRetry().getAttempts() > 0) {
                    checkIfRetriesWereTriggeredCorrectly(UA3ColoHelper, retryType, delay, bundleId);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getCause());
        } finally {
            bundle.deleteBundle(prismHelper);
        }
    }


    @Test(dataProvider = "DP", groups = {"0.2.2", "retry"}, enabled = false)
    public void testRetryInProcessZeroDelayUpdate(Bundle bundle, String retryType, int delay,
                                                  String delayUnits,
                                                  int retryAttempts)
    throws Exception {
        bundle = new Bundle(bundle, UA3ColoHelper);
        displayInputs("testRetryInProcessUpdate", delay, retryType, retryAttempts);
        try {

            System.setProperty("java.security.krb5.realm", "");
            System.setProperty("java.security.krb5.kdc", "");

            String feed = Util.setFeedPathValue(Util.getInputFeedFromBundle(bundle),
                    "/lateDataTest/testFolders/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
            feed = Util.insertLateFeedValue(feed, "8", "minutes");
            bundle.getDataSets().remove(Util.getInputFeedFromBundle(bundle));
            bundle.getDataSets().add(feed);

            //inject data into folders for processing
//        Util.HDFSCleanup(UA3ColoHelper,"lateDataTest/testFolders/");
//        Util.lateDataReplenish(UA3ColoHelper,20,0,0);
//        List<String> initialData=Util.getHadoopLateData(UA3ColoHelper,
// Util.getInputFeedFromBundle(bundle));

            bundle.generateUniqueBundle();

            submitClusters(bundle);

            for (String data : bundle.getDataSets()) {
                Util.assertSucceeded(
                        prismHelper.getFeedHelper().submitEntity(URLS.SUBMIT_URL, data));
            }

            //set process start and end date
            DateTime startDate = new DateTime(DateTimeZone.UTC).plusMinutes(1);
            DateTime endDate = new DateTime(DateTimeZone.UTC).plusMinutes(2);

            bundle.setProcessValidity(startDate, endDate);

            bundle.setRetry(getRetry(bundle, delay, delayUnits, retryType, retryAttempts));

            //submit and schedule process
            ServiceResponse response =
                    prismHelper.getProcessHelper()
                            .submitEntity(URLS.SUBMIT_URL, bundle.getProcessData());

            if (retryAttempts <= 0 || delay <= 0) {
                Util.assertFailed(response);
            } else {

                Util.assertSucceeded(response);
                Util.HDFSCleanup(UA3ColoHelper, "lateDataTest/testFolders/");
                Util.lateDataReplenish(UA3ColoHelper, 20, 0);
                List<String> initialData =
                        Util.getHadoopLateData(UA3ColoHelper, Util.getInputFeedFromBundle(bundle));
                Util.assertSucceeded(
                        prismHelper.getProcessHelper()
                                .schedule(URLS.SCHEDULE_URL, bundle.getProcessData()));
                //now wait till the process is over
                String bundleId = Util.getBundles(UA3ColoHelper.getFeedHelper().getOozieClient(),
                        Util.readEntityName(bundle.getProcessData()), ENTITY_TYPE.PROCESS).get(0);
                String status = Util.getBundleStatus(UA3ColoHelper, bundleId);

                waitTillCertainPercentageOfProcessHasStarted(UA3ColoHelper, bundleId, 25);

                Process oldProcessObject = bundle.getProcessObject();

                Retry retry = new Retry();
                retry = bundle.getProcessObject().getRetry();
                retry.setDelay(new Frequency("minutes(0)"));

                bundle.setRetry(retry);
                //bundle.writeBundleToFiles();

                System.out.println("going to update process at:" + DateTime.now(DateTimeZone.UTC));
                Assert.assertFalse(prismHelper.getProcessHelper()
                        .update(Util.readEntityName(bundle.getProcessData())
                                , bundle.getProcessData()).getMessage()
                        .contains("updated successfully"),
                        "process was updated successfully!!!");
                String newBundleId = InstanceUtil
                        .getLatestBundleID(UA3ColoHelper,
                                Util.readEntityName(bundle.getProcessData()), ENTITY_TYPE.PROCESS);

                Assert.assertEquals(bundleId, newBundleId, "its creating a new bundle!!!");

                //now to validate all failed instances to check if they were retried or not.
                validateRetry(UA3ColoHelper, bundleId,
                        bundle.getProcessObject().getRetry().getAttempts());
                //validateRetry(newBundleId,bundle.getProcessObject().getRetry().getAttempts());
                if (bundle.getProcessObject().getRetry().getAttempts() > 0) {
                    checkIfRetriesWereTriggeredCorrectly(UA3ColoHelper, retryType, delay, bundleId);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getCause());
        } finally {
            bundle.deleteBundle(prismHelper);
        }
    }


    @Test(dataProvider = "DP", groups = {"0.2.2", "retry"}, enabled = false)
    public void testRetryInSimpleFailureCase(Bundle bundle, String retryType, int delay,
                                             String delayUnits,
                                             int retryAttempts)
    throws Exception {
        bundle = new Bundle(bundle, UA3ColoHelper);
        displayInputs("testRetryInSimpleFailureCase", delay, retryType, retryAttempts);

        try {

            System.setProperty("java.security.krb5.realm", "");
            System.setProperty("java.security.krb5.kdc", "");

            String feed = Util.setFeedPathValue(Util.getInputFeedFromBundle(bundle),
                    "/lateDataTest/testFolders/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
            feed = Util.insertLateFeedValue(feed, "8", "minutes");
            bundle.getDataSets().remove(Util.getInputFeedFromBundle(bundle));
            bundle.getDataSets().add(feed);

            //inject data into folders for processing
//        Util.HDFSCleanup(UA3ColoHelper,"lateDataTest/testFolders/");
//        Util.lateDataReplenish(UA3ColoHelper,20,0,0);
//        List<String> initialData=Util.getHadoopLateData(UA3ColoHelper,
// Util.getInputFeedFromBundle(bundle));

            bundle.generateUniqueBundle();


            bundle.setRetry(getRetry(bundle, delay, delayUnits, retryType, retryAttempts));

            submitClusters(bundle);

            for (String data : bundle.getDataSets()) {
                Util.assertSucceeded(
                        prismHelper.getFeedHelper().submitEntity(URLS.SUBMIT_URL, data));
            }

            //set process start and end date
            DateTime startDate = new DateTime(DateTimeZone.UTC).plusMinutes(1);
            DateTime endDate = new DateTime(DateTimeZone.UTC).plusMinutes(2);

            bundle.setProcessValidity(startDate, endDate);

            bundle.setProcessLatePolicy(null);
            //submit and schedule process
            ServiceResponse response =
                    prismHelper.getProcessHelper()
                            .submitEntity(URLS.SUBMIT_URL, bundle.getProcessData());

            if (retryAttempts <= 0 || delay <= 0) {
                Util.assertFailed(response);
            } else {
                Util.assertSucceeded(response);
                Util.HDFSCleanup(UA3ColoHelper, "lateDataTest/testFolders/");
                Util.lateDataReplenish(UA3ColoHelper, 20, 0);
                List<String> initialData =
                        Util.getHadoopLateData(UA3ColoHelper, Util.getInputFeedFromBundle(bundle));
                Util.assertSucceeded(
                        prismHelper.getProcessHelper()
                                .schedule(URLS.SCHEDULE_URL, bundle.getProcessData()));
                //now wait till the process is over
                String bundleId = Util.getBundles(UA3ColoHelper.getFeedHelper().getOozieClient(),
                        Util.readEntityName(bundle.getProcessData()), ENTITY_TYPE.PROCESS).get(0);
                String status = Util.getBundleStatus(UA3ColoHelper, bundleId);

                //now to validate all failed instances to check if they were retried or not.
                validateRetry(UA3ColoHelper, bundleId,
                        bundle.getProcessObject().getRetry().getAttempts());
                if (bundle.getProcessObject().getRetry().getAttempts() > 0) {
                    checkIfRetriesWereTriggeredCorrectly(UA3ColoHelper, retryType, delay, bundleId);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getCause());
        } finally {
            bundle.deleteBundle(prismHelper);
        }
    }


    @Test(dataProvider = "DP", groups = {"0.2.2", "retry"}, enabled = false)
    public void testUserRetryWhileAutomaticRetriesHappen(Bundle bundle, String retryType, int delay,
                                                         String delayUnits,
                                                         int retryAttempts)
    throws Exception {
        bundle = new Bundle(bundle, UA3ColoHelper);
        displayInputs("testUserRetryWhileAutomaticRetriesHappen", delay, retryType, retryAttempts);

        try {

            System.setProperty("java.security.krb5.realm", "");
            System.setProperty("java.security.krb5.kdc", "");

            DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd/hh:mm");

            String feed = Util.setFeedPathValue(Util.getInputFeedFromBundle(bundle),
                    "/lateDataTest/testFolders/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
            feed = Util.insertLateFeedValue(feed, "8", "minutes");
            bundle.getDataSets().remove(Util.getInputFeedFromBundle(bundle));
            bundle.getDataSets().add(feed);

            //inject data into folders for processing
//        Util.HDFSCleanup(UA3ColoHelper,"lateDataTest/testFolders/");
//        Util.lateDataReplenish(UA3ColoHelper,20,0,0);
//        List<String> initialData=Util.getHadoopLateData(UA3ColoHelper,
// Util.getInputFeedFromBundle(bundle));

            bundle.generateUniqueBundle();

            bundle.setRetry(getRetry(bundle, delay, delayUnits, retryType, retryAttempts));

            submitClusters(bundle);

            for (String data : bundle.getDataSets()) {
                Util.assertSucceeded(
                        prismHelper.getFeedHelper().submitEntity(URLS.SUBMIT_URL, data));
            }

            //set process start and end date
            DateTime startDate = new DateTime(DateTimeZone.UTC).plusMinutes(1);
            DateTime endDate = new DateTime(DateTimeZone.UTC).plusMinutes(2);

            System.out.println("process dates: " + startDate + "," + endDate);
            bundle.setProcessValidity(startDate, endDate);

            //submit and schedule process
            ServiceResponse response =
                    prismHelper.getProcessHelper()
                            .submitEntity(URLS.SUBMIT_URL, bundle.getProcessData());

            if (retryAttempts <= 0 || delay <= 0) {
                Util.assertFailed(response);
            } else {
                Util.assertSucceeded(response);
                Util.HDFSCleanup(UA3ColoHelper, "lateDataTest/testFolders/");
                Util.lateDataReplenish(UA3ColoHelper, 20, 0);
                List<String> initialData =
                        Util.getHadoopLateData(UA3ColoHelper, Util.getInputFeedFromBundle(bundle));
                Util.assertSucceeded(
                        prismHelper.getProcessHelper()
                                .schedule(URLS.SCHEDULE_URL, bundle.getProcessData()));
                //now wait till the process is over
                String bundleId = Util.getBundles(UA3ColoHelper.getFeedHelper().getOozieClient(),
                        Util.readEntityName(bundle.getProcessData()), ENTITY_TYPE.PROCESS).get(0);
                String status = Util.getBundleStatus(UA3ColoHelper, bundleId);

                while (!validateFailureRetries(UA3ColoHelper,
                        getDefaultOozieCoord(UA3ColoHelper, bundleId), 1)) {
                    //keep waiting
                }

                //validateRetry(bundleId,1);

                //Thread.sleep(420000);

                //now start firing random retries

                System.out.println("now firing user reruns:");
                for (int i = 0; i < 1; i++) {
                    prismHelper.getProcessHelper()
                            .getProcessInstanceRerun(Util.readEntityName(bundle.getProcessData()),
                                    "?start=" + formatter.print(startDate).replace("/", "T") + "Z" +
                                            "&end=" +
                                            formatter.print(endDate).replace("/", "T") + "Z");
                    //Thread.sleep(20000);
                }
                //now to validate all failed instances to check if they were retried or not.
                validateRetry(UA3ColoHelper, bundleId,
                        bundle.getProcessObject().getRetry().getAttempts());
                if (bundle.getProcessObject().getRetry().getAttempts() > 0) {
                    checkIfRetriesWereTriggeredCorrectly(UA3ColoHelper, retryType, delay, bundleId);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getCause());
        } finally {
            bundle.deleteBundle(prismHelper);
        }
    }

    @Test(dataProvider = "DP", groups = {"0.2.2", "retry"}, enabled = false)
    public void testUserRetryAfterAutomaticRetriesHappen(Bundle bundle, String retryType, int delay,
                                                         String delayUnits,
                                                         int retryAttempts)
    throws Exception {
        bundle = new Bundle(bundle, UA3ColoHelper);
        displayInputs("testUserRetryAfterAutomaticRetriesHappen", delay, retryType, retryAttempts);
        try {

            System.setProperty("java.security.krb5.realm", "");
            System.setProperty("java.security.krb5.kdc", "");

            DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd/hh:mm");

            String feed = Util.setFeedPathValue(Util.getInputFeedFromBundle(bundle),
                    "/lateDataTest/testFolders/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
            feed = Util.insertLateFeedValue(feed, "8", "minutes");
            bundle.getDataSets().remove(Util.getInputFeedFromBundle(bundle));
            bundle.getDataSets().add(feed);

            //inject data into folders for processing
//        Util.HDFSCleanup(UA3ColoHelper,"lateDataTest/testFolders/");
//        Util.lateDataReplenish(UA3ColoHelper,20,0,0);
//        List<String> initialData=Util.getHadoopLateData(UA3ColoHelper,
// Util.getInputFeedFromBundle(bundle));

            bundle.generateUniqueBundle();
            bundle.setRetry(getRetry(bundle, delay, delayUnits, retryType, retryAttempts));

            submitClusters(bundle);

            for (String data : bundle.getDataSets()) {
                Util.assertSucceeded(
                        prismHelper.getFeedHelper().submitEntity(URLS.SUBMIT_URL, data));
            }

            //set process start and end date
            DateTime startDate = new DateTime(DateTimeZone.UTC).plusMinutes(1);
            DateTime endDate = new DateTime(DateTimeZone.UTC).plusMinutes(2);

            System.out.println("process dates: " + startDate + "," + endDate);
            bundle.setProcessValidity(startDate, endDate);

            //submit and schedule process
            ServiceResponse response =
                    prismHelper.getProcessHelper()
                            .submitEntity(URLS.SUBMIT_URL, bundle.getProcessData());

            if (retryAttempts <= 0 || delay <= 0) {
                Util.assertFailed(response);
            } else {
                Util.assertSucceeded(response);
                Util.HDFSCleanup(UA3ColoHelper, "lateDataTest/testFolders/");
                Util.lateDataReplenish(UA3ColoHelper, 20, 0);
                List<String> initialData =
                        Util.getHadoopLateData(UA3ColoHelper, Util.getInputFeedFromBundle(bundle));
                Util.assertSucceeded(
                        prismHelper.getProcessHelper()
                                .schedule(URLS.SCHEDULE_URL, bundle.getProcessData()));
                //now wait till the process is over
                String bundleId = Util.getBundles(UA3ColoHelper.getFeedHelper().getOozieClient(),
                        Util.readEntityName(bundle.getProcessData()), ENTITY_TYPE.PROCESS).get(0);
                String status = Util.getBundleStatus(UA3ColoHelper, bundleId);


                //now to validate all failed instances to check if they were retried or not.
                validateRetry(UA3ColoHelper, bundleId,
                        bundle.getProcessObject().getRetry().getAttempts());

                System.out.println("now firing user reruns:");

                DateTime[] dateBoundaries = getFailureTimeBoundaries(UA3ColoHelper, bundleId);
                ProcessInstancesResult piResult = prismHelper.getProcessHelper()
                        .getProcessInstanceRerun(Util.readEntityName(bundle.getProcessData()),
                                "?start=" + formatter.print(dateBoundaries[0]).replace("/", "T") +
                                        "Z" + "&end=" +
                                        formatter.print(dateBoundaries[dateBoundaries.length - 1])
                                                .replace("/", "T") +
                                        "Z");

                Assert.assertEquals(piResult.getStatusCode(), 0,
                        "rerun failed miserably! you fool!");

                System.out.println();

                validateRetry(UA3ColoHelper, bundleId,
                        bundle.getProcessObject().getRetry().getAttempts() + 1);

                if (bundle.getProcessObject().getRetry().getAttempts() > 0) {
                    checkIfRetriesWereTriggeredCorrectly(UA3ColoHelper, retryType, delay, bundleId);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getCause());
        } finally {
            bundle.deleteBundle(prismHelper);
        }
    }

    @Test(dataProvider = "DP", groups = {"0.2.2", "retry"}, enabled = false)
    public void testRetryInSuspendedAndResumeCaseWithLateData(Bundle bundle, String retryType,
                                                              int delay,
                                                              String delayUnits, int retryAttempts)
    throws Exception {
        bundle = new Bundle(bundle, UA3ColoHelper);
        displayInputs("testRetryInSuspendedAndResumeCaseWithLateData", delay, retryType,
                retryAttempts);
        try {

            System.setProperty("java.security.krb5.realm", "");
            System.setProperty("java.security.krb5.kdc", "");

            String feed = Util.setFeedPathValue(Util.getInputFeedFromBundle(bundle),
                    "/lateDataTest/testFolders/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
            feed = Util.insertLateFeedValue(feed, "10", "minutes");
            bundle.getDataSets().remove(Util.getInputFeedFromBundle(bundle));
            bundle.getDataSets().add(feed);

            //inject data into folders for processing
//        Util.HDFSCleanup(UA3ColoHelper,"lateDataTest/testFolders/");
//        Util.lateDataReplenish(UA3ColoHelper,20,0,0);
//        List<String> initialData=Util.getHadoopLateData(UA3ColoHelper,
// Util.getInputFeedFromBundle(bundle));

            bundle.setRetry(getRetry(bundle, delay, delayUnits, retryType, retryAttempts));
            bundle.generateUniqueBundle();

            submitClusters(bundle);


            for (String data : bundle.getDataSets()) {
                Util.assertSucceeded(
                        prismHelper.getFeedHelper().submitEntity(URLS.SUBMIT_URL, data));
            }

            //set process start and end date
            DateTime startDate = new DateTime(DateTimeZone.UTC).plusMinutes(1);
            DateTime endDate = new DateTime(DateTimeZone.UTC).plusMinutes(2);

            bundle.setProcessValidity(startDate, endDate);

            //submit and schedule process
            ServiceResponse response =
                    prismHelper.getProcessHelper()
                            .submitEntity(URLS.SUBMIT_URL, bundle.getProcessData());

            if (retryAttempts <= 0 || delay <= 0) {
                Util.assertFailed(response);
            } else {

                Util.assertSucceeded(response);
                Util.HDFSCleanup(UA3ColoHelper, "lateDataTest/testFolders/");
                Util.lateDataReplenish(UA3ColoHelper, 20, 0);
                List<String> initialData =
                        Util.getHadoopLateData(UA3ColoHelper, Util.getInputFeedFromBundle(bundle));
                Util.assertSucceeded(
                        prismHelper.getProcessHelper()
                                .schedule(URLS.SCHEDULE_URL, bundle.getProcessData()));
                String bundleId = Util.getBundles(UA3ColoHelper.getFeedHelper().getOozieClient(),
                        Util.readEntityName(bundle.getProcessData()), ENTITY_TYPE.PROCESS).get(0);
                ArrayList<DateTime> dates = null;

                do {

                    dates = Util.getStartTimeForRunningCoordinators(UA3ColoHelper, bundleId);

                } while (dates == null);


                System.out.println("Start time: " + formatter.print(startDate));
                System.out.println("End time: " + formatter.print(endDate));
                System.out.println("candidate nominal time:" + formatter.print(dates.get(0)));
                DateTime now = dates.get(0);

                if (formatter.print(startDate).compareToIgnoreCase(formatter.print(dates.get(0))) >
                        0) {
                    now = startDate;
                }


                //now wait till the process is over

                String status = Util.getBundleStatus(UA3ColoHelper, bundleId);

                while (!validateFailureRetries(UA3ColoHelper,
                        getDefaultOozieCoord(UA3ColoHelper, bundleId), 1)) {
                    //keep waiting
                }

                System.out.println("now suspending the process altogether....");


                Util.assertSucceeded(
                        UA3ColoHelper.getProcessHelper()
                                .suspend(URLS.SUSPEND_URL, bundle.getProcessData()));


                HashMap<String, Integer> initialMap =
                        getFailureRetriesForEachWorkflow(UA3ColoHelper,
                                getDefaultOozieCoord(UA3ColoHelper, bundleId));
                System.out.println("saved state of workflow retries");

                for (String key : initialMap.keySet()) {
                    System.out.println(key + "," + initialMap.get(key));
                }

                Thread.sleep(600000);


                HashMap<String, Integer> finalMap =
                        getFailureRetriesForEachWorkflow(UA3ColoHelper,
                                getDefaultOozieCoord(UA3ColoHelper, bundleId));

                System.out.println("final state of process looks like:");

                for (String key : finalMap.keySet()) {
                    System.out.println(key + "," + finalMap.get(key));
                }

                Assert.assertEquals(initialMap.size(), finalMap.size(),
                        "a new workflow retried while process was suspended!!!!");

                for (String key : initialMap.keySet()) {
                    Assert.assertEquals(initialMap.get(key), finalMap.get(key),
                            "values are different for workflow: " + key);
                }

                System.out.println("now resuming the process...");
                Util.assertSucceeded(UA3ColoHelper.getProcessHelper()
                        .resume(URLS.RESUME_URL, bundle.getProcessData()));

                //now to validate all failed instances to check if they were retried or not.
                validateRetry(UA3ColoHelper, bundleId,
                        bundle.getProcessObject().getRetry().getAttempts());

                if (bundle.getProcessObject().getRetry().getAttempts() > 0) {
                    checkIfRetriesWereTriggeredCorrectly(UA3ColoHelper, retryType, delay, bundleId);
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getCause());
        } finally {
            bundle.deleteBundle(prismHelper);
        }
    }


    @Test(dataProvider = "DP", groups = {"0.2.2", "retry"}, enabled = false)
    public void testRetryInLateDataCase(Bundle bundle, String retryType, int delay,
                                        String delayUnits,
                                        int retryAttempts)
    throws Exception {
        bundle = new Bundle(bundle, UA3ColoHelper);
        displayInputs("testRetryInLateDataCase", delay, retryType, retryAttempts);
        try {

            System.setProperty("java.security.krb5.realm", "");
            System.setProperty("java.security.krb5.kdc", "");

            String feed = Util.setFeedPathValue(Util.getInputFeedFromBundle(bundle),
                    "/lateDataTest/testFolders/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");


            feed = Util.insertLateFeedValue(feed,
                    getDelay(delay, retryAttempts, retryType).toString(), delayUnits);

            bundle.getDataSets().remove(Util.getInputFeedFromBundle(bundle));
            bundle.getDataSets().add(feed);

            //inject data into folders for processing
//        Util.HDFSCleanup(UA3ColoHelper,"lateDataTest/testFolders/");
//        Util.lateDataReplenish(UA3ColoHelper,20,0,0);
//        List<String> initialData=Util.getHadoopLateData(UA3ColoHelper,
// Util.getInputFeedFromBundle(bundle));

            bundle.setRetry(getRetry(bundle, delay, delayUnits, retryType, retryAttempts));
            bundle.generateUniqueBundle();

            submitClusters(bundle);


            for (String data : bundle.getDataSets()) {
                Util.assertSucceeded(
                        prismHelper.getFeedHelper().submitEntity(URLS.SUBMIT_URL, data));
            }

            //set process start and end date
            DateTime startDate = new DateTime(DateTimeZone.UTC).plusMinutes(1);
            DateTime endDate = new DateTime(DateTimeZone.UTC).plusMinutes(2);

            bundle.setProcessValidity(startDate, endDate);

            //submit and schedule process
            ServiceResponse response =
                    prismHelper.getProcessHelper()
                            .submitEntity(URLS.SUBMIT_URL, bundle.getProcessData());

            if (retryAttempts <= 0 || delay <= 0) {
                Util.assertFailed(response);
            } else {
                Util.assertSucceeded(response);
                Util.HDFSCleanup(UA3ColoHelper, "lateDataTest/testFolders/");
                Util.lateDataReplenish(UA3ColoHelper, 20, 0);
                List<String> initialData =
                        Util.getHadoopLateData(UA3ColoHelper, Util.getInputFeedFromBundle(bundle));
                Util.assertSucceeded(
                        prismHelper.getProcessHelper()
                                .schedule(URLS.SCHEDULE_URL, bundle.getProcessData()));
                String bundleId = Util.getBundles(UA3ColoHelper.getFeedHelper().getOozieClient(),
                        Util.readEntityName(bundle.getProcessData()), ENTITY_TYPE.PROCESS).get(0);
                ArrayList<DateTime> dates = null;

                do {

                    dates = Util.getStartTimeForRunningCoordinators(UA3ColoHelper, bundleId);

                } while (dates == null);


                System.out.println("Start time: " + formatter.print(startDate));
                System.out.println("End time: " + formatter.print(endDate));
                System.out.println("candidate nominal time:" + formatter.print(dates.get(0)));
                DateTime now = dates.get(0);

                if (formatter.print(startDate).compareToIgnoreCase(formatter.print(dates.get(0))) >
                        0) {
                    now = startDate;
                }


                //now wait till the process is over

                String status = Util.getBundleStatus(UA3ColoHelper, bundleId);

                boolean inserted = false;

                int tryingToInsertData = 0;

                while (true) {

                    //keep dancing
                    String insertionFolder =
                            Util.findFolderBetweenGivenTimeStamps(now, now.plusMinutes(5),
                                    initialData);

                    if (!inserted &&
                            validateFailureRetries(UA3ColoHelper,
                                    getDefaultOozieCoord(UA3ColoHelper, bundleId),
                                    bundle.getProcessObject().getRetry().getAttempts())) {
                        System.out.println("inserting data in folder " + insertionFolder + " at " +
                                DateTime.now());
                        Util.injectMoreData(UA3ColoHelper, insertionFolder,
                                "src/test/resources/OozieExampleInputData/lateData");
                        inserted = true;
                        break;
                    }

                    Thread.sleep(1000);
                    tryingToInsertData++;
                    status = Util.getBundleStatus(UA3ColoHelper, bundleId);
                }

                //now to validate all failed instances to check if they were retried or not.
                validateRetry(UA3ColoHelper, bundleId,
                        bundle.getProcessObject().getRetry().getAttempts());

                if (bundle.getProcessObject().getRetry().getAttempts() > 0) {
                    checkIfRetriesWereTriggeredCorrectly(UA3ColoHelper, retryType, delay, bundleId);
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getCause());
        } finally {
            bundle.deleteBundle(prismHelper);
        }
    }


    @Test(dataProvider = "DP", groups = {"0.2.2", "retry"}, enabled = false)
    public void testRetryInDeleteAfterPartialRetryCase(Bundle bundle, String retryType, int delay,
                                                       String delayUnits,
                                                       int retryAttempts)
    throws Exception {
        bundle = new Bundle(bundle, UA3ColoHelper);
        displayInputs("testRetryInDeleteAfterPartialRetryCase", delay, retryType, retryAttempts);

        try {

            System.setProperty("java.security.krb5.realm", "");
            System.setProperty("java.security.krb5.kdc", "");

            String feed = Util.setFeedPathValue(Util.getInputFeedFromBundle(bundle),
                    "/lateDataTest/testFolders/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
            feed = Util.insertLateFeedValue(feed, "1", "minutes");
            bundle.getDataSets().remove(Util.getInputFeedFromBundle(bundle));
            bundle.getDataSets().add(feed);

            //inject data into folders for processing
//        Util.HDFSCleanup(UA3ColoHelper,"lateDataTest/testFolders/");
//        Util.lateDataReplenish(UA3ColoHelper,20,0,0);
//        List<String> initialData=Util.getHadoopLateData(UA3ColoHelper,
// Util.getInputFeedFromBundle(bundle));

            bundle.generateUniqueBundle();


            bundle.setRetry(getRetry(bundle, delay, delayUnits, retryType, retryAttempts));

            submitClusters(bundle);

            for (String data : bundle.getDataSets()) {
                Util.assertSucceeded(
                        prismHelper.getFeedHelper().submitEntity(URLS.SUBMIT_URL, data));
            }

            //set process start and end date
            DateTime startDate = new DateTime(DateTimeZone.UTC).plusMinutes(1);
            DateTime endDate = new DateTime(DateTimeZone.UTC).plusMinutes(2);

            bundle.setProcessValidity(startDate, endDate);

            //submit and schedule process
            ServiceResponse response =
                    prismHelper.getProcessHelper()
                            .submitEntity(URLS.SUBMIT_URL, bundle.getProcessData());

            if (retryAttempts <= 0 || delay <= 0) {
                Util.assertFailed(response);
            } else {
                Util.assertSucceeded(response);
                Util.HDFSCleanup(UA3ColoHelper, "lateDataTest/testFolders/");
                Util.lateDataReplenish(UA3ColoHelper, 20, 0);
                List<String> initialData =
                        Util.getHadoopLateData(UA3ColoHelper, Util.getInputFeedFromBundle(bundle));
                Util.assertSucceeded(
                        prismHelper.getProcessHelper()
                                .schedule(URLS.SCHEDULE_URL, bundle.getProcessData()));
                //now wait till the process is over
                String bundleId = Util.getBundles(UA3ColoHelper.getFeedHelper().getOozieClient(),
                        Util.readEntityName(bundle.getProcessData()), ENTITY_TYPE.PROCESS).get(0);
                String status = Util.getBundleStatus(UA3ColoHelper, bundleId);

                validateRetry(UA3ColoHelper, bundleId,
                        (bundle.getProcessObject().getRetry().getAttempts()) / 2);

                Util.assertSucceeded(prismHelper.getProcessHelper()
                        .delete(URLS.DELETE_URL, (bundle.getProcessData())));

                if (retryType.equals("exp-backoff")) {
                    Thread.sleep(1000 * 60 * delay *
                            ((retryAttempts -
                                    (bundle.getProcessObject().getRetry().getAttempts()) / 2) ^ 2));
                } else {
                    Thread.sleep(1000 * 60 * delay *
                            ((bundle.getProcessObject().getRetry().getAttempts()) -
                                    (bundle.getProcessObject().getRetry().getAttempts()) / 2));
                }


                //now to validate all failed instances to check if they were retried or not.
                validateRetry(UA3ColoHelper, bundleId,
                        (bundle.getProcessObject().getRetry().getAttempts()) / 2);

                if (bundle.getProcessObject().getRetry().getAttempts() > 0) {
                    checkIfRetriesWereTriggeredCorrectly(UA3ColoHelper, retryType, delay, bundleId);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getCause());
        } finally {
            bundle.deleteBundle(prismHelper);
        }
    }


    private void validateRetry(ColoHelper coloHelper, String bundleId, int maxNumberOfRetries)
    throws Exception {
        //validate that all failed processes were retried the specified number of times.
        int attempt = 0;
        boolean result = false;
        while (true) {
            result = ensureAllFailedInstancesHaveRetried(coloHelper, bundleId, maxNumberOfRetries);

            if (result || attempt > 3600) {
                break;
            } else {
                Thread.sleep(1000);
                System.out.println("desired state not reached.This was attempt number: " + attempt);
                attempt++;
                Thread.sleep(5000);
            }
        }
        Assert.assertTrue(result, "all retries were not attempted correctly!");

        //now validate correctness of retry timings

    }


    private boolean validateFailureRetries(ColoHelper coloHelper, CoordinatorJob coordinator,
                                           int maxNumberOfRetries)
    throws Exception {

        if (maxNumberOfRetries < 0) {
            maxNumberOfRetries = 0;
        }

        HashMap<String, Boolean> workflowMap = new HashMap<String, Boolean>();

        XOozieClient client = new XOozieClient(coloHelper.getFeedHelper().getOozieURL());

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

        //now to check each of these:

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

    private CoordinatorJob getLateOozieCoord(String bundleId) throws Exception {
        XOozieClient client = new XOozieClient(Util.readPropertiesFile("oozie_url"));
        BundleJob bundlejob = client.getBundleJobInfo(bundleId);

        for (CoordinatorJob coord : bundlejob.getCoordinators()) {
            if (coord.getAppName().contains("LATE")) {
                return client.getCoordJobInfo(coord.getId());
            }
        }
        return null;
    }

    @DataProvider(name = "DP")
    public Object[][] getData() throws Exception {

        String[] retryTypes = new String[]{"periodic", "exp-backoff"};//,"exp-backoff"
        Integer[] delays = new Integer[]{2,
                0};//0,-1,3 //removing -1 since this should be checked at validation level while
                // setting
        String[] delayUnits = new String[]{"minutes"};
        Integer[] retryAttempts = new Integer[]{2, 0, 3};//0,-1,2

        //Object [][] bundleData=Util.readBundles("RetryTests");
        Bundle[] bundleData = Util.getBundleData("RetryTests");

        Object[][] testData = new Object[retryTypes.length * delays.length * delayUnits.length *
                bundleData.length *
                retryAttempts.length][5];

        int i = 0;

        for (String retryType : retryTypes) {
            for (int delay : delays) {
                for (String delayUnit : delayUnits) {
                    for (int retry : retryAttempts)
                    //for(Object bundle:bundleData)
                    {
                        Bundle bundle = new Bundle(bundleData[0]);
                        testData[i][0] = bundle;
                        testData[i][1] = retryType;
                        testData[i][2] = delay;
                        testData[i][3] = delayUnit;
                        testData[i][4] = retry;
                        i++;
                    }
                }
            }
        }

        return testData;
    }


    private boolean allRelevantWorkflowsAreOver(ColoHelper coloHelper, String bundleId,
                                                String insertionFolder)
    throws Exception {
        boolean finished = true;

        XOozieClient oozieClient = new XOozieClient(coloHelper.getProcessHelper().getOozieURL());
        BundleJob bundleJob = oozieClient.getBundleJobInfo(bundleId);

        DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy/MM/dd/HH/mm");
        List<String> actualNominalTimes = new ArrayList<String>();

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
                                                        int maxNumberOfRetries)
    throws Exception {
        boolean retried = false;

        CoordinatorJob defaultCoordinator = getDefaultOozieCoord(coloHelper, bundleId);
        //CoordinatorJob lateCoordinator=getLateOozieCoord(bundleId);

        boolean retriedAllDefault =
                validateFailureRetries(coloHelper, defaultCoordinator, maxNumberOfRetries);
        //boolean retriedAllLate=validateFailureRetries(lateCoordinator, maxNumberOfRetries);

        //if(retriedAllDefault && retriedAllLate)
        if (retriedAllDefault) {
            return true;
        }
        return retried;
    }


    private void waitTillCertainPercentageOfProcessHasStarted(ColoHelper coloHelper,
                                                              String bundleId, int percentage)
    throws Exception {

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
                                                             int percentage)
    throws Exception {

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


    private CoordinatorAction getOozieActionInfo(ColoHelper colohelper, String actionId)
    throws Exception {
        XOozieClient client = new XOozieClient(colohelper.getProcessHelper().getOozieURL());
        return client.getCoordActionInfo(actionId);
    }


    private HashMap<String, Integer> getFailureRetriesForEachWorkflow(ColoHelper coloHelper,
                                                                      CoordinatorJob coordinator)
    throws Exception {
        boolean retriedAll = true;
        boolean wentInside = false;
        XOozieClient client = new XOozieClient(coloHelper.getClusterHelper().getOozieURL());

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

    private DateTime[] getFailureTimeBoundaries(ColoHelper coloHelper, String bundleId)
    throws Exception {
        XOozieClient client = new XOozieClient(coloHelper.getProcessHelper().getOozieURL());
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
                                                      int delay,
                                                      String bundleId)
    throws Exception {

        //it is presumed that this delay here will be expressed in minutes. Hourly/daily is unfeasible to check :)

        checkRetryTriggerForCoordinator(coloHelper, retryType, delay,
                getDefaultOozieCoord(coloHelper, bundleId));
        //checkRetryTriggerForCoordinator(retryType, delay,getLateOozieCoord(bundleId));

    }

    private void checkRetryTriggerForCoordinator(ColoHelper coloHelper, String retryType, int delay,
                                                 CoordinatorJob coordinator)
    throws Exception {


        DateTimeFormatter formatter = DateTimeFormat.forPattern("HH:mm:ss");

        for (CoordinatorAction action : coordinator.getActions()) {

            CoordinatorAction coordAction = getOozieActionInfo(coloHelper, action.getExternalId());
            if (!coordAction.getStatus().equals(CoordinatorAction.Status.SUCCEEDED)) {
                int expectedDelay = delay;
                //first get data from logs:
                ArrayList<String> instanceRetryTimes =
                        Util.getInstanceRetryTimes(coloHelper, action.getExternalId());
                ArrayList<String> instanceFinishTimes =
                        Util.getInstanceFinishTimes(coloHelper, action.getExternalId());

                System.out.println("finish times look like:");
                for (String line : instanceFinishTimes) {
                    System.out.println(line);
                }

                System.out.println("retry times look like:");
                for (String line : instanceRetryTimes) {
                    System.out.println(line);
                }

                System.out.println(
                        "checking timelines for retry type " + retryType + " for delay " +
                                expectedDelay +
                                " for workflow id: " + action.getExternalId());

                if (retryType.equalsIgnoreCase("periodic")) {

                    //in this case the delay unit will always be a constant time diff
                    for (int i = 0; i < instanceFinishTimes.size() - 1; i++) {
                        DateTime temp = formatter.parseDateTime(instanceFinishTimes.get(i));

                        Assert.assertEquals(temp.plusMinutes(expectedDelay).getMillis(),
                                formatter.parseDateTime(instanceRetryTimes.get(i)).getMillis(),
                                5000,
                                "oops! this is out of expected delay range for workflow id  " +
                                        action.getExternalId());
                        //Assert.assertEquals(formatter.print(temp.plusMinutes(expectedDelay)), instanceRetryTimes.get(i));
                    }
                } else {
                    //check for exponential

                    for (int i = 0; i < instanceFinishTimes.size() - 1; i++) {
                        DateTime temp = formatter.parseDateTime(instanceFinishTimes.get(i));
                        //Assert.assertEquals(formatter.print(temp.plusMinutes(expectedDelay)), instanceRetryTimes.get(i));
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

    private void submitClusters(Bundle bundle) throws Exception {
        for (String cluster : bundle.getClusters()) {
            Util.assertSucceeded(
                    prismHelper.getClusterHelper().submitEntity(URLS.SUBMIT_URL, cluster));
        }
    }

}

