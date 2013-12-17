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
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.helpers.PrismHelper;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.core.util.Util.URLS;
import org.testng.Assert;
import org.testng.TestNGException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.lang.reflect.Method;

public class PrismProcessScheduleTest {

    @BeforeMethod(alwaysRun = true)
    public void testName(Method method) {
        Util.print("test name: " + method.getName());
    }


    PrismHelper prismHelper = new PrismHelper("prism.properties");
    ColoHelper UA1ColoHelper = new ColoHelper("mk-qa.config.properties");
    ColoHelper UA2ColoHelper = new ColoHelper("ivoryqa-1.config.properties");

    @Test(dataProvider = "DP", groups = {"prism", "0.2"})
    public void testProcessScheduleOnBothColos(Bundle bundle) throws Exception {
        Bundle UA1Bundle = new Bundle(bundle, UA1ColoHelper.getEnvFileName());
        Bundle UA2Bundle = new Bundle(bundle, UA2ColoHelper.getEnvFileName());

        UA1Bundle.generateUniqueBundle();
        UA2Bundle.generateUniqueBundle();

        //schedule both bundles
        submitAndScheduleProcess(UA1Bundle);
        Assert.assertTrue(
                Util.getOozieJobStatus(Util.readEntityName(UA1Bundle.getProcessData()), "RUNNING",
                        UA1ColoHelper).get(0)
                        .contains("RUNNING"));
        Util.verifyNoJobsFoundInOozie(
                Util.getOozieJobStatus(Util.readEntityName(UA2Bundle.getProcessData()), "RUNNING",
                        UA1ColoHelper));


        submitAndScheduleProcess(UA2Bundle);

        //now check if they have been scheduled correctly or not

        Assert.assertTrue(
                Util.getOozieJobStatus(Util.readEntityName(UA2Bundle.getProcessData()), "RUNNING",
                        UA2ColoHelper).get(0)
                        .contains("RUNNING"));

        //check if there is no criss cross

        Util.verifyNoJobsFoundInOozie(
                Util.getOozieJobStatus(Util.readEntityName(UA1Bundle.getProcessData()), "RUNNING",
                        UA2ColoHelper));

    }

    @Test(dataProvider = "DP", groups = {"prism", "0.2"})
    public void testScheduleAlreadyScheduledProcessOnBothColos(Bundle bundle) throws Exception {
        Bundle UA1Bundle = new Bundle(bundle, UA1ColoHelper.getEnvFileName());
        Bundle UA2Bundle = new Bundle(bundle, UA2ColoHelper.getEnvFileName());

        UA1Bundle.generateUniqueBundle();
        UA2Bundle.generateUniqueBundle();

        //schedule both bundles
        submitAndScheduleProcess(UA1Bundle);
        submitAndScheduleProcess(UA2Bundle);

        //now check if they have been scheduled correctly or not
        Assert.assertTrue(
                Util.getOozieJobStatus(Util.readEntityName(UA1Bundle.getProcessData()), "RUNNING",
                        UA1ColoHelper).get(0)
                        .contains("RUNNING"));
        Assert.assertTrue(
                Util.getOozieJobStatus(Util.readEntityName(UA2Bundle.getProcessData()), "RUNNING",
                        UA2ColoHelper).get(0)
                        .contains("RUNNING"));

        //check if there is no criss cross
        Util.verifyNoJobsFoundInOozie(
                Util.getOozieJobStatus(Util.readEntityName(UA2Bundle.getProcessData()), "RUNNING",
                        UA1ColoHelper));
        Util.verifyNoJobsFoundInOozie(
                Util.getOozieJobStatus(Util.readEntityName(UA1Bundle.getProcessData()), "RUNNING",
                        UA2ColoHelper));


        Util.assertSucceeded(UA1ColoHelper.getProcessHelper()
                .schedule(URLS.SCHEDULE_URL, UA1Bundle.getProcessData()));
        Util.assertSucceeded(UA2ColoHelper.getProcessHelper()
                .schedule(URLS.SCHEDULE_URL, UA2Bundle.getProcessData()));
        //now check if they have been scheduled correctly or not
        Assert.assertTrue(
                Util.getOozieJobStatus(Util.readEntityName(UA1Bundle.getProcessData()), "RUNNING",
                        UA1ColoHelper).get(0)
                        .contains("RUNNING"));
        Assert.assertTrue(
                Util.getOozieJobStatus(Util.readEntityName(UA2Bundle.getProcessData()), "RUNNING",
                        UA2ColoHelper).get(0)
                        .contains("RUNNING"));


    }

    @Test(dataProvider = "DP", groups = {"prism", "0.2"})
    public void testScheduleSuspendedProcessOnBothColos(Bundle bundle) throws Exception {
        Bundle UA1Bundle = new Bundle(bundle, UA1ColoHelper.getEnvFileName());
        Bundle UA2Bundle = new Bundle(bundle, UA2ColoHelper.getEnvFileName());

        UA1Bundle.generateUniqueBundle();
        UA2Bundle.generateUniqueBundle();

        //schedule both bundles
        submitAndScheduleProcess(UA1Bundle);
        submitAndScheduleProcess(UA2Bundle);

        Util.assertSucceeded(UA1ColoHelper.getProcessHelper()
                .suspend(URLS.SUSPEND_URL, UA1Bundle.getProcessData()));
        Assert.assertTrue(
                Util.getOozieJobStatus(Util.readEntityName(UA1Bundle.getProcessData()), "SUSPENDED",
                        UA1ColoHelper)
                        .get(0).contains("SUSPENDED"));
        Assert.assertTrue(
                Util.getOozieJobStatus(Util.readEntityName(UA2Bundle.getProcessData()), "RUNNING",
                        UA2ColoHelper).get(0)
                        .contains("RUNNING"));
        //now check if they have been scheduled correctly or not

        Util.assertSucceeded(UA1ColoHelper.getProcessHelper()
                .schedule(URLS.SCHEDULE_URL, UA1Bundle.getProcessData()));
        Assert.assertTrue(
                Util.getOozieJobStatus(Util.readEntityName(UA1Bundle.getProcessData()), "SUSPENDED",
                        UA1ColoHelper)
                        .get(0).contains("SUSPENDED"));
        Util.assertSucceeded(UA1ColoHelper.getProcessHelper()
                .resume(URLS.RESUME_URL, UA1Bundle.getProcessData()));
        Assert.assertTrue(
                Util.getOozieJobStatus(Util.readEntityName(UA1Bundle.getProcessData()), "RUNNING",
                        UA1ColoHelper).get(0)
                        .contains("RUNNING"));


        Util.assertSucceeded(UA2ColoHelper.getProcessHelper()
                .suspend(URLS.SUSPEND_URL, UA2Bundle.getProcessData()));
        Assert.assertTrue(
                Util.getOozieJobStatus(Util.readEntityName(UA2Bundle.getProcessData()), "SUSPENDED",
                        UA2ColoHelper)
                        .get(0).contains("SUSPENDED"));
        Assert.assertTrue(
                Util.getOozieJobStatus(Util.readEntityName(UA1Bundle.getProcessData()), "RUNNING",
                        UA1ColoHelper).get(0)
                        .contains("RUNNING"));

    }

    @Test(dataProvider = "DP", groups = {"prism", "0.2"})
    public void testScheduleDeletedProcessOnBothColos(Bundle bundle) throws Exception {
        Bundle UA1Bundle = new Bundle(bundle, UA1ColoHelper.getEnvFileName());
        Bundle UA2Bundle = new Bundle(bundle, UA2ColoHelper.getEnvFileName());

        UA1Bundle.generateUniqueBundle();
        UA2Bundle.generateUniqueBundle();

        //schedule both bundles
        submitAndScheduleProcess(UA1Bundle);
        submitAndScheduleProcess(UA2Bundle);

        Util.assertSucceeded(
                prismHelper.getProcessHelper().delete(URLS.DELETE_URL, UA1Bundle.getProcessData()));
        Assert.assertTrue(
                Util.getOozieJobStatus(Util.readEntityName(UA1Bundle.getProcessData()), "KILLED",
                        UA1ColoHelper).get(0)
                        .contains("KILLED"));
        Assert.assertTrue(
                Util.getOozieJobStatus(Util.readEntityName(UA2Bundle.getProcessData()), "RUNNING",
                        UA2ColoHelper).get(0)
                        .contains("RUNNING"));

        Util.assertSucceeded(
                prismHelper.getProcessHelper().delete(URLS.DELETE_URL, UA2Bundle.getProcessData()));
        Assert.assertTrue(
                Util.getOozieJobStatus(Util.readEntityName(UA2Bundle.getProcessData()), "KILLED",
                        UA2ColoHelper).get(0)
                        .contains("KILLED"));
        Assert.assertTrue(
                Util.getOozieJobStatus(Util.readEntityName(UA1Bundle.getProcessData()), "KILLED",
                        UA1ColoHelper).get(0)
                        .contains("KILLED"));

        Util.assertFailed(UA1ColoHelper.getProcessHelper()
                .schedule(URLS.SCHEDULE_URL, UA1Bundle.getProcessData()));
        Util.assertFailed(UA2ColoHelper.getProcessHelper()
                .schedule(URLS.SCHEDULE_URL, UA2Bundle.getProcessData()));

    }


    @Test(dataProvider = "DP", groups = {"prism", "0.2"})
    public void testScheduleNonExistentProcessOnBothColos(Bundle bundle) throws Exception {
        Bundle UA1Bundle = new Bundle(bundle, UA1ColoHelper.getEnvFileName());
        Bundle UA2Bundle = new Bundle(bundle, UA2ColoHelper.getEnvFileName());

        UA1Bundle.generateUniqueBundle();
        UA2Bundle.generateUniqueBundle();

        Util.assertFailed(UA1ColoHelper.getProcessHelper()
                .schedule(URLS.SCHEDULE_URL, UA1Bundle.getProcessData()));
        Util.assertFailed(UA2ColoHelper.getProcessHelper()
                .schedule(URLS.SCHEDULE_URL, UA2Bundle.getProcessData()));

    }


    @Test(dataProvider = "DP", groups = {"prism", "0.2"})
    public void testProcessScheduleOn1ColoWhileOtherColoIsDown(Bundle bundle) throws Exception {
        try {
            Bundle UA1Bundle = new Bundle(bundle, UA1ColoHelper.getEnvFileName());
            Bundle UA2Bundle = new Bundle(bundle, UA2ColoHelper.getEnvFileName());

            UA1Bundle.generateUniqueBundle();
            UA2Bundle.generateUniqueBundle();

            submitProcess(UA2Bundle);

            Util.shutDownService(UA1ColoHelper.getProcessHelper());

            Util.assertSucceeded(prismHelper.getProcessHelper()
                    .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, UA2Bundle.getProcessData()));

            //now check if they have been scheduled correctly or not

            Assert.assertTrue(
                    Util.getOozieJobStatus(Util.readEntityName(UA2Bundle.getProcessData()),
                            "RUNNING", UA2ColoHelper)
                            .get(0).contains("RUNNING"));

            //check if there is no criss cross

            Util.verifyNoJobsFoundInOozie(
                    Util.getOozieJobStatus(Util.readEntityName(UA1Bundle.getProcessData()),
                            "RUNNING", UA2ColoHelper));
        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getMessage());
        } finally {

            Util.restartService(UA1ColoHelper.getProcessHelper());

        }

    }


    @Test(dataProvider = "DP", groups = {"prism", "0.2"})
    public void testProcessScheduleOn1ColoWhileThatColoIsDown(Bundle bundle) throws Exception {
        try {
            Bundle UA1Bundle = new Bundle(bundle, UA1ColoHelper.getEnvFileName());
            Bundle UA2Bundle = new Bundle(bundle, UA2ColoHelper.getEnvFileName());

            UA1Bundle.generateUniqueBundle();
            UA2Bundle.generateUniqueBundle();

            submitProcess(UA1Bundle);

            Util.shutDownService(UA1ColoHelper.getProcessHelper());

            Util.assertFailed(prismHelper.getProcessHelper()
                    .schedule(URLS.SCHEDULE_URL, UA1Bundle.getProcessData()));
            Util.verifyNoJobsFoundInOozie(
                    Util.getOozieJobStatus(Util.readEntityName(UA1Bundle.getProcessData()),
                            "RUNNING", UA2ColoHelper));
        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getMessage());
        } finally {

            Util.restartService(UA1ColoHelper.getProcessHelper());

        }

    }

    @Test(dataProvider = "DP", groups = {"prism", "0.2"})
    public void testProcessScheduleOn1ColoWhileAnotherColoHasSuspendedProcess(Bundle bundle)
    throws Exception {
        try {
            Bundle UA1Bundle = new Bundle(bundle, UA1ColoHelper.getEnvFileName());
            Bundle UA2Bundle = new Bundle(bundle, UA2ColoHelper.getEnvFileName());

            UA1Bundle.generateUniqueBundle();
            UA2Bundle.generateUniqueBundle();

            submitAndScheduleProcess(UA1Bundle);
            Util.assertSucceeded(UA1Bundle.getProcessHelper()
                    .suspend(URLS.SUSPEND_URL, UA1Bundle.getProcessData()));
            Assert.assertTrue(
                    Util.getOozieJobStatus(Util.readEntityName(UA1Bundle.getProcessData()),
                            "SUSPENDED", UA1ColoHelper)
                            .get(0).contains("SUSPENDED"));

            submitAndScheduleProcess(UA2Bundle);
            Assert.assertTrue(
                    Util.getOozieJobStatus(Util.readEntityName(UA2Bundle.getProcessData()),
                            "RUNNING", UA2ColoHelper)
                            .get(0).contains("RUNNING"));
            Util.verifyNoJobsFoundInOozie(
                    Util.getOozieJobStatus(Util.readEntityName(UA1Bundle.getProcessData()),
                            "RUNNING", UA2ColoHelper));
            Assert.assertTrue(
                    Util.getOozieJobStatus(Util.readEntityName(UA1Bundle.getProcessData()),
                            "SUSPENDED", UA1ColoHelper)
                            .get(0).contains("SUSPENDED"));
            Util.verifyNoJobsFoundInOozie(
                    Util.getOozieJobStatus(Util.readEntityName(UA2Bundle.getProcessData()),
                            "RUNNING", UA1ColoHelper));

        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getMessage());
        }

    }

    @Test(dataProvider = "DP", groups = {"prism", "0.2"})
    public void testProcessScheduleOn1ColoWhileAnotherColoHasKilledProcess(Bundle bundle)
    throws Exception {
        try {
            Bundle UA1Bundle = new Bundle(bundle, UA1ColoHelper.getEnvFileName());
            Bundle UA2Bundle = new Bundle(bundle, UA2ColoHelper.getEnvFileName());

            UA1Bundle.generateUniqueBundle();
            UA2Bundle.generateUniqueBundle();

            submitAndScheduleProcess(UA1Bundle);
            Util.assertSucceeded(prismHelper.getProcessHelper()
                    .delete(URLS.DELETE_URL, UA1Bundle.getProcessData()));
            Assert.assertTrue(
                    Util.getOozieJobStatus(Util.readEntityName(UA1Bundle.getProcessData()),
                            "KILLED", UA1ColoHelper)
                            .get(0).contains("KILLED"));

            submitAndScheduleProcess(UA2Bundle);
            Assert.assertTrue(
                    Util.getOozieJobStatus(Util.readEntityName(UA2Bundle.getProcessData()),
                            "RUNNING", UA2ColoHelper)
                            .get(0).contains("RUNNING"));
            Util.verifyNoJobsFoundInOozie(
                    Util.getOozieJobStatus(Util.readEntityName(UA1Bundle.getProcessData()),
                            "RUNNING", UA2ColoHelper));
            Assert.assertTrue(
                    Util.getOozieJobStatus(Util.readEntityName(UA1Bundle.getProcessData()),
                            "KILLED", UA1ColoHelper)
                            .get(0).contains("KILLED"));
            Util.verifyNoJobsFoundInOozie(
                    Util.getOozieJobStatus(Util.readEntityName(UA2Bundle.getProcessData()),
                            "RUNNING", UA1ColoHelper));

        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getMessage());
        }

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
            Util.assertSucceeded(
                    prismHelper.getFeedHelper().submitAndSchedule(URLS.SUBMIT_URL, feed));
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

    @DataProvider(name = "DP")
    public Object[][] getData() throws Exception {
        return Util.readBundles("LateDataBundles");
    }


}
