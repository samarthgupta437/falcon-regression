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

public class PrismProcessSuspendTest {


    @BeforeMethod(alwaysRun = true)
    public void testName(Method method) {
        Util.print("test name: " + method.getName());
    }


    PrismHelper prismHelper = new PrismHelper("prism.properties");
    ColoHelper UA1ColoHelper = new ColoHelper("mk-qa.config.properties");
    ColoHelper UA2ColoHelper = new ColoHelper("ivoryqa-1.config.properties");


    @Test(dataProvider = "DP", groups = {"prism", "0.2"})
    public void testSuspendSuspendedProcessOnBothColosWhen1ColoIsDown(Bundle bundle)
    throws Exception {
        try {
            Bundle UA1Bundle = new Bundle(bundle, UA1ColoHelper.getEnvFileName());
            Bundle UA2Bundle = new Bundle(bundle, UA2ColoHelper.getEnvFileName());

            UA1Bundle.generateUniqueBundle();
            UA2Bundle.generateUniqueBundle();

            //schedule using colohelpers
            submitAndScheduleProcessUsingColoHelper(UA1ColoHelper, UA1Bundle);
            submitAndScheduleProcessUsingColoHelper(UA2ColoHelper, UA2Bundle);


            //suspend using prismHelper
            Util.assertSucceeded(
                    prismHelper.getProcessHelper()
                            .suspend(Util.URLS.SUSPEND_URL, UA1Bundle.getProcessData()));
            //verify
            Assert.assertTrue(
                    Util.getOozieJobStatus(Util.readEntityName(UA1Bundle.getProcessData()),
                            "SUSPENDED", UA1ColoHelper)
                            .get(0).contains("SUSPENDED"));
            Assert.assertTrue(
                    Util.getOozieJobStatus(Util.readEntityName(UA2Bundle.getProcessData()),
                            "RUNNING", UA2ColoHelper)
                            .get(0).contains("RUNNING"));

            Util.shutDownService(UA1ColoHelper.getProcessHelper());

            Util.assertFailed(
                    prismHelper.getProcessHelper()
                            .suspend(Util.URLS.SUSPEND_URL, UA1Bundle.getProcessData()));


            for (int i = 0; i < 2; i++) {
                //suspend on the other one
                Util.assertSucceeded(
                        prismHelper.getProcessHelper()
                                .suspend(Util.URLS.SUSPEND_URL, UA2Bundle.getProcessData()));
                Assert.assertTrue(
                        Util.getOozieJobStatus(Util.readEntityName(UA1Bundle.getProcessData()),
                                "SUSPENDED",
                                UA1ColoHelper).get(0).contains("SUSPENDED"));
                Assert.assertTrue(
                        Util.getOozieJobStatus(Util.readEntityName(UA2Bundle.getProcessData()),
                                "SUSPENDED",
                                UA2ColoHelper).get(0).contains("SUSPENDED"));
            }

        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getCause());
        } finally {
            Util.restartService(UA1ColoHelper.getProcessHelper());
        }
    }


    @Test(dataProvider = "DP", groups = {"prism", "0.2"})
    public void testSuspendScheduledProcessOnBothColos(Bundle bundle) throws Exception {
        Bundle UA1Bundle = new Bundle(bundle, UA1ColoHelper.getEnvFileName());
        Bundle UA2Bundle = new Bundle(bundle, UA2ColoHelper.getEnvFileName());

        UA1Bundle.generateUniqueBundle();
        UA2Bundle.generateUniqueBundle();

        //schedule using colohelpers
        submitAndScheduleProcessUsingColoHelper(UA1ColoHelper, UA1Bundle);
        submitAndScheduleProcessUsingColoHelper(UA2ColoHelper, UA2Bundle);

        //suspend using prismHelper
        Util.assertSucceeded(prismHelper.getProcessHelper()
                .suspend(Util.URLS.SUSPEND_URL, UA1Bundle.getProcessData()));
        //verify
        Assert.assertTrue(
                Util.getOozieJobStatus(Util.readEntityName(UA1Bundle.getProcessData()), "SUSPENDED",
                        UA1ColoHelper)
                        .get(0).contains("SUSPENDED"));
        Assert.assertTrue(
                Util.getOozieJobStatus(Util.readEntityName(UA2Bundle.getProcessData()), "RUNNING",
                        UA2ColoHelper).get(0)
                        .contains("RUNNING"));


        //suspend on the other one
        Util.assertSucceeded(prismHelper.getProcessHelper()
                .suspend(Util.URLS.SUSPEND_URL, UA1Bundle.getProcessData()));
        Assert.assertTrue(
                Util.getOozieJobStatus(Util.readEntityName(UA1Bundle.getProcessData()), "SUSPENDED",
                        UA1ColoHelper)
                        .get(0).contains("SUSPENDED"));
        Assert.assertTrue(
                Util.getOozieJobStatus(Util.readEntityName(UA2Bundle.getProcessData()), "RUNNING",
                        UA2ColoHelper).get(0)
                        .contains("RUNNING"));

        Assert.assertEquals(Util.parseResponse(
                prismHelper.getProcessHelper()
                        .getStatus(URLS.STATUS_URL, UA1Bundle.getProcessData())).getMessage(),
                "ua1/SUSPENDED");
        Assert.assertEquals(Util.parseResponse(
                prismHelper.getProcessHelper()
                        .getStatus(URLS.STATUS_URL, UA2Bundle.getProcessData())).getMessage(),
                "ua2/RUNNING");
    }

    @Test(dataProvider = "DP", groups = {"prism", "0.2"})
    public void testSuspendDeletedProcessOnBothColos(Bundle bundle) throws Exception {
        Bundle UA1Bundle = new Bundle(bundle, UA1ColoHelper.getEnvFileName());
        Bundle UA2Bundle = new Bundle(bundle, UA2ColoHelper.getEnvFileName());

        UA1Bundle.generateUniqueBundle();
        UA2Bundle.generateUniqueBundle();

        //schedule using colohelpers
        submitAndScheduleProcessUsingColoHelper(UA1ColoHelper, UA1Bundle);
        submitAndScheduleProcessUsingColoHelper(UA2ColoHelper, UA2Bundle);

        //delete using coloHelpers
        Util.assertSucceeded(prismHelper.getProcessHelper()
                .delete(Util.URLS.DELETE_URL, UA1Bundle.getProcessData()));


        //suspend using prismHelper
        Util.assertFailed(prismHelper.getProcessHelper()
                .suspend(Util.URLS.SUSPEND_URL, UA1Bundle.getProcessData()));
        //verify
        Assert.assertTrue(
                Util.getOozieJobStatus(Util.readEntityName(UA1Bundle.getProcessData()), "KILLED",
                        UA1ColoHelper).get(0)
                        .contains("KILLED"));
        Assert.assertTrue(
                Util.getOozieJobStatus(Util.readEntityName(UA2Bundle.getProcessData()), "RUNNING",
                        UA2ColoHelper).get(0)
                        .contains("RUNNING"));


        Util.assertSucceeded(prismHelper.getProcessHelper()
                .delete(Util.URLS.DELETE_URL, UA2Bundle.getProcessData()));
        //suspend on the other one
        Util.assertFailed(prismHelper.getProcessHelper()
                .suspend(Util.URLS.SUSPEND_URL, UA2Bundle.getProcessData()));
        Assert.assertTrue(
                Util.getOozieJobStatus(Util.readEntityName(UA1Bundle.getProcessData()), "KILLED",
                        UA1ColoHelper).get(0)
                        .contains("KILLED"));
        Assert.assertTrue(
                Util.getOozieJobStatus(Util.readEntityName(UA2Bundle.getProcessData()), "KILLED",
                        UA2ColoHelper).get(0)
                        .contains("KILLED"));
    }

    @Test(dataProvider = "DP", groups = {"prism", "0.2"})
    public void testSuspendSuspendedProcessOnBothColos(Bundle bundle) throws Exception {
        Bundle UA1Bundle = new Bundle(bundle, UA1ColoHelper.getEnvFileName());
        Bundle UA2Bundle = new Bundle(bundle, UA2ColoHelper.getEnvFileName());

        UA1Bundle.generateUniqueBundle();
        UA2Bundle.generateUniqueBundle();

        //schedule using colohelpers
        submitAndScheduleProcessUsingColoHelper(UA1ColoHelper, UA1Bundle);
        submitAndScheduleProcessUsingColoHelper(UA2ColoHelper, UA2Bundle);


        for (int i = 0; i < 2; i++) {
            //suspend using prismHelper
            Util.assertSucceeded(
                    prismHelper.getProcessHelper()
                            .suspend(Util.URLS.SUSPEND_URL, UA1Bundle.getProcessData()));
            //verify
            Assert.assertTrue(
                    Util.getOozieJobStatus(Util.readEntityName(UA1Bundle.getProcessData()),
                            "SUSPENDED", UA1ColoHelper)
                            .get(0).contains("SUSPENDED"));
            Assert.assertTrue(
                    Util.getOozieJobStatus(Util.readEntityName(UA2Bundle.getProcessData()),
                            "RUNNING", UA2ColoHelper)
                            .get(0).contains("RUNNING"));
        }


        for (int i = 0; i < 2; i++) {
            //suspend on the other one
            Util.assertSucceeded(
                    prismHelper.getProcessHelper()
                            .suspend(Util.URLS.SUSPEND_URL, UA2Bundle.getProcessData()));
            Assert.assertTrue(
                    Util.getOozieJobStatus(Util.readEntityName(UA1Bundle.getProcessData()),
                            "SUSPENDED", UA1ColoHelper)
                            .get(0).contains("SUSPENDED"));
            Assert.assertTrue(
                    Util.getOozieJobStatus(Util.readEntityName(UA2Bundle.getProcessData()),
                            "SUSPENDED", UA2ColoHelper)
                            .get(0).contains("SUSPENDED"));
        }
    }

    @Test(dataProvider = "DP")
    public void testSuspendNonExistentProcessOnBothColos(Bundle bundle) throws Exception {
        Bundle UA1Bundle = new Bundle(bundle, UA1ColoHelper.getEnvFileName());
        Bundle UA2Bundle = new Bundle(bundle, UA2ColoHelper.getEnvFileName());

        UA1Bundle.generateUniqueBundle();
        UA2Bundle.generateUniqueBundle();

        Util.assertFailed(prismHelper.getProcessHelper()
                .suspend(Util.URLS.SUSPEND_URL, UA1Bundle.getProcessData()));
        Util.assertFailed(prismHelper.getProcessHelper()
                .suspend(Util.URLS.SUSPEND_URL, UA2Bundle.getProcessData()));

        Util.assertFailed(UA1ColoHelper.getProcessHelper()
                .suspend(Util.URLS.SUSPEND_URL, UA1Bundle.getProcessData()));
        Util.assertFailed(UA2ColoHelper.getProcessHelper()
                .suspend(Util.URLS.SUSPEND_URL, UA1Bundle.getProcessData()));
    }

    @Test(dataProvider = "DP")
    public void testSuspendSubmittedProcessOnBothColos(Bundle bundle) throws Exception {
        Bundle UA1Bundle = new Bundle(bundle, UA1ColoHelper.getEnvFileName());
        Bundle UA2Bundle = new Bundle(bundle, UA2ColoHelper.getEnvFileName());

        UA1Bundle.generateUniqueBundle();
        UA2Bundle.generateUniqueBundle();

        submitProcess(UA1Bundle);
        submitProcess(UA2Bundle);

        Util.assertFailed(prismHelper.getProcessHelper()
                .suspend(Util.URLS.SUSPEND_URL, UA1Bundle.getProcessData()));
        Util.assertFailed(prismHelper.getProcessHelper()
                .suspend(Util.URLS.SUSPEND_URL, UA2Bundle.getProcessData()));

        Util.assertFailed(UA1ColoHelper.getProcessHelper()
                .suspend(Util.URLS.SUSPEND_URL, UA1Bundle.getProcessData()));
        Util.assertFailed(UA2ColoHelper.getProcessHelper()
                .suspend(Util.URLS.SUSPEND_URL, UA2Bundle.getProcessData()));


    }

    @Test(dataProvider = "DP", groups = {"prism", "0.2"})
    public void testSuspendScheduledProcessOnBothColosWhen1ColoIsDown(Bundle bundle)
    throws Exception {
        try {
            Bundle UA1Bundle = new Bundle(bundle, UA1ColoHelper.getEnvFileName());
            Bundle UA2Bundle = new Bundle(bundle, UA2ColoHelper.getEnvFileName());

            UA1Bundle.generateUniqueBundle();
            UA2Bundle.generateUniqueBundle();

            //schedule using colohelpers
            submitAndScheduleProcessUsingColoHelper(UA1ColoHelper, UA1Bundle);
            submitAndScheduleProcessUsingColoHelper(UA2ColoHelper, UA2Bundle);

            Util.shutDownService(UA1ColoHelper.getProcessHelper());

            //suspend using prismHelper
            Util.assertFailed(
                    prismHelper.getProcessHelper()
                            .suspend(Util.URLS.SUSPEND_URL, UA1Bundle.getProcessData()));
            //verify
            Assert.assertTrue(
                    Util.getOozieJobStatus(Util.readEntityName(UA2Bundle.getProcessData()),
                            "RUNNING", UA2ColoHelper)
                            .get(0).contains("RUNNING"));

            //suspend on the other one
            Util.assertSucceeded(
                    prismHelper.getProcessHelper()
                            .suspend(Util.URLS.SUSPEND_URL, UA2Bundle.getProcessData()));
            Assert.assertTrue(
                    Util.getOozieJobStatus(Util.readEntityName(UA2Bundle.getProcessData()),
                            "SUSPENDED", UA2ColoHelper)
                            .get(0).contains("SUSPENDED"));
            Assert.assertTrue(
                    Util.getOozieJobStatus(Util.readEntityName(UA1Bundle.getProcessData()),
                            "RUNNING", UA1ColoHelper)
                            .get(0).contains("RUNNING"));
        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getCause());
        } finally {

            Util.restartService(UA1ColoHelper.getFeedHelper());
        }

    }

    @Test(dataProvider = "DP", groups = {"prism", "0.2"})
    public void testSuspendDeletedProcessOnBothColosWhen1ColoIsDown(Bundle bundle)
    throws Exception {
        try {
            Bundle UA1Bundle = new Bundle(bundle, UA1ColoHelper.getEnvFileName());
            Bundle UA2Bundle = new Bundle(bundle, UA2ColoHelper.getEnvFileName());

            UA1Bundle.generateUniqueBundle();
            UA2Bundle.generateUniqueBundle();

            //schedule using colohelpers
            submitAndScheduleProcessUsingColoHelper(UA1ColoHelper, UA1Bundle);
            submitAndScheduleProcessUsingColoHelper(UA2ColoHelper, UA2Bundle);

            //delete using coloHelpers
            Util.assertSucceeded(
                    prismHelper.getProcessHelper()
                            .delete(Util.URLS.DELETE_URL, UA1Bundle.getProcessData()));

            Util.shutDownService(UA1ColoHelper.getProcessHelper());

            //suspend using prismHelper
            Util.assertFailed(
                    prismHelper.getProcessHelper()
                            .suspend(Util.URLS.SUSPEND_URL, UA1Bundle.getProcessData()));
            //verify
            Assert.assertTrue(
                    Util.getOozieJobStatus(Util.readEntityName(UA1Bundle.getProcessData()),
                            "KILLED", UA1ColoHelper)
                            .get(0).contains("KILLED"));
            Assert.assertTrue(
                    Util.getOozieJobStatus(Util.readEntityName(UA2Bundle.getProcessData()),
                            "RUNNING", UA2ColoHelper)
                            .get(0).contains("RUNNING"));


            Util.assertSucceeded(
                    prismHelper.getProcessHelper()
                            .delete(Util.URLS.DELETE_URL, UA2Bundle.getProcessData()));
            //suspend on the other one
            Util.assertFailed(
                    prismHelper.getProcessHelper()
                            .suspend(Util.URLS.SUSPEND_URL, UA2Bundle.getProcessData()));
            Assert.assertTrue(
                    Util.getOozieJobStatus(Util.readEntityName(UA1Bundle.getProcessData()),
                            "KILLED", UA1ColoHelper)
                            .get(0).contains("KILLED"));
            Assert.assertTrue(
                    Util.getOozieJobStatus(Util.readEntityName(UA2Bundle.getProcessData()),
                            "KILLED", UA2ColoHelper)
                            .get(0).contains("KILLED"));
        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getCause());
        } finally {
            Util.restartService(UA1ColoHelper.getFeedHelper());
        }
    }


    @Test(dataProvider = "DP")
    public void testSuspendNonExistentProcessOnBothColosWhen1ColoIsDown(Bundle bundle)
    throws Exception {
        try {
            Bundle UA1Bundle = new Bundle(bundle, UA1ColoHelper.getEnvFileName());
            Bundle UA2Bundle = new Bundle(bundle, UA2ColoHelper.getEnvFileName());

            UA1Bundle.generateUniqueBundle();
            UA2Bundle.generateUniqueBundle();

            Util.shutDownService(UA1ColoHelper.getProcessHelper());

            Util.assertFailed(
                    prismHelper.getProcessHelper()
                            .suspend(Util.URLS.SUSPEND_URL, UA2Bundle.getProcessData()));
            Util.assertFailed(
                    prismHelper.getProcessHelper()
                            .suspend(Util.URLS.SUSPEND_URL, UA1Bundle.getProcessData()));

            Util.assertFailed(
                    UA2ColoHelper.getProcessHelper()
                            .suspend(Util.URLS.SUSPEND_URL, UA1Bundle.getProcessData()));
        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getCause());
        } finally {
            Util.restartService(UA1ColoHelper.getProcessHelper());
        }
    }

    @Test(dataProvider = "DP")
    public void testSuspendSubmittedFeedOnBothColosWhen1ColoIsDown(Bundle bundle) throws Exception {
        try {
            Bundle UA1Bundle = new Bundle(bundle, UA1ColoHelper.getEnvFileName());
            Bundle UA2Bundle = new Bundle(bundle, UA2ColoHelper.getEnvFileName());

            UA1Bundle.generateUniqueBundle();
            UA2Bundle.generateUniqueBundle();

            submitProcess(UA1Bundle);
            submitProcess(UA2Bundle);

            Util.shutDownService(UA1ColoHelper.getProcessHelper());

            Util.assertFailed(
                    prismHelper.getProcessHelper()
                            .suspend(Util.URLS.SUSPEND_URL, UA1Bundle.getProcessData()));
            Util.assertFailed(
                    prismHelper.getProcessHelper()
                            .suspend(Util.URLS.SUSPEND_URL, UA2Bundle.getProcessData()));


            Util.assertFailed(
                    UA2ColoHelper.getProcessHelper()
                            .suspend(Util.URLS.SUSPEND_URL, UA2Bundle.getProcessData()));
        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getCause());
        } finally {
            Util.restartService(UA1ColoHelper.getProcessHelper());
        }

    }

    private void submitProcess(Bundle bundle) throws Exception {
        for (String cluster : bundle.getClusters()) {
            Util.assertSucceeded(
                    prismHelper.getClusterHelper().submitEntity(Util.URLS.SUBMIT_URL, cluster));
        }
        for (String feed : bundle.getDataSets()) {
            Util.assertSucceeded(
                    prismHelper.getFeedHelper().submitAndSchedule(URLS.SUBMIT_URL, feed));
        }

        Util.print("process being submitted: " + bundle.getProcessData());
        Util.assertSucceeded(
                prismHelper.getProcessHelper()
                        .submitEntity(Util.URLS.SUBMIT_URL, bundle.getProcessData()));
    }


    private void submitAndScheduleProcess(Bundle bundle) throws Exception {
        submitProcess(bundle);
        Util.assertSucceeded(prismHelper.getProcessHelper()
                .schedule(Util.URLS.SCHEDULE_URL, bundle.getProcessData()));
    }

    private void submitAndScheduleProcessUsingColoHelper(ColoHelper coloHelper, Bundle bundle)
    throws Exception {
        submitProcess(bundle);
        Util.print("process being submitted: " + bundle.getProcessData());
        Util.assertSucceeded(coloHelper.getProcessHelper()
                .schedule(Util.URLS.SCHEDULE_URL, bundle.getProcessData()));
    }

    @DataProvider(name = "DP")
    public Object[][] getData() throws Exception {
        return Util.readBundles("LateDataBundles");
    }

}
