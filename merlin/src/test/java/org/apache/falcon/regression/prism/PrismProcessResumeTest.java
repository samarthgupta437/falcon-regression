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
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.helpers.PrismHelper;
import org.apache.falcon.regression.core.supportClasses.ENTITY_TYPE;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.core.util.Util.URLS;
import org.apache.oozie.client.Job;
import org.testng.Assert;
import org.testng.TestNGException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.lang.reflect.Method;

public class PrismProcessResumeTest {


    @BeforeMethod(alwaysRun = true)
    public void testName(Method method) {
        Util.print("test name: " + method.getName());
    }


    PrismHelper prismHelper = new PrismHelper("prism.properties");
    ColoHelper UA1ColoHelper = new ColoHelper("mk-qa.config.properties");
    ColoHelper UA2ColoHelper = new ColoHelper("ivoryqa-1.config.properties");

    @Test(dataProvider = "DP", groups = {"prism", "0.2"})
    public void testResumeSuspendedFeedOnBothColos(Bundle bundle) throws Exception {
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
        checkStatus(UA1ColoHelper, UA1Bundle, Job.Status.SUSPENDED);
        checkStatus(UA2ColoHelper, UA2Bundle, Job.Status.RUNNING);

        //suspend using prismHelper
        Util.assertSucceeded(prismHelper.getProcessHelper()
                .resume(Util.URLS.RESUME_URL, UA1Bundle.getProcessData()));
        //verify
        checkStatus(UA1ColoHelper, UA1Bundle, Job.Status.RUNNING);
        checkStatus(UA2ColoHelper, UA2Bundle, Job.Status.RUNNING);

        //try using the colohelper                
        Util.assertSucceeded(
                UA1ColoHelper.getProcessHelper()
                        .suspend(Util.URLS.SUSPEND_URL, UA1Bundle.getProcessData()));
        //verify
        checkStatus(UA1ColoHelper, UA1Bundle, Job.Status.SUSPENDED);
        checkStatus(UA2ColoHelper, UA2Bundle, Job.Status.RUNNING);

        //suspend using prismHelper
        Util.assertSucceeded(UA1ColoHelper.getProcessHelper()
                .resume(Util.URLS.RESUME_URL, UA1Bundle.getProcessData()));
        //verify
        checkStatus(UA1ColoHelper, UA1Bundle, Job.Status.RUNNING);
        checkStatus(UA2ColoHelper, UA2Bundle, Job.Status.RUNNING);

        //suspend on the other one
        Util.assertSucceeded(
                UA2ColoHelper.getProcessHelper()
                        .suspend(Util.URLS.SUSPEND_URL, UA2Bundle.getProcessData()));
        checkStatus(UA1ColoHelper, UA1Bundle, Job.Status.RUNNING);
        checkStatus(UA2ColoHelper, UA2Bundle, Job.Status.SUSPENDED);

        Util.assertSucceeded(UA2ColoHelper.getProcessHelper()
                .resume(Util.URLS.RESUME_URL, UA2Bundle.getProcessData()));
        checkStatus(UA1ColoHelper, UA1Bundle, Job.Status.RUNNING);
        checkStatus(UA2ColoHelper, UA2Bundle, Job.Status.RUNNING);
    }

    @Test(dataProvider = "DP", groups = {"prism", "0.2"})
    public void testResumeDeletedProcessOnBothColos(Bundle bundle) throws Exception {
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
                .resume(Util.URLS.RESUME_URL, UA1Bundle.getProcessData()));
        //verify
        checkStatus(UA1ColoHelper, UA1Bundle, Job.Status.KILLED);
        checkStatus(UA2ColoHelper, UA2Bundle, Job.Status.RUNNING);

        Util.assertSucceeded(prismHelper.getProcessHelper()
                .delete(Util.URLS.DELETE_URL, UA2Bundle.getProcessData()));
        //suspend on the other one
        Util.assertFailed(prismHelper.getProcessHelper()
                .resume(Util.URLS.RESUME_URL, UA2Bundle.getProcessData()));
        checkStatus(UA1ColoHelper, UA1Bundle, Job.Status.KILLED);
        checkStatus(UA2ColoHelper, UA2Bundle, Job.Status.KILLED);

        Util.assertFailed(UA1ColoHelper.getProcessHelper()
                .resume(Util.URLS.RESUME_URL, UA1Bundle.getProcessData()));
        checkStatus(UA1ColoHelper, UA1Bundle, Job.Status.KILLED);
        Util.assertFailed(UA2ColoHelper.getProcessHelper()
                .resume(Util.URLS.RESUME_URL, UA2Bundle.getProcessData()));
        checkStatus(UA2ColoHelper, UA2Bundle, Job.Status.KILLED);
    }

    @Test(dataProvider = "DP", groups = {"prism", "0.2"})
    public void testResumeResumedProcessOnBothColos(Bundle bundle) throws Exception {
        Bundle UA1Bundle = new Bundle(bundle, UA1ColoHelper.getEnvFileName());
        Bundle UA2Bundle = new Bundle(bundle, UA2ColoHelper.getEnvFileName());

        UA1Bundle.generateUniqueBundle();
        UA2Bundle.generateUniqueBundle();

        //schedule using colohelpers
        submitAndScheduleProcessUsingColoHelper(UA1ColoHelper, UA1Bundle);
        submitAndScheduleProcessUsingColoHelper(UA2ColoHelper, UA2Bundle);

        Util.assertSucceeded(prismHelper.getProcessHelper()
                .suspend(Util.URLS.SUSPEND_URL, UA1Bundle.getProcessData()));
        checkStatus(UA1ColoHelper, UA1Bundle, Job.Status.SUSPENDED);

        for (int i = 0; i < 2; i++) {
            //suspend using prismHelper
            Util.assertSucceeded(
                    prismHelper.getProcessHelper()
                            .resume(Util.URLS.RESUME_URL, UA1Bundle.getProcessData()));
            //verify
            checkStatus(UA1ColoHelper, UA1Bundle, Job.Status.RUNNING);
            checkStatus(UA2ColoHelper, UA2Bundle, Job.Status.RUNNING);
        }


        Util.assertSucceeded(prismHelper.getProcessHelper()
                .suspend(Util.URLS.SUSPEND_URL, UA2Bundle.getProcessData()));
        checkStatus(UA2ColoHelper, UA2Bundle, Job.Status.SUSPENDED);

        for (int i = 0; i < 2; i++) {
            Util.assertSucceeded(
                    UA1ColoHelper.getProcessHelper()
                            .resume(Util.URLS.RESUME_URL, UA1Bundle.getProcessData()));
            //verify
            checkStatus(UA1ColoHelper, UA1Bundle, Job.Status.RUNNING);
            checkStatus(UA2ColoHelper, UA2Bundle, Job.Status.SUSPENDED);
        }


        for (int i = 0; i < 2; i++) {
            //suspend on the other one
            Util.assertSucceeded(
                    prismHelper.getProcessHelper()
                            .resume(Util.URLS.RESUME_URL, UA2Bundle.getProcessData()));
            checkStatus(UA1ColoHelper, UA1Bundle, Job.Status.RUNNING);
            checkStatus(UA2ColoHelper, UA2Bundle, Job.Status.RUNNING);
        }

        for (int i = 0; i < 2; i++) {
            //suspend on the other one
            Util.assertSucceeded(
                    UA2ColoHelper.getProcessHelper()
                            .resume(Util.URLS.RESUME_URL, UA2Bundle.getProcessData()));
            checkStatus(UA1ColoHelper, UA1Bundle, Job.Status.RUNNING);
            checkStatus(UA2ColoHelper, UA2Bundle, Job.Status.RUNNING);
        }
    }

    @Test(dataProvider = "DP")
    public void testResumeNonExistentProcessOnBothColos(Bundle bundle) throws Exception {
        Bundle UA1Bundle = new Bundle(bundle, UA1ColoHelper.getEnvFileName());
        Bundle UA2Bundle = new Bundle(bundle, UA2ColoHelper.getEnvFileName());

        UA1Bundle.generateUniqueBundle();
        UA2Bundle.generateUniqueBundle();

        Util.assertFailed(prismHelper.getProcessHelper()
                .resume(Util.URLS.RESUME_URL, UA1Bundle.getProcessData()));
        Util.assertFailed(prismHelper.getProcessHelper()
                .resume(Util.URLS.RESUME_URL, UA2Bundle.getProcessData()));

        Util.assertFailed(UA1ColoHelper.getProcessHelper()
                .resume(Util.URLS.RESUME_URL, UA1Bundle.getProcessData()));
        Util.assertFailed(UA2ColoHelper.getProcessHelper()
                .resume(Util.URLS.RESUME_URL, UA1Bundle.getProcessData()));
    }

    @Test(dataProvider = "DP")
    public void testResumeSubmittedProcessOnBothColos(Bundle bundle) throws Exception {
        Bundle UA1Bundle = new Bundle(bundle, UA1ColoHelper.getEnvFileName());
        Bundle UA2Bundle = new Bundle(bundle, UA2ColoHelper.getEnvFileName());

        UA1Bundle.generateUniqueBundle();
        UA2Bundle.generateUniqueBundle();

        submitProcess(UA1Bundle);
        submitProcess(UA2Bundle);

        Util.assertFailed(prismHelper.getProcessHelper()
                .resume(Util.URLS.RESUME_URL, UA1Bundle.getProcessData()));
        Util.assertFailed(prismHelper.getProcessHelper()
                .resume(Util.URLS.RESUME_URL, UA2Bundle.getProcessData()));

        Util.assertFailed(UA1ColoHelper.getProcessHelper()
                .resume(Util.URLS.RESUME_URL, UA1Bundle.getProcessData()));
        Util.assertFailed(UA2ColoHelper.getProcessHelper()
                .resume(Util.URLS.RESUME_URL, UA2Bundle.getProcessData()));


    }

    @Test(dataProvider = "DP", groups = {"prism", "0.2"})
    public void testResumeScheduledProcessOnBothColosWhen1ColoIsDown(Bundle bundle)
    throws Exception {
        try {
            Bundle UA1Bundle = new Bundle(bundle, UA1ColoHelper.getEnvFileName());
            Bundle UA2Bundle = new Bundle(bundle, UA2ColoHelper.getEnvFileName());

            UA1Bundle.generateUniqueBundle();
            UA2Bundle.generateUniqueBundle();

            //schedule using colohelpers
            submitAndScheduleProcessUsingColoHelper(UA1ColoHelper, UA1Bundle);
            submitAndScheduleProcessUsingColoHelper(UA2ColoHelper, UA2Bundle);
            Util.assertSucceeded(
                    UA1ColoHelper.getProcessHelper()
                            .suspend(Util.URLS.SUSPEND_URL, UA1Bundle.getProcessData()));
            Util.assertSucceeded(
                    UA2ColoHelper.getProcessHelper()
                            .suspend(Util.URLS.SUSPEND_URL, UA2Bundle.getProcessData()));

            Util.shutDownService(UA1ColoHelper.getProcessHelper());


            Util.assertFailed(prismHelper.getProcessHelper()
                    .resume(Util.URLS.RESUME_URL, UA1Bundle.getProcessData()));
            //verify
            checkStatus(UA2ColoHelper, UA2Bundle, Job.Status.SUSPENDED);

            //resume on the other one
            Util.assertSucceeded(
                    prismHelper.getProcessHelper()
                            .resume(Util.URLS.RESUME_URL, UA2Bundle.getProcessData()));
            checkStatus(UA2ColoHelper, UA2Bundle, Job.Status.RUNNING);
            checkStatus(UA1ColoHelper, UA1Bundle, Job.Status.SUSPENDED);
        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getCause());
        } finally {

            Util.restartService(UA1ColoHelper.getFeedHelper());
        }

    }

    @Test(dataProvider = "DP", groups = {"prism", "0.2"})
    public void testResumeDeletedProcessOnBothColosWhen1ColoIsDown(Bundle bundle) throws Exception {
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
            Util.assertFailed(prismHelper.getProcessHelper()
                    .resume(Util.URLS.RESUME_URL, UA1Bundle.getProcessData()));
            //verify
            checkStatus(UA1ColoHelper, UA1Bundle, Job.Status.KILLED);
            checkStatus(UA2ColoHelper, UA2Bundle, Job.Status.RUNNING);

            //suspend using prismHelper
            Util.assertFailed(prismHelper.getProcessHelper()
                    .resume(Util.URLS.RESUME_URL, UA1Bundle.getProcessData()));
            //verify
            checkStatus(UA1ColoHelper, UA1Bundle, Job.Status.KILLED);
            checkStatus(UA2ColoHelper, UA2Bundle, Job.Status.RUNNING);

            Util.assertSucceeded(
                    prismHelper.getProcessHelper()
                            .delete(Util.URLS.DELETE_URL, UA2Bundle.getProcessData()));
            //suspend on the other one
            Util.assertFailed(prismHelper.getProcessHelper()
                    .resume(Util.URLS.RESUME_URL, UA2Bundle.getProcessData()));
            checkStatus(UA1ColoHelper, UA1Bundle, Job.Status.KILLED);
            checkStatus(UA2ColoHelper, UA2Bundle, Job.Status.KILLED);

            Util.assertFailed(
                    UA2ColoHelper.getProcessHelper()
                            .resume(Util.URLS.RESUME_URL, UA2Bundle.getProcessData()));
            checkStatus(UA1ColoHelper, UA1Bundle, Job.Status.KILLED);
            checkStatus(UA2ColoHelper, UA2Bundle, Job.Status.KILLED);
        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getCause());
        } finally {
            Util.restartService(UA1ColoHelper.getFeedHelper());
        }
    }

    @Test(dataProvider = "DP", groups = {"prism", "0.2"})
    public void testResumeResumedProcessOnBothColosWhen1ColoIsDown(Bundle bundle) throws Exception {
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
                    UA1ColoHelper.getProcessHelper()
                            .suspend(Util.URLS.SUSPEND_URL, UA1Bundle.getProcessData()));
            //verify
            checkStatus(UA1ColoHelper, UA1Bundle, Job.Status.SUSPENDED);
            checkStatus(UA2ColoHelper, UA2Bundle, Job.Status.RUNNING);
            Util.assertSucceeded(
                    UA1ColoHelper.getProcessHelper()
                            .resume(Util.URLS.RESUME_URL, UA1Bundle.getProcessData()));
            checkStatus(UA1ColoHelper, UA1Bundle, Job.Status.RUNNING);
            Util.shutDownService(UA1ColoHelper.getProcessHelper());

            Util.assertFailed(prismHelper.getProcessHelper()
                    .resume(Util.URLS.RESUME_URL, UA1Bundle.getProcessData()));


            Util.assertSucceeded(
                    prismHelper.getProcessHelper()
                            .suspend(Util.URLS.SUSPEND_URL, UA2Bundle.getProcessData()));
            checkStatus(UA2ColoHelper, UA2Bundle, Job.Status.SUSPENDED);

            for (int i = 0; i < 2; i++) {
                //suspend on the other one
                Util.assertSucceeded(
                        prismHelper.getProcessHelper()
                                .resume(Util.URLS.RESUME_URL, UA2Bundle.getProcessData()));
                checkStatus(UA1ColoHelper, UA1Bundle, Job.Status.RUNNING);
                checkStatus(UA2ColoHelper, UA2Bundle, Job.Status.RUNNING);
            }

            for (int i = 0; i < 2; i++) {
                //suspend on the other one
                Util.assertSucceeded(
                        UA2ColoHelper.getProcessHelper()
                                .resume(Util.URLS.RESUME_URL, UA2Bundle.getProcessData()));
                checkStatus(UA1ColoHelper, UA1Bundle, Job.Status.RUNNING);
                checkStatus(UA2ColoHelper, UA2Bundle, Job.Status.RUNNING);
            }

        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getCause());
        } finally {
            Util.restartService(UA1ColoHelper.getProcessHelper());
        }
    }

    @Test(dataProvider = "DP", groups = {"prism", "0.2"})
    public void testResumeNonExistentProcessOnBothColosWhen1ColoIsDown(Bundle bundle)
    throws Exception {
        try {
            Bundle UA1Bundle = new Bundle(bundle, UA1ColoHelper.getEnvFileName());
            Bundle UA2Bundle = new Bundle(bundle, UA2ColoHelper.getEnvFileName());

            UA1Bundle.generateUniqueBundle();
            UA2Bundle.generateUniqueBundle();

            Util.shutDownService(UA1ColoHelper.getProcessHelper());

            Util.assertFailed(prismHelper.getProcessHelper()
                    .resume(Util.URLS.RESUME_URL, UA2Bundle.getProcessData()));
            Util.assertFailed(prismHelper.getProcessHelper()
                    .resume(Util.URLS.RESUME_URL, UA1Bundle.getProcessData()));
            Util.assertFailed(
                    UA2ColoHelper.getProcessHelper()
                            .resume(Util.URLS.RESUME_URL, UA2Bundle.getProcessData()));

        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getCause());
        } finally {
            Util.restartService(UA1ColoHelper.getProcessHelper());
        }
    }

    @Test(dataProvider = "DP", groups = {"prism", "0.2"})
    public void testResumeSubmittedProcessOnBothColosWhen1ColoIsDown(Bundle bundle)
    throws Exception {
        try {
            Bundle UA1Bundle = new Bundle(bundle, UA1ColoHelper.getEnvFileName());
            Bundle UA2Bundle = new Bundle(bundle, UA2ColoHelper.getEnvFileName());

            UA1Bundle.generateUniqueBundle();
            UA2Bundle.generateUniqueBundle();

            submitProcess(UA1Bundle);
            submitProcess(UA2Bundle);

            Util.shutDownService(UA1ColoHelper.getProcessHelper());

            Util.assertFailed(prismHelper.getProcessHelper()
                    .resume(Util.URLS.RESUME_URL, UA1Bundle.getProcessData()));
            Util.assertFailed(prismHelper.getProcessHelper()
                    .resume(Util.URLS.RESUME_URL, UA2Bundle.getProcessData()));
            Util.assertFailed(
                    UA2ColoHelper.getProcessHelper()
                            .resume(Util.URLS.RESUME_URL, UA2Bundle.getProcessData()));
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

        Util.assertSucceeded(
                prismHelper.getProcessHelper()
                        .submitEntity(Util.URLS.SUBMIT_URL, bundle.getProcessData()));
    }

    private void submitAndScheduleProcessUsingColoHelper(ColoHelper coloHelper, Bundle bundle)
    throws Exception {
        submitProcess(bundle);
        Util.assertSucceeded(coloHelper.getProcessHelper()
                .schedule(Util.URLS.SCHEDULE_URL, bundle.getProcessData()));
    }

    @DataProvider(name = "DP")
    public Object[][] getData() throws Exception {
        return Util.readBundles("LateDataBundles");
    }

    private void checkStatus(ColoHelper coloHelper, Bundle bundle, Job.Status expectedStatus) throws Exception {
        Assert.assertTrue(Util.verifyOozieJobStatus(coloHelper.getFeedHelper().getOozieClient(),
                Util.readEntityName(bundle.getProcessData()), ENTITY_TYPE.PROCESS, expectedStatus));
    }
}
