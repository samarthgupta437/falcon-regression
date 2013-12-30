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
import org.apache.oozie.client.Job;
import org.testng.Assert;
import org.testng.TestNGException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.lang.reflect.Method;

public class PrismFeedSuspendTest {


    @BeforeMethod(alwaysRun = true)
    public void testName(Method method) {
        Util.print("test name: " + method.getName());
    }


    PrismHelper prismHelper = new PrismHelper("prism.properties");
    ColoHelper UA1ColoHelper = new ColoHelper("mk-qa.config.properties");
    ColoHelper UA2ColoHelper = new ColoHelper("ivoryqa-1.config.properties");


    @Test(dataProvider = "DP", groups = {"prism", "0.2"})
    public void testSuspendDeletedFeedOnBothColos(Bundle bundle) throws Exception {
        Bundle UA1Bundle = new Bundle(bundle, UA1ColoHelper.getEnvFileName());
        Bundle UA2Bundle = new Bundle(bundle, UA2ColoHelper.getEnvFileName());

        UA1Bundle.generateUniqueBundle();
        UA2Bundle.generateUniqueBundle();

        submitAndScheduleFeed(UA1Bundle);
        submitAndScheduleFeed(UA2Bundle);

        //delete using prismHelper
        Util.assertSucceeded(prismHelper.getFeedHelper()
                .delete(Util.URLS.DELETE_URL, UA1Bundle.getDataSets().get(0)));


        //suspend using prismHelper
        Util.assertFailed(prismHelper.getFeedHelper()
                .suspend(Util.URLS.SUSPEND_URL, UA1Bundle.getDataSets().get(0)));
        //verify
        checkStatus(UA1ColoHelper, UA1Bundle, Job.Status.KILLED);
        checkStatus(UA2ColoHelper, UA2Bundle, Job.Status.RUNNING);

        Util.assertSucceeded(prismHelper.getFeedHelper()
                .delete(Util.URLS.DELETE_URL, UA2Bundle.getDataSets().get(0)));
        //suspend on the other one
        Util.assertFailed(prismHelper.getFeedHelper()
                .suspend(Util.URLS.SUSPEND_URL, UA2Bundle.getDataSets().get(0)));
        checkStatus(UA1ColoHelper, UA1Bundle, Job.Status.KILLED);
        checkStatus(UA2ColoHelper, UA2Bundle, Job.Status.KILLED);
    }

    @Test(dataProvider = "DP", groups = {"prism", "0.2"})
    public void testSuspendSuspendedFeedOnBothColos(Bundle bundle) throws Exception {
        Bundle UA1Bundle = new Bundle(bundle, UA1ColoHelper.getEnvFileName());
        Bundle UA2Bundle = new Bundle(bundle, UA2ColoHelper.getEnvFileName());

        UA1Bundle.generateUniqueBundle();
        UA2Bundle.generateUniqueBundle();

        //schedule using colohelpers
        submitAndScheduleFeedUsingColoHelper(UA1ColoHelper, UA1Bundle);
        submitAndScheduleFeedUsingColoHelper(UA2ColoHelper, UA2Bundle);


        for (int i = 0; i < 2; i++) {
            //suspend using prismHelper
            Util.assertSucceeded(
                    prismHelper.getFeedHelper()
                            .suspend(Util.URLS.SUSPEND_URL, UA1Bundle.getDataSets().get(0)));
            //verify
            checkStatus(UA1ColoHelper, UA1Bundle, Job.Status.SUSPENDED);
            checkStatus(UA2ColoHelper, UA2Bundle, Job.Status.RUNNING);
        }


        for (int i = 0; i < 2; i++) {
            //suspend on the other one
            Util.assertSucceeded(
                    prismHelper.getFeedHelper()
                            .suspend(Util.URLS.SUSPEND_URL, UA2Bundle.getDataSets().get(0)));
            checkStatus(UA1ColoHelper, UA1Bundle, Job.Status.SUSPENDED);
            checkStatus(UA2ColoHelper, UA2Bundle, Job.Status.SUSPENDED);
        }
    }


    @Test(dataProvider = "DP")
    public void testSuspendNonExistentFeedOnBothColos(Bundle bundle) throws Exception {
        Bundle UA1Bundle = new Bundle(bundle, UA1ColoHelper.getEnvFileName());
        Bundle UA2Bundle = new Bundle(bundle, UA2ColoHelper.getEnvFileName());

        UA1Bundle.generateUniqueBundle();
        UA2Bundle.generateUniqueBundle();

        Util.assertFailed(prismHelper.getFeedHelper()
                .suspend(Util.URLS.SUSPEND_URL, UA1Bundle.getDataSets().get(0)));
        Util.assertFailed(prismHelper.getFeedHelper()
                .suspend(Util.URLS.SUSPEND_URL, UA2Bundle.getDataSets().get(0)));

        Util.assertFailed(UA1ColoHelper.getFeedHelper()
                .suspend(Util.URLS.SUSPEND_URL, UA1Bundle.getDataSets().get(0)));
        Util.assertFailed(UA2ColoHelper.getFeedHelper()
                .suspend(Util.URLS.SUSPEND_URL, UA1Bundle.getDataSets().get(0)));
    }

    @Test(dataProvider = "DP")
    public void testSuspendSubmittedFeedOnBothColos(Bundle bundle) throws Exception {
        Bundle UA1Bundle = new Bundle(bundle, UA1ColoHelper.getEnvFileName());
        Bundle UA2Bundle = new Bundle(bundle, UA2ColoHelper.getEnvFileName());

        UA1Bundle.generateUniqueBundle();
        UA2Bundle.generateUniqueBundle();

        submitFeed(UA1Bundle);
        submitFeed(UA2Bundle);

        Util.assertFailed(prismHelper.getFeedHelper()
                .suspend(Util.URLS.SUSPEND_URL, UA1Bundle.getDataSets().get(0)));
        Util.assertFailed(prismHelper.getFeedHelper()
                .suspend(Util.URLS.SUSPEND_URL, UA2Bundle.getDataSets().get(0)));

        Util.assertFailed(UA1ColoHelper.getFeedHelper()
                .suspend(Util.URLS.SUSPEND_URL, UA1Bundle.getDataSets().get(0)));
        Util.assertFailed(UA2ColoHelper.getFeedHelper()
                .suspend(Util.URLS.SUSPEND_URL, UA2Bundle.getDataSets().get(0)));


    }

    @Test(dataProvider = "DP", groups = {"prism", "0.2"})
    public void testSuspendScheduledFeedOnBothColosWhen1ColoIsDown(Bundle bundle) throws Exception {
        try {
            Bundle UA1Bundle = new Bundle(bundle, UA1ColoHelper.getEnvFileName());
            Bundle UA2Bundle = new Bundle(bundle, UA2ColoHelper.getEnvFileName());

            UA1Bundle.generateUniqueBundle();
            UA2Bundle.generateUniqueBundle();

            //schedule using colohelpers
            submitAndScheduleFeedUsingColoHelper(UA1ColoHelper, UA1Bundle);
            submitAndScheduleFeedUsingColoHelper(UA2ColoHelper, UA2Bundle);


            Util.shutDownService(UA1ColoHelper.getFeedHelper());

            //suspend using prismHelper
            Util.assertFailed(
                    prismHelper.getFeedHelper()
                            .suspend(Util.URLS.SUSPEND_URL, UA1Bundle.getDataSets().get(0)));
            //verify
            checkStatus(UA2ColoHelper, UA2Bundle, Job.Status.RUNNING);

            //suspend on the other one
            Util.assertSucceeded(
                    prismHelper.getFeedHelper()
                            .suspend(Util.URLS.SUSPEND_URL, UA2Bundle.getDataSets().get(0)));
            checkStatus(UA2ColoHelper, UA2Bundle, Job.Status.SUSPENDED);;
        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getCause());
        } finally {

            Util.restartService(UA1ColoHelper.getFeedHelper());
        }

    }

    @Test(dataProvider = "DP", groups = {"prism", "0.2"})
    public void testSuspendDeletedFeedOnBothColosWhen1ColoIsDown(Bundle bundle) throws Exception {
        try {
            Bundle UA1Bundle = new Bundle(bundle, UA1ColoHelper.getEnvFileName());
            Bundle UA2Bundle = new Bundle(bundle, UA2ColoHelper.getEnvFileName());

            UA1Bundle.generateUniqueBundle();
            UA2Bundle.generateUniqueBundle();

            //schedule using colohelpers
            submitAndScheduleFeedUsingColoHelper(UA1ColoHelper, UA1Bundle);
            submitAndScheduleFeedUsingColoHelper(UA2ColoHelper, UA2Bundle);

            //delete using coloHelpers
            Util.assertSucceeded(
                    prismHelper.getFeedHelper()
                            .delete(Util.URLS.DELETE_URL, UA1Bundle.getDataSets().get(0)));

            Util.shutDownService(UA1ColoHelper.getFeedHelper());

            //suspend using prismHelper
            Util.assertFailed(
                    prismHelper.getFeedHelper()
                            .suspend(Util.URLS.SUSPEND_URL, UA1Bundle.getDataSets().get(0)));
            //verify
            checkStatus(UA1ColoHelper, UA1Bundle, Job.Status.KILLED);
            checkStatus(UA2ColoHelper, UA2Bundle, Job.Status.RUNNING);


            Util.assertSucceeded(
                    prismHelper.getFeedHelper()
                            .delete(Util.URLS.DELETE_URL, UA2Bundle.getDataSets().get(0)));
            //suspend on the other one
            Util.assertFailed(
                    prismHelper.getFeedHelper()
                            .suspend(Util.URLS.SUSPEND_URL, UA2Bundle.getDataSets().get(0)));
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
    public void testSuspendSuspendedFeedOnBothColosWhen1ColoIsDown(Bundle bundle) throws Exception {
        try {
            Bundle UA1Bundle = new Bundle(bundle, UA1ColoHelper.getEnvFileName());
            Bundle UA2Bundle = new Bundle(bundle, UA2ColoHelper.getEnvFileName());

            UA1Bundle.generateUniqueBundle();
            UA2Bundle.generateUniqueBundle();

            //schedule using colohelpers
            submitAndScheduleFeedUsingColoHelper(UA1ColoHelper, UA1Bundle);
            submitAndScheduleFeedUsingColoHelper(UA2ColoHelper, UA2Bundle);


            //suspend using prismHelper
            Util.assertSucceeded(
                    prismHelper.getFeedHelper()
                            .suspend(Util.URLS.SUSPEND_URL, UA1Bundle.getDataSets().get(0)));
            //verify
            checkStatus(UA1ColoHelper, UA1Bundle, Job.Status.SUSPENDED);
            checkStatus(UA2ColoHelper, UA2Bundle, Job.Status.RUNNING);

            Util.shutDownService(UA1ColoHelper.getFeedHelper());

            Util.assertFailed(
                    prismHelper.getFeedHelper()
                            .suspend(Util.URLS.SUSPEND_URL, UA1Bundle.getDataSets().get(0)));

            //suspend on the other one
            Util.assertSucceeded(
                    prismHelper.getFeedHelper()
                            .suspend(Util.URLS.SUSPEND_URL, UA2Bundle.getDataSets().get(0)));
            checkStatus(UA1ColoHelper, UA1Bundle, Job.Status.SUSPENDED);
            checkStatus(UA2ColoHelper, UA2Bundle, Job.Status.SUSPENDED);

        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getCause());
        } finally {
            Util.restartService(UA1ColoHelper.getProcessHelper());
        }
    }

    @Test(dataProvider = "DP", groups = {"prism", "0.2"})
    public void testSuspendNonExistentFeedOnBothColosWhen1ColoIsDown(Bundle bundle)
    throws Exception {
        try {
            Bundle UA1Bundle = new Bundle(bundle, UA1ColoHelper.getEnvFileName());
            Bundle UA2Bundle = new Bundle(bundle, UA2ColoHelper.getEnvFileName());

            UA1Bundle.generateUniqueBundle();
            UA2Bundle.generateUniqueBundle();

            Util.shutDownService(UA1ColoHelper.getFeedHelper());

            Util.assertFailed(
                    prismHelper.getFeedHelper()
                            .suspend(Util.URLS.SUSPEND_URL, UA2Bundle.getDataSets().get(0)));
            Util.assertFailed(
                    prismHelper.getFeedHelper()
                            .suspend(Util.URLS.SUSPEND_URL, UA1Bundle.getDataSets().get(0)));

            Util.assertFailed(
                    UA2ColoHelper.getFeedHelper()
                            .suspend(Util.URLS.SUSPEND_URL, UA1Bundle.getDataSets().get(0)));
        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getCause());
        } finally {
            Util.restartService(UA1ColoHelper.getProcessHelper());
        }
    }

    @Test(dataProvider = "DP", groups = {"prism", "0.2"})
    public void testSuspendSubmittedFeedOnBothColosWhen1ColoIsDown(Bundle bundle) throws Exception {
        try {
            Bundle UA1Bundle = new Bundle(bundle, UA1ColoHelper.getEnvFileName());
            Bundle UA2Bundle = new Bundle(bundle, UA2ColoHelper.getEnvFileName());

            UA1Bundle.generateUniqueBundle();
            UA2Bundle.generateUniqueBundle();

            submitFeed(UA1Bundle);
            submitFeed(UA2Bundle);

            Util.shutDownService(UA1ColoHelper.getFeedHelper());

            Util.assertFailed(
                    prismHelper.getFeedHelper()
                            .suspend(Util.URLS.SUSPEND_URL, UA1Bundle.getDataSets().get(0)));
            Util.assertFailed(
                    prismHelper.getFeedHelper()
                            .suspend(Util.URLS.SUSPEND_URL, UA2Bundle.getDataSets().get(0)));


            Util.assertFailed(
                    UA2ColoHelper.getFeedHelper()
                            .suspend(Util.URLS.SUSPEND_URL, UA2Bundle.getDataSets().get(0)));
        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getCause());
        } finally {
            Util.restartService(UA1ColoHelper.getProcessHelper());
        }

    }

    private void checkStatus(ColoHelper coloHelper, Bundle bundle, Job.Status expectedStatus) throws Exception {
        Assert.assertTrue(Util.verifyOozieJobStatus(coloHelper.getFeedHelper().getOozieClient(),
                Util.readDatasetName(bundle.getDataSets().get(0)), ENTITY_TYPE.FEED, expectedStatus));
    }


    private void submitFeed(Bundle bundle) throws Exception {

        for (String cluster : bundle.getClusters()) {
            Util.assertSucceeded(
                    prismHelper.getClusterHelper().submitEntity(Util.URLS.SUBMIT_URL, cluster));
        }
        Util.assertSucceeded(
                prismHelper.getFeedHelper()
                        .submitEntity(Util.URLS.SUBMIT_URL, bundle.getDataSets().get(0)));
    }


    private void submitAndScheduleFeedUsingColoHelper(ColoHelper coloHelper, Bundle bundle)
    throws Exception {
        submitFeed(bundle);
        Util.assertSucceeded(coloHelper.getFeedHelper()
                .schedule(Util.URLS.SCHEDULE_URL, bundle.getDataSets().get(0)));
    }

    private void submitAndScheduleFeed(Bundle bundle) throws Exception {
        for (String cluster : bundle.getClusters()) {
            Util.assertSucceeded(
                    prismHelper.getClusterHelper().submitEntity(Util.URLS.SUBMIT_URL, cluster));
        }
        Util.assertSucceeded(prismHelper.getFeedHelper()
                .submitAndSchedule(Util.URLS.SUBMIT_AND_SCHEDULE_URL, bundle.getDataSets().get(0)));
    }

    @DataProvider(name = "DP")
    public Object[][] getData() throws Exception {
        return Util.readELBundles();
    }
}
