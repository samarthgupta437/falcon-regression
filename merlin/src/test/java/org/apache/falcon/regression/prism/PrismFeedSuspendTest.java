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
import org.apache.falcon.regression.core.supportClasses.ENTITY_TYPE;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.testHelper.BaseMultiClusterTests;
import org.apache.oozie.client.Job;
import org.testng.Assert;
import org.testng.TestNGException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;

public class PrismFeedSuspendTest extends BaseMultiClusterTests {

    Bundle bundle1 = new Bundle();
    Bundle bundle2 = new Bundle();

    @BeforeMethod(alwaysRun = true)
    public void setUp(Method method) throws Exception {
        Util.print("test name: " + method.getName());
        Bundle bundle = (Bundle) Util.readELBundles()[0][0];
        bundle1 = new Bundle(bundle, server1.getEnvFileName(), server1.getPrefix());
        bundle2 = new Bundle(bundle, server2.getEnvFileName(), server2.getPrefix());

        bundle1.generateUniqueBundle();
        bundle2.generateUniqueBundle();
    }

    @Test(groups = {"prism", "0.2"})
    public void testSuspendDeletedFeedOnBothColos() throws Exception {
        submitAndScheduleFeed(bundle1);
        submitAndScheduleFeed(bundle2);

        //delete using prism
        Util.assertSucceeded(prism.getFeedHelper()
                .delete(Util.URLS.DELETE_URL, bundle1.getDataSets().get(0)));
        //suspend using prism
        Util.assertFailed(prism.getFeedHelper()
                .suspend(Util.URLS.SUSPEND_URL, bundle1.getDataSets().get(0)));
        //verify
        checkStatus(server1, bundle1, Job.Status.KILLED);
        checkStatus(server2, bundle2, Job.Status.RUNNING);

        Util.assertSucceeded(prism.getFeedHelper()
                .delete(Util.URLS.DELETE_URL, bundle2.getDataSets().get(0)));
        //suspend on the other one
        Util.assertFailed(prism.getFeedHelper()
                .suspend(Util.URLS.SUSPEND_URL, bundle2.getDataSets().get(0)));
        checkStatus(server1, bundle1, Job.Status.KILLED);
        checkStatus(server2, bundle2, Job.Status.KILLED);
    }

    @Test(groups = {"prism", "0.2"})
    public void testSuspendSuspendedFeedOnBothColos() throws Exception {
        //schedule using colohelpers
        submitAndScheduleFeedUsingColoHelper(server1, bundle1);
        submitAndScheduleFeedUsingColoHelper(server2, bundle2);

        for (int i = 0; i < 2; i++) {
            //suspend using prism
            Util.assertSucceeded(
                    prism.getFeedHelper()
                            .suspend(Util.URLS.SUSPEND_URL, bundle1.getDataSets().get(0))
            );
            //verify
            checkStatus(server1, bundle1, Job.Status.SUSPENDED);
            checkStatus(server2, bundle2, Job.Status.RUNNING);
        }

        for (int i = 0; i < 2; i++) {
            //suspend on the other one
            Util.assertSucceeded(
                    prism.getFeedHelper()
                            .suspend(Util.URLS.SUSPEND_URL, bundle2.getDataSets().get(0))
            );
            checkStatus(server1, bundle1, Job.Status.SUSPENDED);
            checkStatus(server2, bundle2, Job.Status.SUSPENDED);
        }
    }


    @Test()
    public void testSuspendNonExistentFeedOnBothColos() throws Exception {
        Util.assertFailed(prism.getFeedHelper()
                .suspend(Util.URLS.SUSPEND_URL, bundle1.getDataSets().get(0)));
        Util.assertFailed(prism.getFeedHelper()
                .suspend(Util.URLS.SUSPEND_URL, bundle2.getDataSets().get(0)));

        Util.assertFailed(server1.getFeedHelper()
                .suspend(Util.URLS.SUSPEND_URL, bundle1.getDataSets().get(0)));
        Util.assertFailed(server2.getFeedHelper()
                .suspend(Util.URLS.SUSPEND_URL, bundle1.getDataSets().get(0)));
    }

    @Test()
    public void testSuspendSubmittedFeedOnBothColos() throws Exception {
        submitFeed(bundle1);
        submitFeed(bundle2);

        Util.assertFailed(prism.getFeedHelper()
                .suspend(Util.URLS.SUSPEND_URL, bundle1.getDataSets().get(0)));
        Util.assertFailed(prism.getFeedHelper()
                .suspend(Util.URLS.SUSPEND_URL, bundle2.getDataSets().get(0)));

        Util.assertFailed(server1.getFeedHelper()
                .suspend(Util.URLS.SUSPEND_URL, bundle1.getDataSets().get(0)));
        Util.assertFailed(server2.getFeedHelper()
                .suspend(Util.URLS.SUSPEND_URL, bundle2.getDataSets().get(0)));


    }

    @Test(groups = {"prism", "0.2"})
    public void testSuspendScheduledFeedOnBothColosWhen1ColoIsDown() throws Exception {
        try {
            //schedule using colohelpers
            submitAndScheduleFeedUsingColoHelper(server1, bundle1);
            submitAndScheduleFeedUsingColoHelper(server2, bundle2);

            Util.shutDownService(server1.getFeedHelper());

            //suspend using prism
            Util.assertFailed(
                    prism.getFeedHelper()
                            .suspend(Util.URLS.SUSPEND_URL, bundle1.getDataSets().get(0))
            );
            //verify
            checkStatus(server2, bundle2, Job.Status.RUNNING);

            //suspend on the other one
            Util.assertSucceeded(
                    prism.getFeedHelper()
                            .suspend(Util.URLS.SUSPEND_URL, bundle2.getDataSets().get(0)));
            checkStatus(server2, bundle2, Job.Status.SUSPENDED);;
        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getCause());
        } finally {
            Util.restartService(server1.getFeedHelper());
        }
    }

    @Test(groups = {"prism", "0.2"})
    public void testSuspendDeletedFeedOnBothColosWhen1ColoIsDown() throws Exception {
        try {
            //schedule using colohelpers
            submitAndScheduleFeedUsingColoHelper(server1, bundle1);
            submitAndScheduleFeedUsingColoHelper(server2, bundle2);

            //delete using coloHelpers
            Util.assertSucceeded(
                    prism.getFeedHelper()
                            .delete(Util.URLS.DELETE_URL, bundle1.getDataSets().get(0))
            );

            Util.shutDownService(server1.getFeedHelper());

            //suspend using prism
            Util.assertFailed(
                    prism.getFeedHelper()
                            .suspend(Util.URLS.SUSPEND_URL, bundle1.getDataSets().get(0)));
            //verify
            checkStatus(server1, bundle1, Job.Status.KILLED);
            checkStatus(server2, bundle2, Job.Status.RUNNING);

            Util.assertSucceeded(
                    prism.getFeedHelper()
                            .delete(Util.URLS.DELETE_URL, bundle2.getDataSets().get(0))
            );
            //suspend on the other one
            Util.assertFailed(
                    prism.getFeedHelper()
                            .suspend(Util.URLS.SUSPEND_URL, bundle2.getDataSets().get(0))
            );
            checkStatus(server1, bundle1, Job.Status.KILLED);
            checkStatus(server2, bundle2, Job.Status.KILLED);
        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getCause());
        } finally {
            Util.restartService(server1.getFeedHelper());
        }
    }

    @Test(groups = {"prism", "0.2"})
    public void testSuspendSuspendedFeedOnBothColosWhen1ColoIsDown() throws Exception {
        try {
            //schedule using colohelpers
            submitAndScheduleFeedUsingColoHelper(server1, bundle1);
            submitAndScheduleFeedUsingColoHelper(server2, bundle2);


            //suspend using prism
            Util.assertSucceeded(
                    prism.getFeedHelper()
                            .suspend(Util.URLS.SUSPEND_URL, bundle1.getDataSets().get(0))
            );
            //verify
            checkStatus(server1, bundle1, Job.Status.SUSPENDED);
            checkStatus(server2, bundle2, Job.Status.RUNNING);

            Util.shutDownService(server1.getFeedHelper());

            Util.assertFailed(
                    prism.getFeedHelper()
                            .suspend(Util.URLS.SUSPEND_URL, bundle1.getDataSets().get(0))
            );

            //suspend on the other one
            Util.assertSucceeded(
                    prism.getFeedHelper()
                            .suspend(Util.URLS.SUSPEND_URL, bundle2.getDataSets().get(0)));
            checkStatus(server1, bundle1, Job.Status.SUSPENDED);
            checkStatus(server2, bundle2, Job.Status.SUSPENDED);

        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getCause());
        } finally {
            Util.restartService(server1.getProcessHelper());
        }
    }

    @Test(groups = {"prism", "0.2"})
    public void testSuspendNonExistentFeedOnBothColosWhen1ColoIsDown()
    throws Exception {
        try {
            Util.shutDownService(server1.getFeedHelper());

            Util.assertFailed(
                    prism.getFeedHelper()
                            .suspend(Util.URLS.SUSPEND_URL, bundle2.getDataSets().get(0))
            );
            Util.assertFailed(
                    prism.getFeedHelper()
                            .suspend(Util.URLS.SUSPEND_URL, bundle1.getDataSets().get(0)));

            Util.assertFailed(
                    server2.getFeedHelper()
                            .suspend(Util.URLS.SUSPEND_URL, bundle1.getDataSets().get(0)));
        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getCause());
        } finally {
            Util.restartService(server1.getProcessHelper());
        }
    }

    @Test(groups = {"prism", "0.2"})
    public void testSuspendSubmittedFeedOnBothColosWhen1ColoIsDown() throws Exception {
        try {
            submitFeed(bundle1);
            submitFeed(bundle2);

            Util.shutDownService(server1.getFeedHelper());

            Util.assertFailed(
                    prism.getFeedHelper()
                            .suspend(Util.URLS.SUSPEND_URL, bundle1.getDataSets().get(0))
            );
            Util.assertFailed(
                    prism.getFeedHelper()
                            .suspend(Util.URLS.SUSPEND_URL, bundle2.getDataSets().get(0)));


            Util.assertFailed(
                    server2.getFeedHelper()
                            .suspend(Util.URLS.SUSPEND_URL, bundle2.getDataSets().get(0)));
        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getCause());
        } finally {
            Util.restartService(server1.getProcessHelper());
        }
    }

    private void checkStatus(ColoHelper coloHelper, Bundle bundle, Job.Status expectedStatus) throws Exception {
        Assert.assertTrue(Util.verifyOozieJobStatus(coloHelper.getFeedHelper().getOozieClient(),
                Util.readDatasetName(bundle.getDataSets().get(0)), ENTITY_TYPE.FEED, expectedStatus));
    }


    private void submitFeed(Bundle bundle) throws Exception {

        for (String cluster : bundle.getClusters()) {
            Util.assertSucceeded(
                    prism.getClusterHelper().submitEntity(Util.URLS.SUBMIT_URL, cluster));
        }
        Util.assertSucceeded(
                prism.getFeedHelper()
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
                    prism.getClusterHelper().submitEntity(Util.URLS.SUBMIT_URL, cluster));
        }
        Util.assertSucceeded(prism.getFeedHelper()
                .submitAndSchedule(Util.URLS.SUBMIT_AND_SCHEDULE_URL, bundle.getDataSets().get(0)));
    }

}
