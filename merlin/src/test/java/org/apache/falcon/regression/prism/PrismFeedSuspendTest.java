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
import org.apache.falcon.regression.core.supportClasses.ENTITY_TYPE;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.testHelper.BaseMultiClusterTests;
import org.apache.oozie.client.Job;
import org.testng.TestNGException;
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
        bundle1.submitAndScheduleFeed();
        bundle2.submitAndScheduleFeed();

        //delete using prism
        Util.assertSucceeded(prism.getFeedHelper()
                .delete(Util.URLS.DELETE_URL, bundle1.getDataSets().get(0)));
        //suspend using prism
        Util.assertFailed(prism.getFeedHelper()
                .suspend(Util.URLS.SUSPEND_URL, bundle1.getDataSets().get(0)));
        //verify
        AssertUtil.checkStatus(server1OC, ENTITY_TYPE.FEED, bundle1, Job.Status.KILLED);
        AssertUtil.checkStatus(server2OC, ENTITY_TYPE.FEED, bundle2, Job.Status.RUNNING);

        Util.assertSucceeded(prism.getFeedHelper()
                .delete(Util.URLS.DELETE_URL, bundle2.getDataSets().get(0)));
        //suspend on the other one
        Util.assertFailed(prism.getFeedHelper()
                .suspend(Util.URLS.SUSPEND_URL, bundle2.getDataSets().get(0)));
        AssertUtil.checkStatus(server1OC, ENTITY_TYPE.FEED, bundle1, Job.Status.KILLED);
        AssertUtil.checkStatus(server2OC, ENTITY_TYPE.FEED, bundle2, Job.Status.KILLED);
    }

    @Test(groups = {"prism", "0.2"})
    public void testSuspendSuspendedFeedOnBothColos() throws Exception {
        //schedule using colohelpers
        bundle1.submitAndScheduleFeedUsingColoHelper(server1);
        bundle2.submitAndScheduleFeedUsingColoHelper(server2);

        for (int i = 0; i < 2; i++) {
            //suspend using prism
            Util.assertSucceeded(
                    prism.getFeedHelper()
                            .suspend(Util.URLS.SUSPEND_URL, bundle1.getDataSets().get(0))
            );
            //verify
            AssertUtil.checkStatus(server1OC, ENTITY_TYPE.FEED, bundle1, Job.Status.SUSPENDED);
            AssertUtil.checkStatus(server2OC, ENTITY_TYPE.FEED, bundle2, Job.Status.RUNNING);
        }

        for (int i = 0; i < 2; i++) {
            //suspend on the other one
            Util.assertSucceeded(
                    prism.getFeedHelper()
                            .suspend(Util.URLS.SUSPEND_URL, bundle2.getDataSets().get(0))
            );
            AssertUtil.checkStatus(server1OC, ENTITY_TYPE.FEED, bundle1, Job.Status.SUSPENDED);
            AssertUtil.checkStatus(server2OC, ENTITY_TYPE.FEED, bundle2, Job.Status.SUSPENDED);
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
        bundle1.submitFeed();
        bundle2.submitFeed();

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
            bundle1.submitAndScheduleFeedUsingColoHelper(server1);
            bundle2.submitAndScheduleFeedUsingColoHelper(server2);

            Util.shutDownService(server1.getFeedHelper());

            //suspend using prism
            Util.assertFailed(
                    prism.getFeedHelper()
                            .suspend(Util.URLS.SUSPEND_URL, bundle1.getDataSets().get(0))
            );
            //verify
            AssertUtil.checkStatus(server2OC, ENTITY_TYPE.FEED, bundle2, Job.Status.RUNNING);

            //suspend on the other one
            Util.assertSucceeded(
                    prism.getFeedHelper()
                            .suspend(Util.URLS.SUSPEND_URL, bundle2.getDataSets().get(0)));
            AssertUtil.checkStatus(server2OC, ENTITY_TYPE.FEED, bundle2, Job.Status.SUSPENDED);
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
            bundle1.submitAndScheduleFeedUsingColoHelper(server1);
            bundle2.submitAndScheduleFeedUsingColoHelper(server2);

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
            AssertUtil.checkStatus(server1OC, ENTITY_TYPE.FEED, bundle1, Job.Status.KILLED);
            AssertUtil.checkStatus(server2OC, ENTITY_TYPE.FEED, bundle2, Job.Status.RUNNING);

            Util.assertSucceeded(
                    prism.getFeedHelper()
                            .delete(Util.URLS.DELETE_URL, bundle2.getDataSets().get(0))
            );
            //suspend on the other one
            Util.assertFailed(
                    prism.getFeedHelper()
                            .suspend(Util.URLS.SUSPEND_URL, bundle2.getDataSets().get(0))
            );
            AssertUtil.checkStatus(server1OC, ENTITY_TYPE.FEED, bundle1, Job.Status.KILLED);
            AssertUtil.checkStatus(server2OC, ENTITY_TYPE.FEED, bundle2, Job.Status.KILLED);
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
            bundle1.submitAndScheduleFeedUsingColoHelper(server1);
            bundle2.submitAndScheduleFeedUsingColoHelper(server2);


            //suspend using prism
            Util.assertSucceeded(
                    prism.getFeedHelper()
                            .suspend(Util.URLS.SUSPEND_URL, bundle1.getDataSets().get(0))
            );
            //verify
            AssertUtil.checkStatus(server1OC, ENTITY_TYPE.FEED, bundle1, Job.Status.SUSPENDED);
            AssertUtil.checkStatus(server2OC, ENTITY_TYPE.FEED, bundle2, Job.Status.RUNNING);

            Util.shutDownService(server1.getFeedHelper());

            Util.assertFailed(
                    prism.getFeedHelper()
                            .suspend(Util.URLS.SUSPEND_URL, bundle1.getDataSets().get(0))
            );

            //suspend on the other one
            Util.assertSucceeded(
                    prism.getFeedHelper()
                            .suspend(Util.URLS.SUSPEND_URL, bundle2.getDataSets().get(0)));
            AssertUtil.checkStatus(server1OC, ENTITY_TYPE.FEED, bundle1, Job.Status.SUSPENDED);
            AssertUtil.checkStatus(server2OC, ENTITY_TYPE.FEED, bundle2, Job.Status.SUSPENDED);

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
            bundle1.submitFeed();
            bundle2.submitFeed();

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

}
