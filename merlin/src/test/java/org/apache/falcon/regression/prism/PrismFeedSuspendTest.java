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
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.OozieClient;
import org.testng.TestNGException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;

public class PrismFeedSuspendTest extends BaseTestClass {

    ColoHelper cluster1;
    ColoHelper cluster2;
    OozieClient cluster1OC;
    OozieClient cluster2OC;
    Bundle bundle1 = new Bundle();
    Bundle bundle2 = new Bundle();

    public PrismFeedSuspendTest(){
        super();
        cluster1 = servers.get(0);
        cluster2 = servers.get(1);
        cluster1OC = serverOC.get(0);
        cluster2OC = serverOC.get(1);
    }

    @BeforeMethod(alwaysRun = true)
    public void setUp(Method method) throws Exception {
        Util.print("test name: " + method.getName());
        Bundle bundle = (Bundle) Util.readELBundles()[0][0];
        bundle1 = new Bundle(bundle, cluster1.getEnvFileName(), cluster1.getPrefix());
        bundle2 = new Bundle(bundle, cluster2.getEnvFileName(), cluster2.getPrefix());

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
        AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.FEED, bundle1, Job.Status.KILLED);
        AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.FEED, bundle2, Job.Status.RUNNING);

        Util.assertSucceeded(prism.getFeedHelper()
                .delete(Util.URLS.DELETE_URL, bundle2.getDataSets().get(0)));
        //suspend on the other one
        Util.assertFailed(prism.getFeedHelper()
                .suspend(Util.URLS.SUSPEND_URL, bundle2.getDataSets().get(0)));
        AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.FEED, bundle1, Job.Status.KILLED);
        AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.FEED, bundle2, Job.Status.KILLED);
    }

    @Test(groups = {"prism", "0.2"})
    public void testSuspendSuspendedFeedOnBothColos() throws Exception {
        //schedule using colohelpers
        bundle1.submitAndScheduleFeedUsingColoHelper(cluster1);
        bundle2.submitAndScheduleFeedUsingColoHelper(cluster2);

        for (int i = 0; i < 2; i++) {
            //suspend using prism
            Util.assertSucceeded(
                    prism.getFeedHelper()
                            .suspend(Util.URLS.SUSPEND_URL, bundle1.getDataSets().get(0))
            );
            //verify
            AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.FEED, bundle1, Job.Status.SUSPENDED);
            AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.FEED, bundle2, Job.Status.RUNNING);
        }

        for (int i = 0; i < 2; i++) {
            //suspend on the other one
            Util.assertSucceeded(
                    prism.getFeedHelper()
                            .suspend(Util.URLS.SUSPEND_URL, bundle2.getDataSets().get(0))
            );
            AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.FEED, bundle1, Job.Status.SUSPENDED);
            AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.FEED, bundle2, Job.Status.SUSPENDED);
        }
    }


    @Test()
    public void testSuspendNonExistentFeedOnBothColos() throws Exception {
        Util.assertFailed(prism.getFeedHelper()
                .suspend(Util.URLS.SUSPEND_URL, bundle1.getDataSets().get(0)));
        Util.assertFailed(prism.getFeedHelper()
                .suspend(Util.URLS.SUSPEND_URL, bundle2.getDataSets().get(0)));

        Util.assertFailed(cluster1.getFeedHelper()
                .suspend(Util.URLS.SUSPEND_URL, bundle1.getDataSets().get(0)));
        Util.assertFailed(cluster2.getFeedHelper()
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

        Util.assertFailed(cluster1.getFeedHelper()
                .suspend(Util.URLS.SUSPEND_URL, bundle1.getDataSets().get(0)));
        Util.assertFailed(cluster2.getFeedHelper()
                .suspend(Util.URLS.SUSPEND_URL, bundle2.getDataSets().get(0)));


    }

    @Test(groups = {"prism", "0.2"})
    public void testSuspendScheduledFeedOnBothColosWhen1ColoIsDown() throws Exception {
        try {
            //schedule using colohelpers
            bundle1.submitAndScheduleFeedUsingColoHelper(cluster1);
            bundle2.submitAndScheduleFeedUsingColoHelper(cluster2);

            Util.shutDownService(cluster1.getFeedHelper());

            //suspend using prism
            Util.assertFailed(
                    prism.getFeedHelper()
                            .suspend(Util.URLS.SUSPEND_URL, bundle1.getDataSets().get(0))
            );
            //verify
            AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.FEED, bundle2, Job.Status.RUNNING);

            //suspend on the other one
            Util.assertSucceeded(
                    prism.getFeedHelper()
                            .suspend(Util.URLS.SUSPEND_URL, bundle2.getDataSets().get(0)));
            AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.FEED, bundle2, Job.Status.SUSPENDED);
        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getCause());
        } finally {
            Util.restartService(cluster1.getFeedHelper());
        }
    }

    @Test(groups = {"prism", "0.2"})
    public void testSuspendDeletedFeedOnBothColosWhen1ColoIsDown() throws Exception {
        try {
            //schedule using colohelpers
            bundle1.submitAndScheduleFeedUsingColoHelper(cluster1);
            bundle2.submitAndScheduleFeedUsingColoHelper(cluster2);

            //delete using coloHelpers
            Util.assertSucceeded(
                    prism.getFeedHelper()
                            .delete(Util.URLS.DELETE_URL, bundle1.getDataSets().get(0))
            );

            Util.shutDownService(cluster1.getFeedHelper());

            //suspend using prism
            Util.assertFailed(
                    prism.getFeedHelper()
                            .suspend(Util.URLS.SUSPEND_URL, bundle1.getDataSets().get(0)));
            //verify
            AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.FEED, bundle1, Job.Status.KILLED);
            AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.FEED, bundle2, Job.Status.RUNNING);

            Util.assertSucceeded(
                    prism.getFeedHelper()
                            .delete(Util.URLS.DELETE_URL, bundle2.getDataSets().get(0))
            );
            //suspend on the other one
            Util.assertFailed(
                    prism.getFeedHelper()
                            .suspend(Util.URLS.SUSPEND_URL, bundle2.getDataSets().get(0))
            );
            AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.FEED, bundle1, Job.Status.KILLED);
            AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.FEED, bundle2, Job.Status.KILLED);
        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getCause());
        } finally {
            Util.restartService(cluster1.getFeedHelper());
        }
    }

    @Test(groups = {"prism", "0.2"})
    public void testSuspendSuspendedFeedOnBothColosWhen1ColoIsDown() throws Exception {
        try {
            //schedule using colohelpers
            bundle1.submitAndScheduleFeedUsingColoHelper(cluster1);
            bundle2.submitAndScheduleFeedUsingColoHelper(cluster2);


            //suspend using prism
            Util.assertSucceeded(
                    prism.getFeedHelper()
                            .suspend(Util.URLS.SUSPEND_URL, bundle1.getDataSets().get(0))
            );
            //verify
            AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.FEED, bundle1, Job.Status.SUSPENDED);
            AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.FEED, bundle2, Job.Status.RUNNING);

            Util.shutDownService(cluster1.getFeedHelper());

            Util.assertFailed(
                    prism.getFeedHelper()
                            .suspend(Util.URLS.SUSPEND_URL, bundle1.getDataSets().get(0))
            );

            //suspend on the other one
            Util.assertSucceeded(
                    prism.getFeedHelper()
                            .suspend(Util.URLS.SUSPEND_URL, bundle2.getDataSets().get(0)));
            AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.FEED, bundle1, Job.Status.SUSPENDED);
            AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.FEED, bundle2, Job.Status.SUSPENDED);

        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getCause());
        } finally {
            Util.restartService(cluster1.getProcessHelper());
        }
    }

    @Test(groups = {"prism", "0.2"})
    public void testSuspendNonExistentFeedOnBothColosWhen1ColoIsDown()
    throws Exception {
        try {
            Util.shutDownService(cluster1.getFeedHelper());

            Util.assertFailed(
                    prism.getFeedHelper()
                            .suspend(Util.URLS.SUSPEND_URL, bundle2.getDataSets().get(0))
            );
            Util.assertFailed(
                    prism.getFeedHelper()
                            .suspend(Util.URLS.SUSPEND_URL, bundle1.getDataSets().get(0)));

            Util.assertFailed(
                    cluster2.getFeedHelper()
                            .suspend(Util.URLS.SUSPEND_URL, bundle1.getDataSets().get(0)));
        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getCause());
        } finally {
            Util.restartService(cluster1.getProcessHelper());
        }
    }

    @Test(groups = {"prism", "0.2"})
    public void testSuspendSubmittedFeedOnBothColosWhen1ColoIsDown() throws Exception {
        try {
            bundle1.submitFeed();
            bundle2.submitFeed();

            Util.shutDownService(cluster1.getFeedHelper());

            Util.assertFailed(
                    prism.getFeedHelper()
                            .suspend(Util.URLS.SUSPEND_URL, bundle1.getDataSets().get(0))
            );
            Util.assertFailed(
                    prism.getFeedHelper()
                            .suspend(Util.URLS.SUSPEND_URL, bundle2.getDataSets().get(0)));


            Util.assertFailed(
                    cluster2.getFeedHelper()
                            .suspend(Util.URLS.SUSPEND_URL, bundle2.getDataSets().get(0)));
        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getCause());
        } finally {
            Util.restartService(cluster1.getProcessHelper());
        }
    }

}
