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
import org.apache.falcon.regression.core.enumsAndConstants.ENTITY_TYPE;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.OozieClient;
import org.testng.TestNGException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;

public class PrismProcessResumeTest extends BaseTestClass {

    ColoHelper cluster1 = servers.get(0);
    ColoHelper cluster2 = servers.get(1);
    OozieClient cluster1OC = serverOC.get(0);
    OozieClient cluster2OC = serverOC.get(1);
    String aggregateWorkflowDir = baseHDFSDir + "/PrismProcessResumeTest/aggregator";

    @BeforeClass
    public void uploadWorkflow() throws Exception {
        uploadDirToClusters(aggregateWorkflowDir, OSUtil.RESOURCES_OOZIE);
    }

    @BeforeMethod(alwaysRun = true)
    public void setUp(Method method) throws Exception {
        Util.print("test name: " + method.getName());
        Bundle bundle = Util.readBundles("LateDataBundles")[0][0];
        for (int i = 0; i < 2; i++) {
            bundles[i] = new Bundle(bundle, servers.get(i));
            bundles[i].generateUniqueBundle();
            bundles[i].setProcessWorkflow(aggregateWorkflowDir);
        }
    }

    @AfterMethod
    public void tearDown() throws Exception {
        removeBundles();
    }

    @Test(groups = {"prism", "0.2", "distributed"})
    public void testResumeSuspendedFeedOnBothColos() throws Exception {
        //schedule using colohelpers
        bundles[0].submitAndScheduleProcessUsingColoHelper(cluster2);
        bundles[1].submitAndScheduleProcessUsingColoHelper(cluster1);

        //suspend using prism
        Util.assertSucceeded(prism.getProcessHelper()
                .suspend(Util.URLS.SUSPEND_URL, bundles[0].getProcessData()));
        //verify
        AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.PROCESS, bundles[0], Job.Status.SUSPENDED);
        AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.PROCESS, bundles[1], Job.Status.RUNNING);

        //suspend using prism
        Util.assertSucceeded(prism.getProcessHelper()
                .resume(Util.URLS.RESUME_URL, bundles[0].getProcessData()));
        //verify
        AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.PROCESS, bundles[0], Job.Status.RUNNING);
        AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.PROCESS, bundles[1], Job.Status.RUNNING);

        //try using the colohelper                
        Util.assertSucceeded(
                cluster2.getProcessHelper()
                        .suspend(Util.URLS.SUSPEND_URL, bundles[0].getProcessData())
        );
        //verify
        AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.PROCESS, bundles[0], Job.Status.SUSPENDED);
        AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.PROCESS, bundles[1], Job.Status.RUNNING);

        //suspend using prism
        Util.assertSucceeded(cluster2.getProcessHelper()
                .resume(Util.URLS.RESUME_URL, bundles[0].getProcessData()));
        //verify
        AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.PROCESS, bundles[0], Job.Status.RUNNING);
        AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.PROCESS, bundles[1], Job.Status.RUNNING);

        //suspend on the other one
        Util.assertSucceeded(
                cluster1.getProcessHelper()
                        .suspend(Util.URLS.SUSPEND_URL, bundles[1].getProcessData())
        );
        AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.PROCESS, bundles[0], Job.Status.RUNNING);
        AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.PROCESS, bundles[1], Job.Status.SUSPENDED);

        Util.assertSucceeded(cluster1.getProcessHelper()
                .resume(Util.URLS.RESUME_URL, bundles[1].getProcessData()));
        AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.PROCESS, bundles[0], Job.Status.RUNNING);
        AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.PROCESS, bundles[1], Job.Status.RUNNING);
    }

    @Test(groups = {"prism", "0.2", "distributed"})
    public void testResumeDeletedProcessOnBothColos() throws Exception {
        //schedule using colohelpers
        bundles[0].submitAndScheduleProcessUsingColoHelper(cluster2);
        bundles[1].submitAndScheduleProcessUsingColoHelper(cluster1);

        //delete using coloHelpers
        Util.assertSucceeded(prism.getProcessHelper()
                .delete(Util.URLS.DELETE_URL, bundles[0].getProcessData()));


        //suspend using prism
        Util.assertFailed(prism.getProcessHelper()
                .resume(Util.URLS.RESUME_URL, bundles[0].getProcessData()));
        //verify
        AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.PROCESS, bundles[0], Job.Status.KILLED);
        AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.PROCESS, bundles[1], Job.Status.RUNNING);

        Util.assertSucceeded(prism.getProcessHelper()
                .delete(Util.URLS.DELETE_URL, bundles[1].getProcessData()));
        //suspend on the other one
        Util.assertFailed(prism.getProcessHelper()
                .resume(Util.URLS.RESUME_URL, bundles[1].getProcessData()));
        AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.PROCESS, bundles[0], Job.Status.KILLED);
        AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.PROCESS, bundles[1], Job.Status.KILLED);

        Util.assertFailed(cluster2.getProcessHelper()
                .resume(Util.URLS.RESUME_URL, bundles[0].getProcessData()));
        AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.PROCESS, bundles[0], Job.Status.KILLED);
        Util.assertFailed(cluster1.getProcessHelper()
                .resume(Util.URLS.RESUME_URL, bundles[1].getProcessData()));
        AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.PROCESS, bundles[1], Job.Status.KILLED);
    }

    @Test(groups = {"prism", "0.2", "distributed"})
    public void testResumeResumedProcessOnBothColos() throws Exception {
        //schedule using colohelpers
        bundles[0].submitAndScheduleProcessUsingColoHelper(cluster2);
        bundles[1].submitAndScheduleProcessUsingColoHelper(cluster1);

        Util.assertSucceeded(prism.getProcessHelper()
                .suspend(Util.URLS.SUSPEND_URL, bundles[0].getProcessData()));
        AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.PROCESS, bundles[0], Job.Status.SUSPENDED);

        for (int i = 0; i < 2; i++) {
            //suspend using prism
            Util.assertSucceeded(
                    prism.getProcessHelper()
                            .resume(Util.URLS.RESUME_URL, bundles[0].getProcessData()));
            //verify
            AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.PROCESS, bundles[0], Job.Status.RUNNING);
            AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.PROCESS, bundles[1], Job.Status.RUNNING);
        }


        Util.assertSucceeded(prism.getProcessHelper()
                .suspend(Util.URLS.SUSPEND_URL, bundles[1].getProcessData()));
        AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.PROCESS, bundles[1], Job.Status.SUSPENDED);

        for (int i = 0; i < 2; i++) {
            Util.assertSucceeded(
                    cluster2.getProcessHelper()
                            .resume(Util.URLS.RESUME_URL, bundles[0].getProcessData())
            );
            //verify
            AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.PROCESS, bundles[0], Job.Status.RUNNING);
            AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.PROCESS, bundles[1], Job.Status.SUSPENDED);
        }


        for (int i = 0; i < 2; i++) {
            //suspend on the other one
            Util.assertSucceeded(
                    prism.getProcessHelper()
                            .resume(Util.URLS.RESUME_URL, bundles[1].getProcessData())
            );
            AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.PROCESS, bundles[0], Job.Status.RUNNING);
            AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.PROCESS, bundles[1], Job.Status.RUNNING);
        }

        for (int i = 0; i < 2; i++) {
            //suspend on the other one
            Util.assertSucceeded(
                    cluster1.getProcessHelper()
                            .resume(Util.URLS.RESUME_URL, bundles[1].getProcessData())
            );
            AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.PROCESS, bundles[0], Job.Status.RUNNING);
            AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.PROCESS, bundles[1], Job.Status.RUNNING);
        }
    }

    @Test(groups = "distributed")
    public void testResumeNonExistentProcessOnBothColos() throws Exception {
        Util.assertFailed(prism.getProcessHelper()
                .resume(Util.URLS.RESUME_URL, bundles[0].getProcessData()));
        Util.assertFailed(prism.getProcessHelper()
                .resume(Util.URLS.RESUME_URL, bundles[1].getProcessData()));

        Util.assertFailed(cluster2.getProcessHelper()
                .resume(Util.URLS.RESUME_URL, bundles[0].getProcessData()));
        Util.assertFailed(cluster1.getProcessHelper()
                .resume(Util.URLS.RESUME_URL, bundles[0].getProcessData()));
    }

    @Test(groups = "distributed")
    public void testResumeSubmittedProcessOnBothColos() throws Exception {
        bundles[0].submitProcess(true);
        bundles[1].submitProcess(true);

        Util.assertFailed(prism.getProcessHelper()
                .resume(Util.URLS.RESUME_URL, bundles[0].getProcessData()));
        Util.assertFailed(prism.getProcessHelper()
                .resume(Util.URLS.RESUME_URL, bundles[1].getProcessData()));

        Util.assertFailed(cluster2.getProcessHelper()
                .resume(Util.URLS.RESUME_URL, bundles[0].getProcessData()));
        Util.assertFailed(cluster1.getProcessHelper()
                .resume(Util.URLS.RESUME_URL, bundles[1].getProcessData()));


    }

    @Test(groups = {"prism", "0.2", "distributed"})
    public void testResumeScheduledProcessOnBothColosWhen1ColoIsDown()
    throws Exception {
        try {
            //schedule using colohelpers
            bundles[0].submitAndScheduleProcessUsingColoHelper(cluster2);
            bundles[1].submitAndScheduleProcessUsingColoHelper(cluster1);
            Util.assertSucceeded(
                    cluster2.getProcessHelper()
                            .suspend(Util.URLS.SUSPEND_URL, bundles[0].getProcessData())
            );
            Util.assertSucceeded(
                    cluster1.getProcessHelper()
                            .suspend(Util.URLS.SUSPEND_URL, bundles[1].getProcessData())
            );

            Util.shutDownService(cluster2.getProcessHelper());


            Util.assertFailed(prism.getProcessHelper()
                    .resume(Util.URLS.RESUME_URL, bundles[0].getProcessData()));
            //verify
            AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.PROCESS, bundles[1], Job.Status.SUSPENDED);

            //resume on the other one
            Util.assertSucceeded(
                    prism.getProcessHelper()
                            .resume(Util.URLS.RESUME_URL, bundles[1].getProcessData()));
            AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.PROCESS, bundles[1], Job.Status.RUNNING);
            AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.PROCESS, bundles[0], Job.Status.SUSPENDED);
        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getCause());
        } finally {

            Util.restartService(cluster2.getFeedHelper());
        }

    }

    @Test(groups = {"prism", "0.2", "distributed"})
    public void testResumeDeletedProcessOnBothColosWhen1ColoIsDown() throws Exception {
        try {
            //schedule using colohelpers
            bundles[0].submitAndScheduleProcessUsingColoHelper(cluster2);
            bundles[1].submitAndScheduleProcessUsingColoHelper(cluster1);

            //delete using coloHelpers
            Util.assertSucceeded(
                    prism.getProcessHelper()
                            .delete(Util.URLS.DELETE_URL, bundles[0].getProcessData())
            );

            Util.shutDownService(cluster2.getProcessHelper());

            //suspend using prism
            Util.assertFailed(prism.getProcessHelper()
                    .resume(Util.URLS.RESUME_URL, bundles[0].getProcessData()));
            //verify
            AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.PROCESS, bundles[0], Job.Status.KILLED);
            AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.PROCESS, bundles[1], Job.Status.RUNNING);

            //suspend using prism
            Util.assertFailed(prism.getProcessHelper()
                    .resume(Util.URLS.RESUME_URL, bundles[0].getProcessData()));
            //verify
            AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.PROCESS, bundles[0], Job.Status.KILLED);
            AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.PROCESS, bundles[1], Job.Status.RUNNING);

            Util.assertSucceeded(
                    prism.getProcessHelper()
                            .delete(Util.URLS.DELETE_URL, bundles[1].getProcessData())
            );
            //suspend on the other one
            Util.assertFailed(prism.getProcessHelper()
                    .resume(Util.URLS.RESUME_URL, bundles[1].getProcessData()));
            AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.PROCESS, bundles[0], Job.Status.KILLED);
            AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.PROCESS, bundles[1], Job.Status.KILLED);

            Util.assertFailed(
                    cluster1.getProcessHelper()
                            .resume(Util.URLS.RESUME_URL, bundles[1].getProcessData())
            );
            AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.PROCESS, bundles[0], Job.Status.KILLED);
            AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.PROCESS, bundles[1], Job.Status.KILLED);
        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getCause());
        } finally {
            Util.restartService(cluster2.getFeedHelper());
        }
    }

    @Test(groups = {"prism", "0.2", "distributed"})
    public void testResumeResumedProcessOnBothColosWhen1ColoIsDown() throws Exception {
        try {
            //schedule using colohelpers
            bundles[0].submitAndScheduleProcessUsingColoHelper(cluster2);
            bundles[1].submitAndScheduleProcessUsingColoHelper(cluster1);

            //suspend using prism
            Util.assertSucceeded(
                    cluster2.getProcessHelper()
                            .suspend(Util.URLS.SUSPEND_URL, bundles[0].getProcessData())
            );
            //verify
            AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.PROCESS, bundles[0], Job.Status.SUSPENDED);
            AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.PROCESS, bundles[1], Job.Status.RUNNING);
            Util.assertSucceeded(
                    cluster2.getProcessHelper()
                            .resume(Util.URLS.RESUME_URL, bundles[0].getProcessData()));
            AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.PROCESS, bundles[0], Job.Status.RUNNING);
            Util.shutDownService(cluster2.getProcessHelper());

            Util.assertFailed(prism.getProcessHelper()
                    .resume(Util.URLS.RESUME_URL, bundles[0].getProcessData()));


            Util.assertSucceeded(
                    prism.getProcessHelper()
                            .suspend(Util.URLS.SUSPEND_URL, bundles[1].getProcessData()));
            AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.PROCESS, bundles[1], Job.Status.SUSPENDED);

            for (int i = 0; i < 2; i++) {
                //suspend on the other one
                Util.assertSucceeded(
                        prism.getProcessHelper()
                                .resume(Util.URLS.RESUME_URL, bundles[1].getProcessData()));
                AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.PROCESS, bundles[0], Job.Status.RUNNING);
                AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.PROCESS, bundles[1], Job.Status.RUNNING);
            }

            for (int i = 0; i < 2; i++) {
                //suspend on the other one
                Util.assertSucceeded(
                        cluster1.getProcessHelper()
                                .resume(Util.URLS.RESUME_URL, bundles[1].getProcessData()));
                AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.PROCESS, bundles[0], Job.Status.RUNNING);
                AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.PROCESS, bundles[1], Job.Status.RUNNING);
            }

        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getCause());
        } finally {
            Util.restartService(cluster2.getProcessHelper());
        }
    }

    @Test(groups = {"prism", "0.2", "distributed"})
    public void testResumeNonExistentProcessOnBothColosWhen1ColoIsDown()
    throws Exception {
        try {
            Util.shutDownService(cluster2.getProcessHelper());

            Util.assertFailed(prism.getProcessHelper()
                    .resume(Util.URLS.RESUME_URL, bundles[1].getProcessData()));
            Util.assertFailed(prism.getProcessHelper()
                    .resume(Util.URLS.RESUME_URL, bundles[0].getProcessData()));
            Util.assertFailed(
                    cluster1.getProcessHelper()
                            .resume(Util.URLS.RESUME_URL, bundles[1].getProcessData()));

        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getCause());
        } finally {
            Util.restartService(cluster2.getProcessHelper());
        }
    }

    @Test(groups = {"prism", "0.2", "distributed"})
    public void testResumeSubmittedProcessOnBothColosWhen1ColoIsDown()
    throws Exception {
        try {
            bundles[0].submitProcess(true);
            bundles[1].submitProcess(true);

            Util.shutDownService(cluster2.getProcessHelper());

            Util.assertFailed(prism.getProcessHelper()
                    .resume(Util.URLS.RESUME_URL, bundles[0].getProcessData()));
            Util.assertFailed(prism.getProcessHelper()
                    .resume(Util.URLS.RESUME_URL, bundles[1].getProcessData()));
            Util.assertFailed(
                    cluster1.getProcessHelper()
                            .resume(Util.URLS.RESUME_URL, bundles[1].getProcessData())
            );
        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getCause());
        } finally {
            Util.restartService(cluster2.getProcessHelper());
        }

    }
}
