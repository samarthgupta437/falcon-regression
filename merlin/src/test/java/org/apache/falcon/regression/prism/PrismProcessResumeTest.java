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
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;

public class PrismProcessResumeTest extends BaseTestClass {

    ColoHelper cluster1;
    ColoHelper cluster2;
    OozieClient cluster1OC;
    OozieClient cluster2OC;
    Bundle UA1Bundle = new Bundle();
    Bundle UA2Bundle = new Bundle();

    public PrismProcessResumeTest(){
        super();
        cluster1 = servers.get(0);
        cluster2 = servers.get(1);
        cluster1OC = serverOC.get(0);
        cluster2OC = serverOC.get(1);
    }

    @BeforeMethod(alwaysRun = true)
    public void setUp(Method method) throws Exception {
        Util.print("test name: " + method.getName());
        Bundle bundle = Util.readBundles("LateDataBundles")[0][0];
        UA1Bundle = new Bundle(bundle, cluster2.getEnvFileName(), cluster2.getPrefix());
        UA2Bundle = new Bundle(bundle, cluster1.getEnvFileName(), cluster1.getPrefix());

        UA1Bundle.generateUniqueBundle();
        UA2Bundle.generateUniqueBundle();

    }

    @AfterMethod
    public void tearDown() throws Exception {
        removeBundles(UA1Bundle, UA2Bundle);
    }

    @Test(groups = {"prism", "0.2"})
    public void testResumeSuspendedFeedOnBothColos() throws Exception {
        //schedule using colohelpers
        UA1Bundle.submitAndScheduleProcessUsingColoHelper(cluster2);
        UA2Bundle.submitAndScheduleProcessUsingColoHelper(cluster1);

        //suspend using prism
        Util.assertSucceeded(prism.getProcessHelper()
                .suspend(Util.URLS.SUSPEND_URL, UA1Bundle.getProcessData()));
        //verify
        AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.PROCESS, UA1Bundle, Job.Status.SUSPENDED);
        AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.PROCESS, UA2Bundle, Job.Status.RUNNING);

        //suspend using prism
        Util.assertSucceeded(prism.getProcessHelper()
                .resume(Util.URLS.RESUME_URL, UA1Bundle.getProcessData()));
        //verify
        AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.PROCESS, UA1Bundle, Job.Status.RUNNING);
        AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.PROCESS, UA2Bundle, Job.Status.RUNNING);

        //try using the colohelper                
        Util.assertSucceeded(
                cluster2.getProcessHelper()
                        .suspend(Util.URLS.SUSPEND_URL, UA1Bundle.getProcessData())
        );
        //verify
        AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.PROCESS, UA1Bundle, Job.Status.SUSPENDED);
        AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.PROCESS, UA2Bundle, Job.Status.RUNNING);

        //suspend using prism
        Util.assertSucceeded(cluster2.getProcessHelper()
                .resume(Util.URLS.RESUME_URL, UA1Bundle.getProcessData()));
        //verify
        AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.PROCESS, UA1Bundle, Job.Status.RUNNING);
        AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.PROCESS, UA2Bundle, Job.Status.RUNNING);

        //suspend on the other one
        Util.assertSucceeded(
                cluster1.getProcessHelper()
                        .suspend(Util.URLS.SUSPEND_URL, UA2Bundle.getProcessData())
        );
        AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.PROCESS, UA1Bundle, Job.Status.RUNNING);
        AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.PROCESS, UA2Bundle, Job.Status.SUSPENDED);

        Util.assertSucceeded(cluster1.getProcessHelper()
                .resume(Util.URLS.RESUME_URL, UA2Bundle.getProcessData()));
        AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.PROCESS, UA1Bundle, Job.Status.RUNNING);
        AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.PROCESS, UA2Bundle, Job.Status.RUNNING);
    }

    @Test(groups = {"prism", "0.2"})
    public void testResumeDeletedProcessOnBothColos() throws Exception {
        //schedule using colohelpers
        UA1Bundle.submitAndScheduleProcessUsingColoHelper(cluster2);
        UA2Bundle.submitAndScheduleProcessUsingColoHelper(cluster1);

        //delete using coloHelpers
        Util.assertSucceeded(prism.getProcessHelper()
                .delete(Util.URLS.DELETE_URL, UA1Bundle.getProcessData()));


        //suspend using prism
        Util.assertFailed(prism.getProcessHelper()
                .resume(Util.URLS.RESUME_URL, UA1Bundle.getProcessData()));
        //verify
        AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.PROCESS, UA1Bundle, Job.Status.KILLED);
        AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.PROCESS, UA2Bundle, Job.Status.RUNNING);

        Util.assertSucceeded(prism.getProcessHelper()
                .delete(Util.URLS.DELETE_URL, UA2Bundle.getProcessData()));
        //suspend on the other one
        Util.assertFailed(prism.getProcessHelper()
                .resume(Util.URLS.RESUME_URL, UA2Bundle.getProcessData()));
        AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.PROCESS, UA1Bundle, Job.Status.KILLED);
        AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.PROCESS, UA2Bundle, Job.Status.KILLED);

        Util.assertFailed(cluster2.getProcessHelper()
                .resume(Util.URLS.RESUME_URL, UA1Bundle.getProcessData()));
        AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.PROCESS, UA1Bundle, Job.Status.KILLED);
        Util.assertFailed(cluster1.getProcessHelper()
                .resume(Util.URLS.RESUME_URL, UA2Bundle.getProcessData()));
        AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.PROCESS, UA2Bundle, Job.Status.KILLED);
    }

    @Test(groups = {"prism", "0.2"})
    public void testResumeResumedProcessOnBothColos() throws Exception {
        //schedule using colohelpers
        UA1Bundle.submitAndScheduleProcessUsingColoHelper(cluster2);
        UA2Bundle.submitAndScheduleProcessUsingColoHelper(cluster1);

        Util.assertSucceeded(prism.getProcessHelper()
                .suspend(Util.URLS.SUSPEND_URL, UA1Bundle.getProcessData()));
        AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.PROCESS, UA1Bundle, Job.Status.SUSPENDED);

        for (int i = 0; i < 2; i++) {
            //suspend using prism
            Util.assertSucceeded(
                    prism.getProcessHelper()
                            .resume(Util.URLS.RESUME_URL, UA1Bundle.getProcessData()));
            //verify
            AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.PROCESS, UA1Bundle, Job.Status.RUNNING);
            AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.PROCESS, UA2Bundle, Job.Status.RUNNING);
        }


        Util.assertSucceeded(prism.getProcessHelper()
                .suspend(Util.URLS.SUSPEND_URL, UA2Bundle.getProcessData()));
        AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.PROCESS, UA2Bundle, Job.Status.SUSPENDED);

        for (int i = 0; i < 2; i++) {
            Util.assertSucceeded(
                    cluster2.getProcessHelper()
                            .resume(Util.URLS.RESUME_URL, UA1Bundle.getProcessData())
            );
            //verify
            AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.PROCESS, UA1Bundle, Job.Status.RUNNING);
            AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.PROCESS, UA2Bundle, Job.Status.SUSPENDED);
        }


        for (int i = 0; i < 2; i++) {
            //suspend on the other one
            Util.assertSucceeded(
                    prism.getProcessHelper()
                            .resume(Util.URLS.RESUME_URL, UA2Bundle.getProcessData())
            );
            AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.PROCESS, UA1Bundle, Job.Status.RUNNING);
            AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.PROCESS, UA2Bundle, Job.Status.RUNNING);
        }

        for (int i = 0; i < 2; i++) {
            //suspend on the other one
            Util.assertSucceeded(
                    cluster1.getProcessHelper()
                            .resume(Util.URLS.RESUME_URL, UA2Bundle.getProcessData())
            );
            AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.PROCESS, UA1Bundle, Job.Status.RUNNING);
            AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.PROCESS, UA2Bundle, Job.Status.RUNNING);
        }
    }

    @Test()
    public void testResumeNonExistentProcessOnBothColos() throws Exception {
        Util.assertFailed(prism.getProcessHelper()
                .resume(Util.URLS.RESUME_URL, UA1Bundle.getProcessData()));
        Util.assertFailed(prism.getProcessHelper()
                .resume(Util.URLS.RESUME_URL, UA2Bundle.getProcessData()));

        Util.assertFailed(cluster2.getProcessHelper()
                .resume(Util.URLS.RESUME_URL, UA1Bundle.getProcessData()));
        Util.assertFailed(cluster1.getProcessHelper()
                .resume(Util.URLS.RESUME_URL, UA1Bundle.getProcessData()));
    }

    @Test()
    public void testResumeSubmittedProcessOnBothColos() throws Exception {
        UA1Bundle.submitProcess();
        UA2Bundle.submitProcess();

        Util.assertFailed(prism.getProcessHelper()
                .resume(Util.URLS.RESUME_URL, UA1Bundle.getProcessData()));
        Util.assertFailed(prism.getProcessHelper()
                .resume(Util.URLS.RESUME_URL, UA2Bundle.getProcessData()));

        Util.assertFailed(cluster2.getProcessHelper()
                .resume(Util.URLS.RESUME_URL, UA1Bundle.getProcessData()));
        Util.assertFailed(cluster1.getProcessHelper()
                .resume(Util.URLS.RESUME_URL, UA2Bundle.getProcessData()));


    }

    @Test(groups = {"prism", "0.2"})
    public void testResumeScheduledProcessOnBothColosWhen1ColoIsDown()
    throws Exception {
        try {
            //schedule using colohelpers
            UA1Bundle.submitAndScheduleProcessUsingColoHelper(cluster2);
            UA2Bundle.submitAndScheduleProcessUsingColoHelper(cluster1);
            Util.assertSucceeded(
                    cluster2.getProcessHelper()
                            .suspend(Util.URLS.SUSPEND_URL, UA1Bundle.getProcessData())
            );
            Util.assertSucceeded(
                    cluster1.getProcessHelper()
                            .suspend(Util.URLS.SUSPEND_URL, UA2Bundle.getProcessData())
            );

            Util.shutDownService(cluster2.getProcessHelper());


            Util.assertFailed(prism.getProcessHelper()
                    .resume(Util.URLS.RESUME_URL, UA1Bundle.getProcessData()));
            //verify
            AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.PROCESS, UA2Bundle, Job.Status.SUSPENDED);

            //resume on the other one
            Util.assertSucceeded(
                    prism.getProcessHelper()
                            .resume(Util.URLS.RESUME_URL, UA2Bundle.getProcessData()));
            AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.PROCESS, UA2Bundle, Job.Status.RUNNING);
            AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.PROCESS, UA1Bundle, Job.Status.SUSPENDED);
        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getCause());
        } finally {

            Util.restartService(cluster2.getFeedHelper());
        }

    }

    @Test(groups = {"prism", "0.2"})
    public void testResumeDeletedProcessOnBothColosWhen1ColoIsDown() throws Exception {
        try {
            //schedule using colohelpers
            UA1Bundle.submitAndScheduleProcessUsingColoHelper(cluster2);
            UA2Bundle.submitAndScheduleProcessUsingColoHelper(cluster1);

            //delete using coloHelpers
            Util.assertSucceeded(
                    prism.getProcessHelper()
                            .delete(Util.URLS.DELETE_URL, UA1Bundle.getProcessData())
            );

            Util.shutDownService(cluster2.getProcessHelper());

            //suspend using prism
            Util.assertFailed(prism.getProcessHelper()
                    .resume(Util.URLS.RESUME_URL, UA1Bundle.getProcessData()));
            //verify
            AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.PROCESS, UA1Bundle, Job.Status.KILLED);
            AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.PROCESS, UA2Bundle, Job.Status.RUNNING);

            //suspend using prism
            Util.assertFailed(prism.getProcessHelper()
                    .resume(Util.URLS.RESUME_URL, UA1Bundle.getProcessData()));
            //verify
            AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.PROCESS, UA1Bundle, Job.Status.KILLED);
            AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.PROCESS, UA2Bundle, Job.Status.RUNNING);

            Util.assertSucceeded(
                    prism.getProcessHelper()
                            .delete(Util.URLS.DELETE_URL, UA2Bundle.getProcessData())
            );
            //suspend on the other one
            Util.assertFailed(prism.getProcessHelper()
                    .resume(Util.URLS.RESUME_URL, UA2Bundle.getProcessData()));
            AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.PROCESS, UA1Bundle, Job.Status.KILLED);
            AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.PROCESS, UA2Bundle, Job.Status.KILLED);

            Util.assertFailed(
                    cluster1.getProcessHelper()
                            .resume(Util.URLS.RESUME_URL, UA2Bundle.getProcessData())
            );
            AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.PROCESS, UA1Bundle, Job.Status.KILLED);
            AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.PROCESS, UA2Bundle, Job.Status.KILLED);
        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getCause());
        } finally {
            Util.restartService(cluster2.getFeedHelper());
        }
    }

    @Test(groups = {"prism", "0.2"})
    public void testResumeResumedProcessOnBothColosWhen1ColoIsDown() throws Exception {
        try {
            //schedule using colohelpers
            UA1Bundle.submitAndScheduleProcessUsingColoHelper(cluster2);
            UA2Bundle.submitAndScheduleProcessUsingColoHelper(cluster1);

            //suspend using prism
            Util.assertSucceeded(
                    cluster2.getProcessHelper()
                            .suspend(Util.URLS.SUSPEND_URL, UA1Bundle.getProcessData())
            );
            //verify
            AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.PROCESS, UA1Bundle, Job.Status.SUSPENDED);
            AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.PROCESS, UA2Bundle, Job.Status.RUNNING);
            Util.assertSucceeded(
                    cluster2.getProcessHelper()
                            .resume(Util.URLS.RESUME_URL, UA1Bundle.getProcessData()));
            AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.PROCESS, UA1Bundle, Job.Status.RUNNING);
            Util.shutDownService(cluster2.getProcessHelper());

            Util.assertFailed(prism.getProcessHelper()
                    .resume(Util.URLS.RESUME_URL, UA1Bundle.getProcessData()));


            Util.assertSucceeded(
                    prism.getProcessHelper()
                            .suspend(Util.URLS.SUSPEND_URL, UA2Bundle.getProcessData()));
            AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.PROCESS, UA2Bundle, Job.Status.SUSPENDED);

            for (int i = 0; i < 2; i++) {
                //suspend on the other one
                Util.assertSucceeded(
                        prism.getProcessHelper()
                                .resume(Util.URLS.RESUME_URL, UA2Bundle.getProcessData()));
                AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.PROCESS, UA1Bundle, Job.Status.RUNNING);
                AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.PROCESS, UA2Bundle, Job.Status.RUNNING);
            }

            for (int i = 0; i < 2; i++) {
                //suspend on the other one
                Util.assertSucceeded(
                        cluster1.getProcessHelper()
                                .resume(Util.URLS.RESUME_URL, UA2Bundle.getProcessData()));
                AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.PROCESS, UA1Bundle, Job.Status.RUNNING);
                AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.PROCESS, UA2Bundle, Job.Status.RUNNING);
            }

        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getCause());
        } finally {
            Util.restartService(cluster2.getProcessHelper());
        }
    }

    @Test(groups = {"prism", "0.2"})
    public void testResumeNonExistentProcessOnBothColosWhen1ColoIsDown()
    throws Exception {
        try {
            Util.shutDownService(cluster2.getProcessHelper());

            Util.assertFailed(prism.getProcessHelper()
                    .resume(Util.URLS.RESUME_URL, UA2Bundle.getProcessData()));
            Util.assertFailed(prism.getProcessHelper()
                    .resume(Util.URLS.RESUME_URL, UA1Bundle.getProcessData()));
            Util.assertFailed(
                    cluster1.getProcessHelper()
                            .resume(Util.URLS.RESUME_URL, UA2Bundle.getProcessData()));

        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getCause());
        } finally {
            Util.restartService(cluster2.getProcessHelper());
        }
    }

    @Test(groups = {"prism", "0.2"})
    public void testResumeSubmittedProcessOnBothColosWhen1ColoIsDown()
    throws Exception {
        try {
            UA1Bundle.submitProcess();
            UA2Bundle.submitProcess();

            Util.shutDownService(cluster2.getProcessHelper());

            Util.assertFailed(prism.getProcessHelper()
                    .resume(Util.URLS.RESUME_URL, UA1Bundle.getProcessData()));
            Util.assertFailed(prism.getProcessHelper()
                    .resume(Util.URLS.RESUME_URL, UA2Bundle.getProcessData()));
            Util.assertFailed(
                    cluster1.getProcessHelper()
                            .resume(Util.URLS.RESUME_URL, UA2Bundle.getProcessData())
            );
        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getCause());
        } finally {
            Util.restartService(cluster2.getProcessHelper());
        }

    }
}
