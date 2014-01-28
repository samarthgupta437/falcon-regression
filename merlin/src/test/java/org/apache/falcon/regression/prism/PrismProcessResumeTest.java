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

public class PrismProcessResumeTest extends BaseMultiClusterTests {

    Bundle UA1Bundle = new Bundle();
    Bundle UA2Bundle = new Bundle();

    @BeforeMethod(alwaysRun = true)
    public void setUp(Method method) throws Exception {
        Util.print("test name: " + method.getName());
        Bundle bundle = Util.readBundles("LateDataBundles")[0][0];
        UA1Bundle = new Bundle(bundle, server2.getEnvFileName(), server2.getPrefix());
        UA2Bundle = new Bundle(bundle, server1.getEnvFileName(), server1.getPrefix());

        UA1Bundle.generateUniqueBundle();
        UA2Bundle.generateUniqueBundle();

    }

    @Test(groups = {"prism", "0.2"})
    public void testResumeSuspendedFeedOnBothColos() throws Exception {
        //schedule using colohelpers
        UA1Bundle.submitAndScheduleProcessUsingColoHelper(server2);
        UA2Bundle.submitAndScheduleProcessUsingColoHelper(server1);

        //suspend using prism
        Util.assertSucceeded(prism.getProcessHelper()
                .suspend(Util.URLS.SUSPEND_URL, UA1Bundle.getProcessData()));
        //verify
        AssertUtil.checkStatus(server2OC, ENTITY_TYPE.PROCESS, UA1Bundle, Job.Status.SUSPENDED);
        AssertUtil.checkStatus(server1OC, ENTITY_TYPE.PROCESS, UA2Bundle, Job.Status.RUNNING);

        //suspend using prism
        Util.assertSucceeded(prism.getProcessHelper()
                .resume(Util.URLS.RESUME_URL, UA1Bundle.getProcessData()));
        //verify
        AssertUtil.checkStatus(server2OC, ENTITY_TYPE.PROCESS, UA1Bundle, Job.Status.RUNNING);
        AssertUtil.checkStatus(server1OC, ENTITY_TYPE.PROCESS, UA2Bundle, Job.Status.RUNNING);

        //try using the colohelper                
        Util.assertSucceeded(
                server2.getProcessHelper()
                        .suspend(Util.URLS.SUSPEND_URL, UA1Bundle.getProcessData())
        );
        //verify
        AssertUtil.checkStatus(server2OC, ENTITY_TYPE.PROCESS, UA1Bundle, Job.Status.SUSPENDED);
        AssertUtil.checkStatus(server1OC, ENTITY_TYPE.PROCESS, UA2Bundle, Job.Status.RUNNING);

        //suspend using prism
        Util.assertSucceeded(server2.getProcessHelper()
                .resume(Util.URLS.RESUME_URL, UA1Bundle.getProcessData()));
        //verify
        AssertUtil.checkStatus(server2OC, ENTITY_TYPE.PROCESS, UA1Bundle, Job.Status.RUNNING);
        AssertUtil.checkStatus(server1OC, ENTITY_TYPE.PROCESS, UA2Bundle, Job.Status.RUNNING);

        //suspend on the other one
        Util.assertSucceeded(
                server1.getProcessHelper()
                        .suspend(Util.URLS.SUSPEND_URL, UA2Bundle.getProcessData())
        );
        AssertUtil.checkStatus(server2OC, ENTITY_TYPE.PROCESS, UA1Bundle, Job.Status.RUNNING);
        AssertUtil.checkStatus(server1OC, ENTITY_TYPE.PROCESS, UA2Bundle, Job.Status.SUSPENDED);

        Util.assertSucceeded(server1.getProcessHelper()
                .resume(Util.URLS.RESUME_URL, UA2Bundle.getProcessData()));
        AssertUtil.checkStatus(server2OC, ENTITY_TYPE.PROCESS, UA1Bundle, Job.Status.RUNNING);
        AssertUtil.checkStatus(server1OC, ENTITY_TYPE.PROCESS, UA2Bundle, Job.Status.RUNNING);
    }

    @Test(groups = {"prism", "0.2"})
    public void testResumeDeletedProcessOnBothColos() throws Exception {
        //schedule using colohelpers
        UA1Bundle.submitAndScheduleProcessUsingColoHelper(server2);
        UA2Bundle.submitAndScheduleProcessUsingColoHelper(server1);

        //delete using coloHelpers
        Util.assertSucceeded(prism.getProcessHelper()
                .delete(Util.URLS.DELETE_URL, UA1Bundle.getProcessData()));


        //suspend using prism
        Util.assertFailed(prism.getProcessHelper()
                .resume(Util.URLS.RESUME_URL, UA1Bundle.getProcessData()));
        //verify
        AssertUtil.checkStatus(server2OC, ENTITY_TYPE.PROCESS, UA1Bundle, Job.Status.KILLED);
        AssertUtil.checkStatus(server1OC, ENTITY_TYPE.PROCESS, UA2Bundle, Job.Status.RUNNING);

        Util.assertSucceeded(prism.getProcessHelper()
                .delete(Util.URLS.DELETE_URL, UA2Bundle.getProcessData()));
        //suspend on the other one
        Util.assertFailed(prism.getProcessHelper()
                .resume(Util.URLS.RESUME_URL, UA2Bundle.getProcessData()));
        AssertUtil.checkStatus(server2OC, ENTITY_TYPE.PROCESS, UA1Bundle, Job.Status.KILLED);
        AssertUtil.checkStatus(server1OC, ENTITY_TYPE.PROCESS, UA2Bundle, Job.Status.KILLED);

        Util.assertFailed(server2.getProcessHelper()
                .resume(Util.URLS.RESUME_URL, UA1Bundle.getProcessData()));
        AssertUtil.checkStatus(server2OC, ENTITY_TYPE.PROCESS, UA1Bundle, Job.Status.KILLED);
        Util.assertFailed(server1.getProcessHelper()
                .resume(Util.URLS.RESUME_URL, UA2Bundle.getProcessData()));
        AssertUtil.checkStatus(server1OC, ENTITY_TYPE.PROCESS, UA2Bundle, Job.Status.KILLED);
    }

    @Test(groups = {"prism", "0.2"})
    public void testResumeResumedProcessOnBothColos() throws Exception {
        //schedule using colohelpers
        UA1Bundle.submitAndScheduleProcessUsingColoHelper(server2);
        UA2Bundle.submitAndScheduleProcessUsingColoHelper(server1);

        Util.assertSucceeded(prism.getProcessHelper()
                .suspend(Util.URLS.SUSPEND_URL, UA1Bundle.getProcessData()));
        AssertUtil.checkStatus(server2OC, ENTITY_TYPE.PROCESS, UA1Bundle, Job.Status.SUSPENDED);

        for (int i = 0; i < 2; i++) {
            //suspend using prism
            Util.assertSucceeded(
                    prism.getProcessHelper()
                            .resume(Util.URLS.RESUME_URL, UA1Bundle.getProcessData()));
            //verify
            AssertUtil.checkStatus(server2OC, ENTITY_TYPE.PROCESS, UA1Bundle, Job.Status.RUNNING);
            AssertUtil.checkStatus(server1OC, ENTITY_TYPE.PROCESS, UA2Bundle, Job.Status.RUNNING);
        }


        Util.assertSucceeded(prism.getProcessHelper()
                .suspend(Util.URLS.SUSPEND_URL, UA2Bundle.getProcessData()));
        AssertUtil.checkStatus(server1OC, ENTITY_TYPE.PROCESS, UA2Bundle, Job.Status.SUSPENDED);

        for (int i = 0; i < 2; i++) {
            Util.assertSucceeded(
                    server2.getProcessHelper()
                            .resume(Util.URLS.RESUME_URL, UA1Bundle.getProcessData())
            );
            //verify
            AssertUtil.checkStatus(server2OC, ENTITY_TYPE.PROCESS, UA1Bundle, Job.Status.RUNNING);
            AssertUtil.checkStatus(server1OC, ENTITY_TYPE.PROCESS, UA2Bundle, Job.Status.SUSPENDED);
        }


        for (int i = 0; i < 2; i++) {
            //suspend on the other one
            Util.assertSucceeded(
                    prism.getProcessHelper()
                            .resume(Util.URLS.RESUME_URL, UA2Bundle.getProcessData())
            );
            AssertUtil.checkStatus(server2OC, ENTITY_TYPE.PROCESS, UA1Bundle, Job.Status.RUNNING);
            AssertUtil.checkStatus(server1OC, ENTITY_TYPE.PROCESS, UA2Bundle, Job.Status.RUNNING);
        }

        for (int i = 0; i < 2; i++) {
            //suspend on the other one
            Util.assertSucceeded(
                    server1.getProcessHelper()
                            .resume(Util.URLS.RESUME_URL, UA2Bundle.getProcessData())
            );
            AssertUtil.checkStatus(server2OC, ENTITY_TYPE.PROCESS, UA1Bundle, Job.Status.RUNNING);
            AssertUtil.checkStatus(server1OC, ENTITY_TYPE.PROCESS, UA2Bundle, Job.Status.RUNNING);
        }
    }

    @Test()
    public void testResumeNonExistentProcessOnBothColos() throws Exception {
        Util.assertFailed(prism.getProcessHelper()
                .resume(Util.URLS.RESUME_URL, UA1Bundle.getProcessData()));
        Util.assertFailed(prism.getProcessHelper()
                .resume(Util.URLS.RESUME_URL, UA2Bundle.getProcessData()));

        Util.assertFailed(server2.getProcessHelper()
                .resume(Util.URLS.RESUME_URL, UA1Bundle.getProcessData()));
        Util.assertFailed(server1.getProcessHelper()
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

        Util.assertFailed(server2.getProcessHelper()
                .resume(Util.URLS.RESUME_URL, UA1Bundle.getProcessData()));
        Util.assertFailed(server1.getProcessHelper()
                .resume(Util.URLS.RESUME_URL, UA2Bundle.getProcessData()));


    }

    @Test(groups = {"prism", "0.2"})
    public void testResumeScheduledProcessOnBothColosWhen1ColoIsDown()
    throws Exception {
        try {
            //schedule using colohelpers
            UA1Bundle.submitAndScheduleProcessUsingColoHelper(server2);
            UA2Bundle.submitAndScheduleProcessUsingColoHelper(server1);
            Util.assertSucceeded(
                    server2.getProcessHelper()
                            .suspend(Util.URLS.SUSPEND_URL, UA1Bundle.getProcessData())
            );
            Util.assertSucceeded(
                    server1.getProcessHelper()
                            .suspend(Util.URLS.SUSPEND_URL, UA2Bundle.getProcessData())
            );

            Util.shutDownService(server2.getProcessHelper());


            Util.assertFailed(prism.getProcessHelper()
                    .resume(Util.URLS.RESUME_URL, UA1Bundle.getProcessData()));
            //verify
            AssertUtil.checkStatus(server1OC, ENTITY_TYPE.PROCESS, UA2Bundle, Job.Status.SUSPENDED);

            //resume on the other one
            Util.assertSucceeded(
                    prism.getProcessHelper()
                            .resume(Util.URLS.RESUME_URL, UA2Bundle.getProcessData()));
            AssertUtil.checkStatus(server1OC, ENTITY_TYPE.PROCESS, UA2Bundle, Job.Status.RUNNING);
            AssertUtil.checkStatus(server2OC, ENTITY_TYPE.PROCESS, UA1Bundle, Job.Status.SUSPENDED);
        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getCause());
        } finally {

            Util.restartService(server2.getFeedHelper());
        }

    }

    @Test(groups = {"prism", "0.2"})
    public void testResumeDeletedProcessOnBothColosWhen1ColoIsDown() throws Exception {
        try {
            //schedule using colohelpers
            UA1Bundle.submitAndScheduleProcessUsingColoHelper(server2);
            UA2Bundle.submitAndScheduleProcessUsingColoHelper(server1);

            //delete using coloHelpers
            Util.assertSucceeded(
                    prism.getProcessHelper()
                            .delete(Util.URLS.DELETE_URL, UA1Bundle.getProcessData())
            );

            Util.shutDownService(server2.getProcessHelper());

            //suspend using prism
            Util.assertFailed(prism.getProcessHelper()
                    .resume(Util.URLS.RESUME_URL, UA1Bundle.getProcessData()));
            //verify
            AssertUtil.checkStatus(server2OC, ENTITY_TYPE.PROCESS, UA1Bundle, Job.Status.KILLED);
            AssertUtil.checkStatus(server1OC, ENTITY_TYPE.PROCESS, UA2Bundle, Job.Status.RUNNING);

            //suspend using prism
            Util.assertFailed(prism.getProcessHelper()
                    .resume(Util.URLS.RESUME_URL, UA1Bundle.getProcessData()));
            //verify
            AssertUtil.checkStatus(server2OC, ENTITY_TYPE.PROCESS, UA1Bundle, Job.Status.KILLED);
            AssertUtil.checkStatus(server1OC, ENTITY_TYPE.PROCESS, UA2Bundle, Job.Status.RUNNING);

            Util.assertSucceeded(
                    prism.getProcessHelper()
                            .delete(Util.URLS.DELETE_URL, UA2Bundle.getProcessData())
            );
            //suspend on the other one
            Util.assertFailed(prism.getProcessHelper()
                    .resume(Util.URLS.RESUME_URL, UA2Bundle.getProcessData()));
            AssertUtil.checkStatus(server2OC, ENTITY_TYPE.PROCESS, UA1Bundle, Job.Status.KILLED);
            AssertUtil.checkStatus(server1OC, ENTITY_TYPE.PROCESS, UA2Bundle, Job.Status.KILLED);

            Util.assertFailed(
                    server1.getProcessHelper()
                            .resume(Util.URLS.RESUME_URL, UA2Bundle.getProcessData())
            );
            AssertUtil.checkStatus(server2OC, ENTITY_TYPE.PROCESS, UA1Bundle, Job.Status.KILLED);
            AssertUtil.checkStatus(server1OC, ENTITY_TYPE.PROCESS, UA2Bundle, Job.Status.KILLED);
        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getCause());
        } finally {
            Util.restartService(server2.getFeedHelper());
        }
    }

    @Test(groups = {"prism", "0.2"})
    public void testResumeResumedProcessOnBothColosWhen1ColoIsDown() throws Exception {
        try {
            //schedule using colohelpers
            UA1Bundle.submitAndScheduleProcessUsingColoHelper(server2);
            UA2Bundle.submitAndScheduleProcessUsingColoHelper(server1);

            //suspend using prism
            Util.assertSucceeded(
                    server2.getProcessHelper()
                            .suspend(Util.URLS.SUSPEND_URL, UA1Bundle.getProcessData())
            );
            //verify
            AssertUtil.checkStatus(server2OC, ENTITY_TYPE.PROCESS, UA1Bundle, Job.Status.SUSPENDED);
            AssertUtil.checkStatus(server1OC, ENTITY_TYPE.PROCESS, UA2Bundle, Job.Status.RUNNING);
            Util.assertSucceeded(
                    server2.getProcessHelper()
                            .resume(Util.URLS.RESUME_URL, UA1Bundle.getProcessData()));
            AssertUtil.checkStatus(server2OC, ENTITY_TYPE.PROCESS, UA1Bundle, Job.Status.RUNNING);
            Util.shutDownService(server2.getProcessHelper());

            Util.assertFailed(prism.getProcessHelper()
                    .resume(Util.URLS.RESUME_URL, UA1Bundle.getProcessData()));


            Util.assertSucceeded(
                    prism.getProcessHelper()
                            .suspend(Util.URLS.SUSPEND_URL, UA2Bundle.getProcessData()));
            AssertUtil.checkStatus(server1OC, ENTITY_TYPE.PROCESS, UA2Bundle, Job.Status.SUSPENDED);

            for (int i = 0; i < 2; i++) {
                //suspend on the other one
                Util.assertSucceeded(
                        prism.getProcessHelper()
                                .resume(Util.URLS.RESUME_URL, UA2Bundle.getProcessData()));
                AssertUtil.checkStatus(server2OC, ENTITY_TYPE.PROCESS, UA1Bundle, Job.Status.RUNNING);
                AssertUtil.checkStatus(server1OC, ENTITY_TYPE.PROCESS, UA2Bundle, Job.Status.RUNNING);
            }

            for (int i = 0; i < 2; i++) {
                //suspend on the other one
                Util.assertSucceeded(
                        server1.getProcessHelper()
                                .resume(Util.URLS.RESUME_URL, UA2Bundle.getProcessData()));
                AssertUtil.checkStatus(server2OC, ENTITY_TYPE.PROCESS, UA1Bundle, Job.Status.RUNNING);
                AssertUtil.checkStatus(server1OC, ENTITY_TYPE.PROCESS, UA2Bundle, Job.Status.RUNNING);
            }

        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getCause());
        } finally {
            Util.restartService(server2.getProcessHelper());
        }
    }

    @Test(groups = {"prism", "0.2"})
    public void testResumeNonExistentProcessOnBothColosWhen1ColoIsDown()
    throws Exception {
        try {
            Util.shutDownService(server2.getProcessHelper());

            Util.assertFailed(prism.getProcessHelper()
                    .resume(Util.URLS.RESUME_URL, UA2Bundle.getProcessData()));
            Util.assertFailed(prism.getProcessHelper()
                    .resume(Util.URLS.RESUME_URL, UA1Bundle.getProcessData()));
            Util.assertFailed(
                    server1.getProcessHelper()
                            .resume(Util.URLS.RESUME_URL, UA2Bundle.getProcessData()));

        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getCause());
        } finally {
            Util.restartService(server2.getProcessHelper());
        }
    }

    @Test(groups = {"prism", "0.2"})
    public void testResumeSubmittedProcessOnBothColosWhen1ColoIsDown()
    throws Exception {
        try {
            UA1Bundle.submitProcess();
            UA2Bundle.submitProcess();

            Util.shutDownService(server2.getProcessHelper());

            Util.assertFailed(prism.getProcessHelper()
                    .resume(Util.URLS.RESUME_URL, UA1Bundle.getProcessData()));
            Util.assertFailed(prism.getProcessHelper()
                    .resume(Util.URLS.RESUME_URL, UA2Bundle.getProcessData()));
            Util.assertFailed(
                    server1.getProcessHelper()
                            .resume(Util.URLS.RESUME_URL, UA2Bundle.getProcessData())
            );
        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getCause());
        } finally {
            Util.restartService(server2.getProcessHelper());
        }

    }
}
