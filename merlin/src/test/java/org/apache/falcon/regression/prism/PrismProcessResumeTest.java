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
import org.apache.falcon.regression.core.util.Util.URLS;
import org.apache.falcon.regression.testHelper.BaseMultiClusterTests;
import org.apache.oozie.client.Job;
import org.testng.Assert;
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
        UA1Bundle = new Bundle(bundle, server2.getEnvFileName());
        UA2Bundle = new Bundle(bundle, server1.getEnvFileName());

        UA1Bundle.generateUniqueBundle();
        UA2Bundle.generateUniqueBundle();

    }

    @Test(groups = {"prism", "0.2"})
    public void testResumeSuspendedFeedOnBothColos() throws Exception {
        //schedule using colohelpers
        submitAndScheduleProcessUsingColoHelper(server2, UA1Bundle);
        submitAndScheduleProcessUsingColoHelper(server1, UA2Bundle);

        //suspend using prism
        Util.assertSucceeded(prism.getProcessHelper()
                .suspend(Util.URLS.SUSPEND_URL, UA1Bundle.getProcessData()));
        //verify
        checkStatus(server2, UA1Bundle, Job.Status.SUSPENDED);
        checkStatus(server1, UA2Bundle, Job.Status.RUNNING);

        //suspend using prism
        Util.assertSucceeded(prism.getProcessHelper()
                .resume(Util.URLS.RESUME_URL, UA1Bundle.getProcessData()));
        //verify
        checkStatus(server2, UA1Bundle, Job.Status.RUNNING);
        checkStatus(server1, UA2Bundle, Job.Status.RUNNING);

        //try using the colohelper                
        Util.assertSucceeded(
                server2.getProcessHelper()
                        .suspend(Util.URLS.SUSPEND_URL, UA1Bundle.getProcessData())
        );
        //verify
        checkStatus(server2, UA1Bundle, Job.Status.SUSPENDED);
        checkStatus(server1, UA2Bundle, Job.Status.RUNNING);

        //suspend using prism
        Util.assertSucceeded(server2.getProcessHelper()
                .resume(Util.URLS.RESUME_URL, UA1Bundle.getProcessData()));
        //verify
        checkStatus(server2, UA1Bundle, Job.Status.RUNNING);
        checkStatus(server1, UA2Bundle, Job.Status.RUNNING);

        //suspend on the other one
        Util.assertSucceeded(
                server1.getProcessHelper()
                        .suspend(Util.URLS.SUSPEND_URL, UA2Bundle.getProcessData())
        );
        checkStatus(server2, UA1Bundle, Job.Status.RUNNING);
        checkStatus(server1, UA2Bundle, Job.Status.SUSPENDED);

        Util.assertSucceeded(server1.getProcessHelper()
                .resume(Util.URLS.RESUME_URL, UA2Bundle.getProcessData()));
        checkStatus(server2, UA1Bundle, Job.Status.RUNNING);
        checkStatus(server1, UA2Bundle, Job.Status.RUNNING);
    }

    @Test(groups = {"prism", "0.2"})
    public void testResumeDeletedProcessOnBothColos() throws Exception {
        //schedule using colohelpers
        submitAndScheduleProcessUsingColoHelper(server2, UA1Bundle);
        submitAndScheduleProcessUsingColoHelper(server1, UA2Bundle);

        //delete using coloHelpers
        Util.assertSucceeded(prism.getProcessHelper()
                .delete(Util.URLS.DELETE_URL, UA1Bundle.getProcessData()));


        //suspend using prism
        Util.assertFailed(prism.getProcessHelper()
                .resume(Util.URLS.RESUME_URL, UA1Bundle.getProcessData()));
        //verify
        checkStatus(server2, UA1Bundle, Job.Status.KILLED);
        checkStatus(server1, UA2Bundle, Job.Status.RUNNING);

        Util.assertSucceeded(prism.getProcessHelper()
                .delete(Util.URLS.DELETE_URL, UA2Bundle.getProcessData()));
        //suspend on the other one
        Util.assertFailed(prism.getProcessHelper()
                .resume(Util.URLS.RESUME_URL, UA2Bundle.getProcessData()));
        checkStatus(server2, UA1Bundle, Job.Status.KILLED);
        checkStatus(server1, UA2Bundle, Job.Status.KILLED);

        Util.assertFailed(server2.getProcessHelper()
                .resume(Util.URLS.RESUME_URL, UA1Bundle.getProcessData()));
        checkStatus(server2, UA1Bundle, Job.Status.KILLED);
        Util.assertFailed(server1.getProcessHelper()
                .resume(Util.URLS.RESUME_URL, UA2Bundle.getProcessData()));
        checkStatus(server1, UA2Bundle, Job.Status.KILLED);
    }

    @Test(groups = {"prism", "0.2"})
    public void testResumeResumedProcessOnBothColos() throws Exception {
        //schedule using colohelpers
        submitAndScheduleProcessUsingColoHelper(server2, UA1Bundle);
        submitAndScheduleProcessUsingColoHelper(server1, UA2Bundle);

        Util.assertSucceeded(prism.getProcessHelper()
                .suspend(Util.URLS.SUSPEND_URL, UA1Bundle.getProcessData()));
        checkStatus(server2, UA1Bundle, Job.Status.SUSPENDED);

        for (int i = 0; i < 2; i++) {
            //suspend using prism
            Util.assertSucceeded(
                    prism.getProcessHelper()
                            .resume(Util.URLS.RESUME_URL, UA1Bundle.getProcessData()));
            //verify
            checkStatus(server2, UA1Bundle, Job.Status.RUNNING);
            checkStatus(server1, UA2Bundle, Job.Status.RUNNING);
        }


        Util.assertSucceeded(prism.getProcessHelper()
                .suspend(Util.URLS.SUSPEND_URL, UA2Bundle.getProcessData()));
        checkStatus(server1, UA2Bundle, Job.Status.SUSPENDED);

        for (int i = 0; i < 2; i++) {
            Util.assertSucceeded(
                    server2.getProcessHelper()
                            .resume(Util.URLS.RESUME_URL, UA1Bundle.getProcessData())
            );
            //verify
            checkStatus(server2, UA1Bundle, Job.Status.RUNNING);
            checkStatus(server1, UA2Bundle, Job.Status.SUSPENDED);
        }


        for (int i = 0; i < 2; i++) {
            //suspend on the other one
            Util.assertSucceeded(
                    prism.getProcessHelper()
                            .resume(Util.URLS.RESUME_URL, UA2Bundle.getProcessData())
            );
            checkStatus(server2, UA1Bundle, Job.Status.RUNNING);
            checkStatus(server1, UA2Bundle, Job.Status.RUNNING);
        }

        for (int i = 0; i < 2; i++) {
            //suspend on the other one
            Util.assertSucceeded(
                    server1.getProcessHelper()
                            .resume(Util.URLS.RESUME_URL, UA2Bundle.getProcessData())
            );
            checkStatus(server2, UA1Bundle, Job.Status.RUNNING);
            checkStatus(server1, UA2Bundle, Job.Status.RUNNING);
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
        submitProcess(UA1Bundle);
        submitProcess(UA2Bundle);

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
            submitAndScheduleProcessUsingColoHelper(server2, UA1Bundle);
            submitAndScheduleProcessUsingColoHelper(server1, UA2Bundle);
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
            checkStatus(server1, UA2Bundle, Job.Status.SUSPENDED);

            //resume on the other one
            Util.assertSucceeded(
                    prism.getProcessHelper()
                            .resume(Util.URLS.RESUME_URL, UA2Bundle.getProcessData()));
            checkStatus(server1, UA2Bundle, Job.Status.RUNNING);
            checkStatus(server2, UA1Bundle, Job.Status.SUSPENDED);
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
            submitAndScheduleProcessUsingColoHelper(server2, UA1Bundle);
            submitAndScheduleProcessUsingColoHelper(server1, UA2Bundle);

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
            checkStatus(server2, UA1Bundle, Job.Status.KILLED);
            checkStatus(server1, UA2Bundle, Job.Status.RUNNING);

            //suspend using prism
            Util.assertFailed(prism.getProcessHelper()
                    .resume(Util.URLS.RESUME_URL, UA1Bundle.getProcessData()));
            //verify
            checkStatus(server2, UA1Bundle, Job.Status.KILLED);
            checkStatus(server1, UA2Bundle, Job.Status.RUNNING);

            Util.assertSucceeded(
                    prism.getProcessHelper()
                            .delete(Util.URLS.DELETE_URL, UA2Bundle.getProcessData())
            );
            //suspend on the other one
            Util.assertFailed(prism.getProcessHelper()
                    .resume(Util.URLS.RESUME_URL, UA2Bundle.getProcessData()));
            checkStatus(server2, UA1Bundle, Job.Status.KILLED);
            checkStatus(server1, UA2Bundle, Job.Status.KILLED);

            Util.assertFailed(
                    server1.getProcessHelper()
                            .resume(Util.URLS.RESUME_URL, UA2Bundle.getProcessData())
            );
            checkStatus(server2, UA1Bundle, Job.Status.KILLED);
            checkStatus(server1, UA2Bundle, Job.Status.KILLED);
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
            submitAndScheduleProcessUsingColoHelper(server2, UA1Bundle);
            submitAndScheduleProcessUsingColoHelper(server1, UA2Bundle);

            //suspend using prism
            Util.assertSucceeded(
                    server2.getProcessHelper()
                            .suspend(Util.URLS.SUSPEND_URL, UA1Bundle.getProcessData())
            );
            //verify
            checkStatus(server2, UA1Bundle, Job.Status.SUSPENDED);
            checkStatus(server1, UA2Bundle, Job.Status.RUNNING);
            Util.assertSucceeded(
                    server2.getProcessHelper()
                            .resume(Util.URLS.RESUME_URL, UA1Bundle.getProcessData()));
            checkStatus(server2, UA1Bundle, Job.Status.RUNNING);
            Util.shutDownService(server2.getProcessHelper());

            Util.assertFailed(prism.getProcessHelper()
                    .resume(Util.URLS.RESUME_URL, UA1Bundle.getProcessData()));


            Util.assertSucceeded(
                    prism.getProcessHelper()
                            .suspend(Util.URLS.SUSPEND_URL, UA2Bundle.getProcessData()));
            checkStatus(server1, UA2Bundle, Job.Status.SUSPENDED);

            for (int i = 0; i < 2; i++) {
                //suspend on the other one
                Util.assertSucceeded(
                        prism.getProcessHelper()
                                .resume(Util.URLS.RESUME_URL, UA2Bundle.getProcessData()));
                checkStatus(server2, UA1Bundle, Job.Status.RUNNING);
                checkStatus(server1, UA2Bundle, Job.Status.RUNNING);
            }

            for (int i = 0; i < 2; i++) {
                //suspend on the other one
                Util.assertSucceeded(
                        server1.getProcessHelper()
                                .resume(Util.URLS.RESUME_URL, UA2Bundle.getProcessData()));
                checkStatus(server2, UA1Bundle, Job.Status.RUNNING);
                checkStatus(server1, UA2Bundle, Job.Status.RUNNING);
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
            submitProcess(UA1Bundle);
            submitProcess(UA2Bundle);

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


    private void submitProcess(Bundle bundle) throws Exception {

        for (String cluster : bundle.getClusters()) {
            Util.assertSucceeded(
                    prism.getClusterHelper().submitEntity(Util.URLS.SUBMIT_URL, cluster));
        }
        for (String feed : bundle.getDataSets()) {
            Util.assertSucceeded(
                    prism.getFeedHelper().submitAndSchedule(URLS.SUBMIT_URL, feed));
        }

        Util.assertSucceeded(
                prism.getProcessHelper()
                        .submitEntity(Util.URLS.SUBMIT_URL, bundle.getProcessData())
        );
    }

    private void submitAndScheduleProcessUsingColoHelper(ColoHelper coloHelper, Bundle bundle)
    throws Exception {
        submitProcess(bundle);
        Util.assertSucceeded(coloHelper.getProcessHelper()
                .schedule(Util.URLS.SCHEDULE_URL, bundle.getProcessData()));
    }

    private void checkStatus(ColoHelper coloHelper, Bundle bundle, Job.Status expectedStatus) throws Exception {
        Assert.assertTrue(Util.verifyOozieJobStatus(coloHelper.getFeedHelper().getOozieClient(),
                Util.readEntityName(bundle.getProcessData()), ENTITY_TYPE.PROCESS, expectedStatus));
    }
}
