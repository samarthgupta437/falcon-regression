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
import org.apache.falcon.regression.core.response.ServiceResponse;
import org.apache.falcon.regression.core.supportClasses.ENTITY_TYPE;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.core.util.Util.URLS;
import org.apache.falcon.regression.testHelper.BaseMultiClusterTests;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.OozieClient;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;

public class PrismProcessSnSTest extends BaseMultiClusterTests {

    public static final String UA1_RUNNING = "ua1/RUNNING";
    public static final String UA2_RUNNING = "ua2/RUNNING";
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
    public void testProcessSnSOnBothColos() throws Exception {
        //schedule both bundles
        submitAndScheduleProcess(UA1Bundle);
        checkStatus(server2OC, UA1Bundle, Job.Status.RUNNING);
        checkNotStatus(server2OC, UA2Bundle, Job.Status.RUNNING);
        submitAndScheduleProcess(UA2Bundle);

        //now check if they have been scheduled correctly or not
        checkStatus(server1OC, UA2Bundle, Job.Status.RUNNING);
        //check if there is no criss cross
        ServiceResponse response =
                prism.getProcessHelper()
                        .getStatus(URLS.STATUS_URL, UA2Bundle.getProcessData());
        System.out.println(response.getMessage());
        checkNotStatus(server1OC, UA1Bundle, Job.Status.RUNNING);

    }

    @Test(groups = {"prism", "0.2"})
    public void testProcessSnSForSubmittedProcessOnBothColos() throws Exception {
        //schedule both bundles
        submitProcess(UA1Bundle);
        Util.assertSucceeded(prism.getProcessHelper()
                .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, UA1Bundle.getProcessData()));
        checkStatus(server2OC, UA1Bundle, Job.Status.RUNNING);
        checkNotStatus(server2OC, UA2Bundle, Job.Status.RUNNING);

        submitProcess(UA2Bundle);
        Util.assertSucceeded(prism.getProcessHelper()
                .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, UA1Bundle.getProcessData()));
        //now check if they have been scheduled correctly or not
        checkStatus(server2OC, UA1Bundle, Job.Status.RUNNING);
        //check if there is no criss cross
        checkNotStatus(server1OC, UA1Bundle, Job.Status.RUNNING);

    }

    @Test(groups = {"prism", "0.2"})
    public void testProcessSnSForSubmittedProcessOnBothColosUsingColoHelper()
    throws Exception {
        //schedule both bundles
        submitProcess(UA1Bundle);
        Util.assertSucceeded(prism.getProcessHelper()
                .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, UA1Bundle.getProcessData()));
        checkStatus(server2OC, UA1Bundle, Job.Status.RUNNING);
        checkNotStatus(server2OC, UA2Bundle, Job.Status.RUNNING);
        submitProcess(UA2Bundle);
        Util.assertSucceeded(prism.getProcessHelper()
                .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, UA1Bundle.getProcessData()));
        //now check if they have been scheduled correctly or not
        checkStatus(server2OC, UA1Bundle, Job.Status.RUNNING);
        //check if there is no criss cross
        checkNotStatus(server1OC, UA1Bundle, Job.Status.RUNNING);

    }

    @Test(groups = {"prism", "0.2"})
    public void testProcessSnSAlreadyScheduledOnBothColos() throws Exception {
        //schedule both bundles
        submitAndScheduleProcess(UA1Bundle);
        checkStatus(server2OC, UA1Bundle, Job.Status.RUNNING);
        checkNotStatus(server2OC, UA2Bundle, Job.Status.RUNNING);
        submitAndScheduleProcess(UA2Bundle);

        //now check if they have been scheduled correctly or not
        checkStatus(server1OC, UA2Bundle, Job.Status.RUNNING);
        //check if there is no criss cross
        checkNotStatus(server1OC, UA1Bundle, Job.Status.RUNNING);

        //reschedule trial

        Util.assertSucceeded(server2.getProcessHelper()
                .schedule(URLS.SCHEDULE_URL, UA1Bundle.getProcessData()));
        Assert.assertEquals(Util.getBundles(server2.getFeedHelper().getOozieClient(),
                Util.readEntityName(UA1Bundle.getProcessData()), ENTITY_TYPE.PROCESS).size(), 1);
        checkStatus(server2OC, UA1Bundle, Job.Status.RUNNING);
        checkNotStatus(server2OC, UA2Bundle, Job.Status.RUNNING);
    }

    @Test(groups = {"prism", "0.2"})
    public void testSnSSuspendedProcessOnBothColos() throws Exception {
        //schedule both bundles
        submitAndScheduleProcess(UA1Bundle);
        submitAndScheduleProcess(UA2Bundle);

        Util.assertSucceeded(server2.getProcessHelper()
                .suspend(URLS.SUSPEND_URL, UA1Bundle.getProcessData()));
        checkStatus(server2OC, UA1Bundle, Job.Status.SUSPENDED);
        checkStatus(server1OC, UA2Bundle, Job.Status.RUNNING);
        //now check if they have been scheduled correctly or not
        Util.assertSucceeded(prism.getProcessHelper()
                .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, UA1Bundle.getProcessData()));
        Assert.assertEquals(Util.getBundles(server2.getFeedHelper().getOozieClient(),
                Util.readEntityName(UA1Bundle.getProcessData()), ENTITY_TYPE.PROCESS).size(), 1);
        Util.assertSucceeded(server2.getProcessHelper()
                .resume(URLS.SUSPEND_URL, UA1Bundle.getProcessData()));

        Util.assertSucceeded(server1.getProcessHelper()
                .suspend(URLS.SUSPEND_URL, UA2Bundle.getProcessData()));
        Util.assertSucceeded(prism.getProcessHelper()
                .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, UA2Bundle.getProcessData()));

        Assert.assertEquals(Util.getBundles(server1.getFeedHelper().getOozieClient(),
                Util.readEntityName(UA2Bundle.getProcessData()), ENTITY_TYPE.PROCESS).size(), 1);
        checkStatus(server1OC, UA2Bundle, Job.Status.SUSPENDED);
        checkStatus(server2OC, UA1Bundle, Job.Status.SUSPENDED);
    }

    @Test(groups = {"prism", "0.2"})
    public void testSnSDeletedProcessOnBothColos() throws Exception {
        //schedule both bundles
        submitAndScheduleProcess(UA1Bundle);
        Assert.assertEquals(Util.parseResponse(
                prism.getProcessHelper()
                        .getStatus(URLS.STATUS_URL, UA1Bundle.getProcessData())).getMessage(),
                UA1_RUNNING
        );

        submitAndScheduleProcess(UA2Bundle);
        Assert.assertEquals(Util.parseResponse(
                prism.getProcessHelper()
                        .getStatus(URLS.STATUS_URL, UA2Bundle.getProcessData())).getMessage(),
                UA2_RUNNING
        );

        Util.assertSucceeded(
                prism.getProcessHelper().delete(URLS.DELETE_URL, UA1Bundle.getProcessData()));
        checkStatus(server2OC, UA1Bundle, Job.Status.KILLED);
        checkStatus(server1OC, UA2Bundle, Job.Status.RUNNING);
        Util.assertSucceeded(
                prism.getProcessHelper().delete(URLS.DELETE_URL, UA2Bundle.getProcessData()));
        checkStatus(server1OC, UA2Bundle, Job.Status.KILLED);
        checkStatus(server2OC, UA1Bundle, Job.Status.KILLED);
        Util.assertSucceeded(prism.getProcessHelper()
                .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, UA1Bundle.getProcessData()));
        Util.assertSucceeded(prism.getProcessHelper()
                .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, UA2Bundle.getProcessData()));

        Assert.assertEquals(Util.parseResponse(
                prism.getProcessHelper()
                        .getStatus(URLS.STATUS_URL, UA1Bundle.getProcessData())
        ).getMessage(),
                UA1_RUNNING
        );
        Assert.assertEquals(Util.parseResponse(
                prism.getProcessHelper()
                        .getStatus(URLS.STATUS_URL, UA2Bundle.getProcessData())
        ).getMessage(),
                UA2_RUNNING
        );

    }

    @Test(groups = {"prism", "0.2"})
    public void testScheduleNonExistentProcessOnBothColos() throws Exception {
        Assert.assertEquals(Util.parseResponse(server2.getProcessHelper()
                .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, UA1Bundle.getProcessData()))
                .getStatusCode(), 404);
        Assert.assertEquals(Util.parseResponse(server1.getProcessHelper()
                .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, UA2Bundle.getProcessData()))
                .getStatusCode(), 404);

    }

    private void checkStatus(OozieClient client, Bundle bundle, Job.Status status) throws Exception {
        Assert.assertTrue(Util.verifyOozieJobStatus(client, Util.readEntityName(bundle.getProcessData()),
                ENTITY_TYPE.PROCESS, status));
    }

    private void checkNotStatus(OozieClient client, Bundle bundle, Job.Status status) throws Exception {
        Assert.assertTrue(Util.getOozieJobStatus(client, Util.readEntityName(bundle.getProcessData()),
                ENTITY_TYPE.PROCESS) != status);
    }

    private void submitProcess(Bundle bundle) throws Exception {
        for (String cluster : bundle.getClusters()) {
            Util.assertSucceeded(
                    prism.getClusterHelper().submitEntity(URLS.SUBMIT_URL, cluster));
        }

        for (String feed : bundle.getDataSets()) {
            Util.assertSucceeded(
                    prism.getFeedHelper().submitAndSchedule(URLS.SUBMIT_URL, feed));
        }

        Util.assertSucceeded(
                prism.getProcessHelper()
                        .submitEntity(URLS.SUBMIT_URL, bundle.getProcessData())
        );
    }

    private void submitAndScheduleProcess(Bundle bundle) throws Exception {
        for (String cluster : bundle.getClusters()) {
            Util.assertSucceeded(
                    prism.getClusterHelper().submitEntity(URLS.SUBMIT_URL, cluster));
        }
        for (String feed : bundle.getDataSets()) {
            Util.assertSucceeded(
                    prism.getFeedHelper().submitAndSchedule(URLS.SUBMIT_URL, feed));
        }

        Util.assertSucceeded(prism.getProcessHelper()
                .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, bundle.getProcessData()));
    }
}
