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
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.helpers.PrismHelper;
import org.apache.falcon.regression.core.response.ServiceResponse;
import org.apache.falcon.regression.core.supportClasses.ENTITY_TYPE;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.core.util.Util.URLS;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.OozieClient;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.lang.reflect.Method;

public class PrismProcessSnSTest {


    @BeforeMethod(alwaysRun = true)
    public void testName(Method method) {
        Util.print("test name: " + method.getName());
    }


    PrismHelper prismHelper = new PrismHelper("prism.properties");
    ColoHelper UA1ColoHelper = new ColoHelper("mk-qa.config.properties");
    ColoHelper UA2ColoHelper = new ColoHelper("ivoryqa-1.config.properties");
    OozieClient UA1OozieClient = UA1ColoHelper.getFeedHelper().getOozieClient();
    OozieClient UA2OozieClient = UA2ColoHelper.getFeedHelper().getOozieClient();

    @Test(dataProvider = "DP", groups = {"prism", "0.2"})
    public void testProcessSnSOnBothColos(Bundle bundle) throws Exception {
        Bundle UA1Bundle = new Bundle(bundle, UA1ColoHelper.getEnvFileName());
        Bundle UA2Bundle = new Bundle(bundle, UA2ColoHelper.getEnvFileName());

        UA1Bundle.generateUniqueBundle();
        UA2Bundle.generateUniqueBundle();

        //schedule both bundles
        submitAndScheduleProcess(UA1Bundle);
        checkStatus(UA1OozieClient, UA1Bundle, Job.Status.RUNNING);
        checkNotStatus(UA1OozieClient, UA2Bundle, Job.Status.RUNNING);
        submitAndScheduleProcess(UA2Bundle);

        //now check if they have been scheduled correctly or not
        checkStatus(UA2OozieClient, UA2Bundle, Job.Status.RUNNING);
        //check if there is no criss cross
        ServiceResponse response =
                prismHelper.getProcessHelper()
                        .getStatus(URLS.STATUS_URL, UA2Bundle.getProcessData());
        System.out.println(response.getMessage());
        checkNotStatus(UA2OozieClient, UA1Bundle, Job.Status.RUNNING);

    }

    @Test(dataProvider = "DP", groups = {"prism", "0.2"})
    public void testProcessSnSForSubmittedProcessOnBothColos(Bundle bundle) throws Exception {
        Bundle UA1Bundle = new Bundle(bundle, UA1ColoHelper.getEnvFileName());
        Bundle UA2Bundle = new Bundle(bundle, UA2ColoHelper.getEnvFileName());

        UA1Bundle.generateUniqueBundle();
        UA2Bundle.generateUniqueBundle();

        //schedule both bundles
        submitProcess(UA1Bundle);
        Util.assertSucceeded(prismHelper.getProcessHelper()
                .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, UA1Bundle.getProcessData()));
        checkStatus(UA1OozieClient, UA1Bundle, Job.Status.RUNNING);
        checkNotStatus(UA1OozieClient, UA2Bundle, Job.Status.RUNNING);

        submitProcess(UA2Bundle);
        Util.assertSucceeded(prismHelper.getProcessHelper()
                .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, UA1Bundle.getProcessData()));
        //now check if they have been scheduled correctly or not
        checkStatus(UA1OozieClient, UA1Bundle, Job.Status.RUNNING);
        //check if there is no criss cross
        checkNotStatus(UA2OozieClient, UA1Bundle, Job.Status.RUNNING);

    }

    @Test(dataProvider = "DP", groups = {"prism", "0.2"})
    public void testProcessSnSForSubmittedProcessOnBothColosUsingColoHelper(Bundle bundle)
    throws Exception {
        Bundle UA1Bundle = new Bundle(bundle, UA1ColoHelper.getEnvFileName());
        Bundle UA2Bundle = new Bundle(bundle, UA2ColoHelper.getEnvFileName());

        UA1Bundle.generateUniqueBundle();
        UA2Bundle.generateUniqueBundle();

        //schedule both bundles
        submitProcess(UA1Bundle);
        Util.assertSucceeded(prismHelper.getProcessHelper()
                .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, UA1Bundle.getProcessData()));
        checkStatus(UA1OozieClient, UA1Bundle, Job.Status.RUNNING);
        checkNotStatus(UA1OozieClient, UA2Bundle, Job.Status.RUNNING);
        submitProcess(UA2Bundle);
        Util.assertSucceeded(prismHelper.getProcessHelper()
                .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, UA1Bundle.getProcessData()));
        //now check if they have been scheduled correctly or not
        checkStatus(UA1OozieClient, UA1Bundle, Job.Status.RUNNING);
        //check if there is no criss cross
        checkNotStatus(UA2OozieClient, UA1Bundle, Job.Status.RUNNING);

    }

    @Test(dataProvider = "DP", groups = {"prism", "0.2"})
    public void testProcessSnSAlreadyScheduledOnBothColos(Bundle bundle) throws Exception {
        Bundle UA1Bundle = new Bundle(bundle, UA1ColoHelper.getEnvFileName());
        Bundle UA2Bundle = new Bundle(bundle, UA2ColoHelper.getEnvFileName());

        UA1Bundle.generateUniqueBundle();
        UA2Bundle.generateUniqueBundle();

        //schedule both bundles
        submitAndScheduleProcess(UA1Bundle);
        checkStatus(UA1OozieClient, UA1Bundle, Job.Status.RUNNING);
        checkNotStatus(UA1OozieClient, UA2Bundle, Job.Status.RUNNING);
        submitAndScheduleProcess(UA2Bundle);

        //now check if they have been scheduled correctly or not
        checkStatus(UA2OozieClient, UA2Bundle, Job.Status.RUNNING);
        //check if there is no criss cross
        checkNotStatus(UA2OozieClient, UA1Bundle, Job.Status.RUNNING);

        //reschedule trial

        Util.assertSucceeded(UA1ColoHelper.getProcessHelper()
                .schedule(URLS.SCHEDULE_URL, UA1Bundle.getProcessData()));
        Assert.assertEquals(Util.getBundles(UA1ColoHelper.getFeedHelper().getOozieClient(),
                        Util.readEntityName(UA1Bundle.getProcessData()), ENTITY_TYPE.PROCESS).size(), 1);
        checkStatus(UA1OozieClient, UA1Bundle, Job.Status.RUNNING);
        checkNotStatus(UA1OozieClient, UA2Bundle, Job.Status.RUNNING);
    }

    @Test(dataProvider = "DP", groups = {"prism", "0.2"})
    public void testSnSSuspendedProcessOnBothColos(Bundle bundle) throws Exception {
        Bundle UA1Bundle = new Bundle(bundle, UA1ColoHelper.getEnvFileName());
        Bundle UA2Bundle = new Bundle(bundle, UA2ColoHelper.getEnvFileName());

        UA1Bundle.generateUniqueBundle();
        UA2Bundle.generateUniqueBundle();

        //schedule both bundles
        submitAndScheduleProcess(UA1Bundle);
        submitAndScheduleProcess(UA2Bundle);

        Util.assertSucceeded(UA1ColoHelper.getProcessHelper()
                .suspend(URLS.SUSPEND_URL, UA1Bundle.getProcessData()));
        checkStatus(UA1OozieClient, UA1Bundle, Job.Status.SUSPENDED);
        checkStatus(UA2OozieClient, UA2Bundle, Job.Status.RUNNING);
        //now check if they have been scheduled correctly or not
        Util.assertSucceeded(prismHelper.getProcessHelper()
                .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, UA1Bundle.getProcessData()));
        Assert.assertEquals(Util.getBundles(UA1ColoHelper.getFeedHelper().getOozieClient(),
                Util.readEntityName(UA1Bundle.getProcessData()), ENTITY_TYPE.PROCESS).size(), 1);
        Util.assertSucceeded(UA1ColoHelper.getProcessHelper()
                .resume(URLS.SUSPEND_URL, UA1Bundle.getProcessData()));

        Util.assertSucceeded(UA2ColoHelper.getProcessHelper()
                .suspend(URLS.SUSPEND_URL, UA2Bundle.getProcessData()));
        Util.assertSucceeded(prismHelper.getProcessHelper()
                .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, UA2Bundle.getProcessData()));

        Assert.assertEquals(Util.getBundles(UA2ColoHelper.getFeedHelper().getOozieClient(),
                Util.readEntityName(UA2Bundle.getProcessData()), ENTITY_TYPE.PROCESS).size(), 1);
        checkStatus(UA2OozieClient, UA2Bundle, Job.Status.SUSPENDED);
        checkStatus(UA1OozieClient, UA1Bundle, Job.Status.SUSPENDED);
    }

    @Test(dataProvider = "DP", groups = {"prism", "0.2"})
    public void testSnSDeletedProcessOnBothColos(Bundle bundle) throws Exception {
        Bundle UA1Bundle = new Bundle(bundle, UA1ColoHelper.getEnvFileName());
        Bundle UA2Bundle = new Bundle(bundle, UA2ColoHelper.getEnvFileName());

        UA1Bundle.generateUniqueBundle();
        UA2Bundle.generateUniqueBundle();

        //schedule both bundles
        submitAndScheduleProcess(UA1Bundle);
        submitAndScheduleProcess(UA2Bundle);

        Assert.assertEquals(Util.parseResponse(
                prismHelper.getProcessHelper()
                        .getStatus(URLS.STATUS_URL, UA1Bundle.getProcessData())).getMessage(),
                "ua1/RUNNING");
        Assert.assertEquals(Util.parseResponse(
                prismHelper.getProcessHelper()
                        .getStatus(URLS.STATUS_URL, UA2Bundle.getProcessData())).getMessage(),
                "ua2/RUNNING");

        Util.assertSucceeded(
                prismHelper.getProcessHelper().delete(URLS.DELETE_URL, UA1Bundle.getProcessData()));
        checkStatus(UA1OozieClient, UA1Bundle, Job.Status.KILLED);
        checkStatus(UA2OozieClient, UA2Bundle, Job.Status.RUNNING);
        Util.assertSucceeded(
                prismHelper.getProcessHelper().delete(URLS.DELETE_URL, UA2Bundle.getProcessData()));
        checkStatus(UA2OozieClient, UA2Bundle, Job.Status.KILLED);
        checkStatus(UA1OozieClient, UA1Bundle, Job.Status.KILLED);
        Util.assertSucceeded(prismHelper.getProcessHelper()
                .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, UA1Bundle.getProcessData()));
        Util.assertSucceeded(prismHelper.getProcessHelper()
                .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, UA2Bundle.getProcessData()));

        Assert.assertEquals(Util.parseResponse(
                prismHelper.getProcessHelper()
                        .getStatus(URLS.STATUS_URL, UA1Bundle.getProcessData())).getMessage(),
                "ua1/RUNNING");
        Assert.assertEquals(Util.parseResponse(
                prismHelper.getProcessHelper()
                        .getStatus(URLS.STATUS_URL, UA2Bundle.getProcessData())).getMessage(),
                "ua2/RUNNING");

    }

    @Test(dataProvider = "DP", groups = {"prism", "0.2"})
    public void testScheduleNonExistentProcessOnBothColos(Bundle bundle) throws Exception {
        Bundle UA1Bundle = new Bundle(bundle, UA1ColoHelper.getEnvFileName());
        Bundle UA2Bundle = new Bundle(bundle, UA2ColoHelper.getEnvFileName());

        UA1Bundle.generateUniqueBundle();
        UA2Bundle.generateUniqueBundle();

        Assert.assertEquals(Util.parseResponse(UA1ColoHelper.getProcessHelper()
                .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, UA1Bundle.getProcessData()))
                .getStatusCode(), 404);
        Assert.assertEquals(Util.parseResponse(UA2ColoHelper.getProcessHelper()
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

    private void submitAndScheduleProcess(Bundle bundle) throws Exception {
        for (String cluster : bundle.getClusters()) {
            Util.assertSucceeded(
                    prismHelper.getClusterHelper().submitEntity(Util.URLS.SUBMIT_URL, cluster));
        }
        for (String feed : bundle.getDataSets()) {
            Util.assertSucceeded(
                    prismHelper.getFeedHelper().submitAndSchedule(URLS.SUBMIT_URL, feed));
        }

        Util.assertSucceeded(prismHelper.getProcessHelper()
                .submitAndSchedule(Util.URLS.SUBMIT_AND_SCHEDULE_URL, bundle.getProcessData()));
    }


    @DataProvider(name = "DP")
    public Object[][] getData() throws Exception {
        return Util.readBundles("LateDataBundles");
    }
}
