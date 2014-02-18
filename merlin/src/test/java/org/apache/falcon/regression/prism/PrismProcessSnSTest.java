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
import org.apache.falcon.regression.core.response.ServiceResponse;
import org.apache.falcon.regression.core.supportClasses.ENTITY_TYPE;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.core.util.Util.URLS;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.OozieClient;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;

public class PrismProcessSnSTest extends BaseTestClass {

    ColoHelper cluster1;
    ColoHelper cluster2;
    OozieClient cluster1OC;
    OozieClient cluster2OC;
    public static String RUNNING = "/RUNNING";
    public static String UA1_RUNNING;
    public static String UA2_RUNNING;
    Bundle UA1Bundle = new Bundle();
    Bundle UA2Bundle = new Bundle();


    public PrismProcessSnSTest(){
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

    @Test(groups = {"prism", "0.2"})
    public void testProcessSnSOnBothColos() throws Exception {
        //schedule both bundles
        UA1Bundle.submitAndScheduleProcess();
        AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.PROCESS, UA1Bundle, Job.Status.RUNNING);
        AssertUtil.checkNotStatus(cluster2OC, ENTITY_TYPE.PROCESS, UA2Bundle, Job.Status.RUNNING);
        UA2Bundle.submitAndScheduleProcess();

        //now check if they have been scheduled correctly or not
        AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.PROCESS, UA2Bundle, Job.Status.RUNNING);
        //check if there is no criss cross
        ServiceResponse response =
                prism.getProcessHelper()
                        .getStatus(URLS.STATUS_URL, UA2Bundle.getProcessData());
        System.out.println(response.getMessage());
        AssertUtil.checkNotStatus(cluster1OC, ENTITY_TYPE.PROCESS, UA1Bundle, Job.Status.RUNNING);

    }

    @Test(groups = {"prism", "0.2"})
    public void testProcessSnSForSubmittedProcessOnBothColos() throws Exception {
        //schedule both bundles
        UA1Bundle.submitProcess();

        Util.assertSucceeded(prism.getProcessHelper()
                .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, UA1Bundle.getProcessData()));
        AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.PROCESS, UA1Bundle, Job.Status.RUNNING);
        AssertUtil.checkNotStatus(cluster2OC, ENTITY_TYPE.PROCESS, UA2Bundle, Job.Status.RUNNING);

        UA2Bundle.submitProcess();

        Util.assertSucceeded(prism.getProcessHelper()
                .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, UA1Bundle.getProcessData()));
        //now check if they have been scheduled correctly or not
        AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.PROCESS, UA1Bundle, Job.Status.RUNNING);
        //check if there is no criss cross
        AssertUtil.checkNotStatus(cluster1OC, ENTITY_TYPE.PROCESS, UA1Bundle, Job.Status.RUNNING);

    }

    @Test(groups = {"prism", "0.2"})
    public void testProcessSnSForSubmittedProcessOnBothColosUsingColoHelper()
    throws Exception {
        //schedule both bundles
        UA1Bundle.submitProcess();

        Util.assertSucceeded(prism.getProcessHelper()
                .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, UA1Bundle.getProcessData()));
        AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.PROCESS, UA1Bundle, Job.Status.RUNNING);
        AssertUtil.checkNotStatus(cluster2OC, ENTITY_TYPE.PROCESS, UA2Bundle, Job.Status.RUNNING);
        UA2Bundle.submitProcess();
        AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.PROCESS, UA1Bundle, Job.Status.RUNNING);
        AssertUtil.checkNotStatus(cluster2OC, ENTITY_TYPE.PROCESS, UA2Bundle, Job.Status.RUNNING);
        UA2Bundle.submitProcess();

        Util.assertSucceeded(prism.getProcessHelper()
                .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, UA1Bundle.getProcessData()));
        //now check if they have been scheduled correctly or not
        AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.PROCESS, UA1Bundle, Job.Status.RUNNING);
        //check if there is no criss cross
        AssertUtil.checkNotStatus(cluster1OC, ENTITY_TYPE.PROCESS, UA1Bundle, Job.Status.RUNNING);

    }

    @Test(groups = {"prism", "0.2"})
    public void testProcessSnSAlreadyScheduledOnBothColos() throws Exception {
        //schedule both bundles
        UA1Bundle.submitAndScheduleProcess();
        AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.PROCESS, UA1Bundle, Job.Status.RUNNING);
        AssertUtil.checkNotStatus(cluster2OC, ENTITY_TYPE.PROCESS, UA2Bundle, Job.Status.RUNNING);
        UA2Bundle.submitAndScheduleProcess();

        //now check if they have been scheduled correctly or not
        AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.PROCESS, UA2Bundle, Job.Status.RUNNING);
        //check if there is no criss cross
        AssertUtil.checkNotStatus(cluster1OC, ENTITY_TYPE.PROCESS, UA1Bundle, Job.Status.RUNNING);

        //reschedule trial

        Util.assertSucceeded(cluster2.getProcessHelper()
                .schedule(URLS.SCHEDULE_URL, UA1Bundle.getProcessData()));
        Assert.assertEquals(Util.getBundles(cluster2.getFeedHelper().getOozieClient(),
                Util.readEntityName(UA1Bundle.getProcessData()), ENTITY_TYPE.PROCESS).size(), 1);
        AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.PROCESS, UA1Bundle, Job.Status.RUNNING);
        AssertUtil.checkNotStatus(cluster2OC, ENTITY_TYPE.PROCESS, UA2Bundle, Job.Status.RUNNING);
    }

    @Test(groups = {"prism", "0.2"})
    public void testSnSSuspendedProcessOnBothColos() throws Exception {
        //schedule both bundles
        UA1Bundle.submitAndScheduleProcess();
        UA2Bundle.submitAndScheduleProcess();

        Util.assertSucceeded(cluster2.getProcessHelper()
                .suspend(URLS.SUSPEND_URL, UA1Bundle.getProcessData()));
        AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.PROCESS, UA1Bundle, Job.Status.SUSPENDED);
        AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.PROCESS, UA2Bundle, Job.Status.RUNNING);
        //now check if they have been scheduled correctly or not
        Util.assertSucceeded(prism.getProcessHelper()
                .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, UA1Bundle.getProcessData()));
        Assert.assertEquals(Util.getBundles(cluster2.getFeedHelper().getOozieClient(),
                Util.readEntityName(UA1Bundle.getProcessData()), ENTITY_TYPE.PROCESS).size(), 1);
        Util.assertSucceeded(cluster2.getProcessHelper()
                .resume(URLS.SUSPEND_URL, UA1Bundle.getProcessData()));

        Util.assertSucceeded(cluster1.getProcessHelper()
                .suspend(URLS.SUSPEND_URL, UA2Bundle.getProcessData()));
        Util.assertSucceeded(prism.getProcessHelper()
                .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, UA2Bundle.getProcessData()));

        Assert.assertEquals(Util.getBundles(cluster1.getFeedHelper().getOozieClient(),
                Util.readEntityName(UA2Bundle.getProcessData()), ENTITY_TYPE.PROCESS).size(), 1);
        AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.PROCESS, UA2Bundle, Job.Status.SUSPENDED);
        AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.PROCESS, UA1Bundle, Job.Status.SUSPENDED);
    }

    @Test(groups = {"prism", "0.2"})
    public void testSnSDeletedProcessOnBothColos() throws Exception {
        //schedule both bundles
        UA1_RUNNING = UA1Bundle.getClusterHelper().getColo().split("=")[1] + RUNNING;
        UA2_RUNNING = UA2Bundle.getClusterHelper().getColo().split("=")[1] + RUNNING;
        UA1Bundle.submitAndScheduleProcess();
        Assert.assertEquals(Util.parseResponse(
                prism.getProcessHelper()
                        .getStatus(URLS.STATUS_URL, UA1Bundle.getProcessData())).getMessage(),
                UA1_RUNNING
        );

        UA2Bundle.submitAndScheduleProcess();
        Assert.assertEquals(Util.parseResponse(
                prism.getProcessHelper()
                        .getStatus(URLS.STATUS_URL, UA2Bundle.getProcessData())).getMessage(),
                UA2_RUNNING
        );

        Util.assertSucceeded(
                prism.getProcessHelper().delete(URLS.DELETE_URL, UA1Bundle.getProcessData()));
        AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.PROCESS, UA1Bundle, Job.Status.KILLED);
        AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.PROCESS, UA2Bundle, Job.Status.RUNNING);
        Util.assertSucceeded(
                prism.getProcessHelper().delete(URLS.DELETE_URL, UA2Bundle.getProcessData()));
        AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.PROCESS, UA2Bundle, Job.Status.KILLED);
        AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.PROCESS, UA1Bundle, Job.Status.KILLED);
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
        Assert.assertEquals(Util.parseResponse(cluster2.getProcessHelper()
                .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, UA1Bundle.getProcessData()))
                .getStatusCode(), 404);
        Assert.assertEquals(Util.parseResponse(cluster1.getProcessHelper()
                .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, UA2Bundle.getProcessData()))
                .getStatusCode(), 404);

    }

}
