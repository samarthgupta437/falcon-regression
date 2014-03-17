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
import org.apache.falcon.regression.core.response.ServiceResponse;
import org.apache.falcon.regression.core.enumsAndConstants.ENTITY_TYPE;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.core.util.Util.URLS;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.OozieClient;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;

@Test(groups = "distributed")
public class PrismProcessSnSTest extends BaseTestClass {

    ColoHelper cluster1 = servers.get(0);
    ColoHelper cluster2 = servers.get(1);
    OozieClient cluster1OC = serverOC.get(0);
    OozieClient cluster2OC = serverOC.get(1);
    public static String RUNNING = "/RUNNING";
    public static String CLUSTER1_RUNNING;
    public static String CLUSTER2_RUNNING;


    @BeforeMethod(alwaysRun = true)
    public void setUp(Method method) throws Exception {
        Util.print("test name: " + method.getName());
        Bundle bundle = Util.readBundles("LateDataBundles")[0][0];
        bundles[0] = new Bundle(bundle, cluster2.getEnvFileName(), cluster2.getPrefix());
        bundles[1] = new Bundle(bundle, cluster1.getEnvFileName(), cluster1.getPrefix());
        bundles[0].generateUniqueBundle();
        bundles[1].generateUniqueBundle();
    }

    @AfterMethod
    public void tearDown() throws Exception {
        removeBundles();
    }

    @Test(groups = {"prism", "0.2"})
    public void testProcessSnSOnBothColos() throws Exception {
        //schedule both bundles
        bundles[0].submitAndScheduleProcess();
        AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.PROCESS, bundles[0], Job.Status.RUNNING);
        AssertUtil.checkNotStatus(cluster2OC, ENTITY_TYPE.PROCESS, bundles[1], Job.Status.RUNNING);
        bundles[1].submitAndScheduleProcess();

        //now check if they have been scheduled correctly or not
        AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.PROCESS, bundles[1], Job.Status.RUNNING);
        //check if there is no criss cross
        ServiceResponse response =
                prism.getProcessHelper()
                        .getStatus(URLS.STATUS_URL, bundles[1].getProcessData());
        System.out.println(response.getMessage());
        AssertUtil.checkNotStatus(cluster1OC, ENTITY_TYPE.PROCESS, bundles[0], Job.Status.RUNNING);

    }

    @Test(groups = {"prism", "0.2"})
    public void testProcessSnSForSubmittedProcessOnBothColos() throws Exception {
        //schedule both bundles

        bundles[0].submitProcess(true);

        Util.assertSucceeded(prism.getProcessHelper()
                .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, bundles[0].getProcessData()));
        AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.PROCESS, bundles[0], Job.Status.RUNNING);
        AssertUtil.checkNotStatus(cluster2OC, ENTITY_TYPE.PROCESS, bundles[1], Job.Status.RUNNING);

        bundles[1].submitProcess(true);

        Util.assertSucceeded(prism.getProcessHelper()
                .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, bundles[0].getProcessData()));
        //now check if they have been scheduled correctly or not
        AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.PROCESS, bundles[0], Job.Status.RUNNING);
        //check if there is no criss cross
        AssertUtil.checkNotStatus(cluster1OC, ENTITY_TYPE.PROCESS, bundles[0], Job.Status.RUNNING);

    }

    @Test(groups = {"prism", "0.2"})
    public void testProcessSnSForSubmittedProcessOnBothColosUsingColoHelper()
    throws Exception {
        //schedule both bundles

        bundles[0].submitProcess(true);

        Util.assertSucceeded(prism.getProcessHelper()
                .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, bundles[0].getProcessData()));
        AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.PROCESS, bundles[0], Job.Status.RUNNING);
        AssertUtil.checkNotStatus(cluster2OC, ENTITY_TYPE.PROCESS, bundles[1], Job.Status.RUNNING);
        bundles[1].submitProcess(true);
        AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.PROCESS, bundles[0], Job.Status.RUNNING);
        AssertUtil.checkNotStatus(cluster2OC, ENTITY_TYPE.PROCESS, bundles[1], Job.Status.RUNNING);
        bundles[1].submitProcess(true);

        Util.assertSucceeded(prism.getProcessHelper()
                .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, bundles[0].getProcessData()));
        //now check if they have been scheduled correctly or not
        AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.PROCESS, bundles[0], Job.Status.RUNNING);
        //check if there is no criss cross
        AssertUtil.checkNotStatus(cluster1OC, ENTITY_TYPE.PROCESS, bundles[0], Job.Status.RUNNING);

    }

    @Test(groups = {"prism", "0.2"})
    public void testProcessSnSAlreadyScheduledOnBothColos() throws Exception {
        //schedule both bundles
        bundles[0].submitAndScheduleProcess();
        AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.PROCESS, bundles[0], Job.Status.RUNNING);
        AssertUtil.checkNotStatus(cluster2OC, ENTITY_TYPE.PROCESS, bundles[1], Job.Status.RUNNING);
        bundles[1].submitAndScheduleProcess();

        //now check if they have been scheduled correctly or not
        AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.PROCESS, bundles[1], Job.Status.RUNNING);
        //check if there is no criss cross
        AssertUtil.checkNotStatus(cluster1OC, ENTITY_TYPE.PROCESS, bundles[0], Job.Status.RUNNING);

        //reschedule trial

        Util.assertSucceeded(cluster2.getProcessHelper()
                .schedule(URLS.SCHEDULE_URL, bundles[0].getProcessData()));
        Assert.assertEquals(Util.getBundles(cluster2.getFeedHelper().getOozieClient(),
                Util.readEntityName(bundles[0].getProcessData()), ENTITY_TYPE.PROCESS).size(), 1);
        AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.PROCESS, bundles[0], Job.Status.RUNNING);
        AssertUtil.checkNotStatus(cluster2OC, ENTITY_TYPE.PROCESS, bundles[1], Job.Status.RUNNING);
    }

    @Test(groups = {"prism", "0.2"})
    public void testSnSSuspendedProcessOnBothColos() throws Exception {
        //schedule both bundles
        bundles[0].submitAndScheduleProcess();
        bundles[1].submitAndScheduleProcess();

        Util.assertSucceeded(cluster2.getProcessHelper()
                .suspend(URLS.SUSPEND_URL, bundles[0].getProcessData()));
        AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.PROCESS, bundles[0], Job.Status.SUSPENDED);
        AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.PROCESS, bundles[1], Job.Status.RUNNING);
        //now check if they have been scheduled correctly or not
        Util.assertSucceeded(prism.getProcessHelper()
                .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, bundles[0].getProcessData()));
        Assert.assertEquals(Util.getBundles(cluster2.getFeedHelper().getOozieClient(),
                Util.readEntityName(bundles[0].getProcessData()), ENTITY_TYPE.PROCESS).size(), 1);
        Util.assertSucceeded(cluster2.getProcessHelper()
                .resume(URLS.SUSPEND_URL, bundles[0].getProcessData()));

        Util.assertSucceeded(cluster1.getProcessHelper()
                .suspend(URLS.SUSPEND_URL, bundles[1].getProcessData()));
        Util.assertSucceeded(prism.getProcessHelper()
                .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, bundles[1].getProcessData()));

        Assert.assertEquals(Util.getBundles(cluster1.getFeedHelper().getOozieClient(),
                Util.readEntityName(bundles[1].getProcessData()), ENTITY_TYPE.PROCESS).size(), 1);
        AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.PROCESS, bundles[1], Job.Status.SUSPENDED);
        AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.PROCESS, bundles[0], Job.Status.SUSPENDED);
    }

    @Test(groups = {"prism", "0.2"})
    public void testSnSDeletedProcessOnBothColos() throws Exception {
        //schedule both bundles
        CLUSTER1_RUNNING = bundles[0].getClusterHelper().getColo().split("=")[1] + RUNNING;
        CLUSTER2_RUNNING = bundles[1].getClusterHelper().getColo().split("=")[1] + RUNNING;
        bundles[0].submitAndScheduleProcess();

        Assert.assertEquals(Util.parseResponse(
                prism.getProcessHelper()
                        .getStatus(URLS.STATUS_URL, bundles[0].getProcessData())).getMessage(),
                CLUSTER1_RUNNING
        );

        bundles[1].submitAndScheduleProcess();
        Assert.assertEquals(Util.parseResponse(
                prism.getProcessHelper()
                        .getStatus(URLS.STATUS_URL, bundles[1].getProcessData())).getMessage(),
                CLUSTER2_RUNNING
        );

        Util.assertSucceeded(
                prism.getProcessHelper().delete(URLS.DELETE_URL, bundles[0].getProcessData()));
        AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.PROCESS, bundles[0], Job.Status.KILLED);
        AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.PROCESS, bundles[1], Job.Status.RUNNING);
        Util.assertSucceeded(
                prism.getProcessHelper().delete(URLS.DELETE_URL, bundles[1].getProcessData()));
        AssertUtil.checkStatus(cluster1OC, ENTITY_TYPE.PROCESS, bundles[1], Job.Status.KILLED);
        AssertUtil.checkStatus(cluster2OC, ENTITY_TYPE.PROCESS, bundles[0], Job.Status.KILLED);
        Util.assertSucceeded(prism.getProcessHelper()
                .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, bundles[0].getProcessData()));
        Util.assertSucceeded(prism.getProcessHelper()
                .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, bundles[1].getProcessData()));

        Assert.assertEquals(Util.parseResponse(
                prism.getProcessHelper()
                        .getStatus(URLS.STATUS_URL, bundles[0].getProcessData())
        ).getMessage(),
                CLUSTER1_RUNNING
        );
        Assert.assertEquals(Util.parseResponse(
                prism.getProcessHelper()
                        .getStatus(URLS.STATUS_URL, bundles[1].getProcessData())
        ).getMessage(),
                CLUSTER2_RUNNING
        );

    }

    @Test(groups = {"prism", "0.2"})
    public void testScheduleNonExistentProcessOnBothColos() throws Exception {
        Assert.assertEquals(Util.parseResponse(cluster2.getProcessHelper()
                .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, bundles[0].getProcessData()))
                .getStatusCode(), 404);
        Assert.assertEquals(Util.parseResponse(cluster1.getProcessHelper()
                .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, bundles[1].getProcessData()))
                .getStatusCode(), 404);

    }

}
