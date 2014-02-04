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
import org.apache.falcon.regression.core.util.Util.URLS;
import org.apache.falcon.regression.testHelper.BaseMultiClusterTests;
import org.apache.oozie.client.Job;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;

public class PrismProcessSuspendTest extends BaseMultiClusterTests {

    private Bundle bundle1;
    private Bundle bundle2;
    private boolean restartRequired;


    @BeforeMethod(alwaysRun = true)
    public void setUp(Method method) throws Exception {
        Util.print("test name: " + method.getName());
        restartRequired = false;
        Bundle bundle = Util.readBundles("LateDataBundles")[0][0];
        bundle1 = new Bundle(bundle, server1.getEnvFileName(), server1.getPrefix());
        bundle2 = new Bundle(bundle, server2.getEnvFileName(), server2.getPrefix());
        bundle1.generateUniqueBundle();
        bundle2.generateUniqueBundle();

    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws Exception {
        if (restartRequired) {
            Util.restartService(server1.getProcessHelper());
        }
        bundle1.deleteBundle(prism);
        bundle2.deleteBundle(prism);
    }


    @Test(groups = {"prism", "0.2"})
    public void testSuspendSuspendedProcessOnBothColosWhen1ColoIsDown() throws Exception {
        restartRequired = true;

        //schedule using colohelpers
        bundle1.submitAndScheduleProcessUsingColoHelper(server1);
        bundle2.submitAndScheduleProcessUsingColoHelper(server2);


        //suspend using prismHelper
        Util.assertSucceeded(prism.getProcessHelper().suspend(Util.URLS.SUSPEND_URL, bundle1.getProcessData()));
        //verify
        AssertUtil.checkStatus(server1OC, ENTITY_TYPE.PROCESS, bundle1, Job.Status.SUSPENDED);
        AssertUtil.checkStatus(server2OC, ENTITY_TYPE.PROCESS, bundle2, Job.Status.RUNNING);

        Util.shutDownService(server1.getProcessHelper());

        Util.assertFailed(prism.getProcessHelper().suspend(Util.URLS.SUSPEND_URL, bundle1.getProcessData()));

        for (int i = 0; i < 2; i++) {
            //suspend on the other one
            Util.assertSucceeded(prism.getProcessHelper().suspend(Util.URLS.SUSPEND_URL, bundle2.getProcessData()));
            AssertUtil.checkStatus(server1OC, ENTITY_TYPE.PROCESS, bundle1, Job.Status.SUSPENDED);
            AssertUtil.checkStatus(server2OC, ENTITY_TYPE.PROCESS, bundle2, Job.Status.SUSPENDED);
        }
    }


    @Test(groups = {"prism", "0.2"})
    public void testSuspendScheduledProcessOnBothColos() throws Exception {
        //schedule using colohelpers
        bundle1.submitAndScheduleProcessUsingColoHelper(server1);
        bundle2.submitAndScheduleProcessUsingColoHelper(server2);

        //suspend using prismHelper
        Util.assertSucceeded(prism.getProcessHelper().suspend(Util.URLS.SUSPEND_URL, bundle1.getProcessData()));
        //verify
        AssertUtil.checkStatus(server1OC, ENTITY_TYPE.PROCESS, bundle1, Job.Status.SUSPENDED);
        AssertUtil.checkStatus(server2OC, ENTITY_TYPE.PROCESS, bundle2, Job.Status.RUNNING);

        //suspend on the other one
        Util.assertSucceeded(prism.getProcessHelper().suspend(Util.URLS.SUSPEND_URL, bundle1.getProcessData()));
        AssertUtil.checkStatus(server1OC, ENTITY_TYPE.PROCESS, bundle1, Job.Status.SUSPENDED);
        AssertUtil.checkStatus(server2OC, ENTITY_TYPE.PROCESS, bundle2, Job.Status.RUNNING);

        Assert.assertTrue(Util.parseResponse(prism.getProcessHelper()
                .getStatus(URLS.STATUS_URL, bundle1.getProcessData())).getMessage().contains("SUSPENDED"));
        Assert.assertTrue(Util.parseResponse(prism.getProcessHelper()
                .getStatus(URLS.STATUS_URL, bundle2.getProcessData())).getMessage().contains("RUNNING"));
    }

    @Test(groups = {"prism", "0.2"})
    public void testSuspendDeletedProcessOnBothColos() throws Exception {
        //schedule using colohelpers
        bundle1.submitAndScheduleProcessUsingColoHelper(server1);
        bundle2.submitAndScheduleProcessUsingColoHelper(server2);

        //delete using coloHelpers
        Util.assertSucceeded(prism.getProcessHelper().delete(Util.URLS.DELETE_URL, bundle1.getProcessData()));


        //suspend using prismHelper
        Util.assertFailed(prism.getProcessHelper().suspend(Util.URLS.SUSPEND_URL, bundle1.getProcessData()));
        //verify
        AssertUtil.checkStatus(server1OC, ENTITY_TYPE.PROCESS, bundle1, Job.Status.KILLED);
        AssertUtil.checkStatus(server2OC, ENTITY_TYPE.PROCESS, bundle2, Job.Status.RUNNING);

        Util.assertSucceeded(prism.getProcessHelper().delete(Util.URLS.DELETE_URL, bundle2.getProcessData()));
        //suspend on the other one
        Util.assertFailed(prism.getProcessHelper().suspend(Util.URLS.SUSPEND_URL, bundle2.getProcessData()));
        AssertUtil.checkStatus(server1OC, ENTITY_TYPE.PROCESS, bundle1, Job.Status.KILLED);
        AssertUtil.checkStatus(server2OC, ENTITY_TYPE.PROCESS, bundle2, Job.Status.KILLED);
    }

    @Test(groups = {"prism", "0.2"})
    public void testSuspendSuspendedProcessOnBothColos() throws Exception {
        //schedule using colohelpers
        bundle1.submitAndScheduleProcessUsingColoHelper(server1);
        bundle2.submitAndScheduleProcessUsingColoHelper(server2);


        for (int i = 0; i < 2; i++) {
            //suspend using prismHelper
            Util.assertSucceeded(prism.getProcessHelper().suspend(Util.URLS.SUSPEND_URL, bundle1.getProcessData()));
            //verify
            AssertUtil.checkStatus(server1OC, ENTITY_TYPE.PROCESS, bundle1, Job.Status.SUSPENDED);
            AssertUtil.checkStatus(server2OC, ENTITY_TYPE.PROCESS, bundle2, Job.Status.RUNNING);
        }


        for (int i = 0; i < 2; i++) {
            //suspend on the other one
            Util.assertSucceeded(prism.getProcessHelper().suspend(Util.URLS.SUSPEND_URL, bundle2.getProcessData()));
            AssertUtil.checkStatus(server1OC, ENTITY_TYPE.PROCESS, bundle1, Job.Status.SUSPENDED);
            AssertUtil.checkStatus(server2OC, ENTITY_TYPE.PROCESS, bundle2, Job.Status.SUSPENDED);
        }
    }

    @Test()
    public void testSuspendNonExistentProcessOnBothColos() throws Exception {
        Util.assertFailed(prism.getProcessHelper().suspend(Util.URLS.SUSPEND_URL, bundle1.getProcessData()));
        Util.assertFailed(prism.getProcessHelper().suspend(Util.URLS.SUSPEND_URL, bundle2.getProcessData()));

        Util.assertFailed(server1.getProcessHelper().suspend(Util.URLS.SUSPEND_URL, bundle1.getProcessData()));
        Util.assertFailed(server2.getProcessHelper().suspend(Util.URLS.SUSPEND_URL, bundle1.getProcessData()));
    }

    @Test()
    public void testSuspendSubmittedProcessOnBothColos() throws Exception {
        bundle1.submitProcess();
        bundle2.submitProcess();

        Util.assertFailed(prism.getProcessHelper().suspend(Util.URLS.SUSPEND_URL, bundle1.getProcessData()));
        Util.assertFailed(prism.getProcessHelper().suspend(Util.URLS.SUSPEND_URL, bundle2.getProcessData()));

        Util.assertFailed(server1.getProcessHelper().suspend(Util.URLS.SUSPEND_URL, bundle1.getProcessData()));
        Util.assertFailed(server2.getProcessHelper().suspend(Util.URLS.SUSPEND_URL, bundle2.getProcessData()));
    }

    @Test(groups = {"prism", "0.2"})
    public void testSuspendScheduledProcessOnBothColosWhen1ColoIsDown() throws Exception {
        restartRequired = true;
        //schedule using colohelpers
        bundle1.submitAndScheduleProcessUsingColoHelper(server1);
        bundle2.submitAndScheduleProcessUsingColoHelper(server2);

        Util.shutDownService(server1.getProcessHelper());

        //suspend using prismHelper
        Util.assertFailed(prism.getProcessHelper().suspend(Util.URLS.SUSPEND_URL, bundle1.getProcessData()));
        //verify
        AssertUtil.checkStatus(server2OC, ENTITY_TYPE.PROCESS, bundle2, Job.Status.RUNNING);

        //suspend on the other one
        Util.assertSucceeded(prism.getProcessHelper().suspend(Util.URLS.SUSPEND_URL, bundle2.getProcessData()));
        AssertUtil.checkStatus(server2OC, ENTITY_TYPE.PROCESS, bundle2, Job.Status.SUSPENDED);
        AssertUtil.checkStatus(server1OC, ENTITY_TYPE.PROCESS, bundle1, Job.Status.RUNNING);
    }

    @Test(groups = {"prism", "0.2"})
    public void testSuspendDeletedProcessOnBothColosWhen1ColoIsDown() throws Exception {
        restartRequired = true;         //schedule using colohelpers
        bundle1.submitAndScheduleProcessUsingColoHelper(server1);
        bundle2.submitAndScheduleProcessUsingColoHelper(server2);

        //delete using coloHelpers
        Util.assertSucceeded(prism.getProcessHelper().delete(Util.URLS.DELETE_URL, bundle1.getProcessData()));

        Util.shutDownService(server1.getProcessHelper());

        //suspend using prismHelper
        Util.assertFailed(prism.getProcessHelper().suspend(Util.URLS.SUSPEND_URL, bundle1.getProcessData()));
        //verify
        AssertUtil.checkStatus(server1OC, ENTITY_TYPE.PROCESS, bundle1, Job.Status.KILLED);
        AssertUtil.checkStatus(server2OC, ENTITY_TYPE.PROCESS, bundle2, Job.Status.RUNNING);

        Util.assertSucceeded(prism.getProcessHelper().delete(Util.URLS.DELETE_URL, bundle2.getProcessData()));
        //suspend on the other one
        Util.assertFailed(prism.getProcessHelper().suspend(Util.URLS.SUSPEND_URL, bundle2.getProcessData()));
        AssertUtil.checkStatus(server1OC, ENTITY_TYPE.PROCESS, bundle1, Job.Status.KILLED);
        AssertUtil.checkStatus(server2OC, ENTITY_TYPE.PROCESS, bundle2, Job.Status.KILLED);
    }


    @Test()
    public void testSuspendNonExistentProcessOnBothColosWhen1ColoIsDown() throws Exception {
        restartRequired = true;
        Util.shutDownService(server1.getProcessHelper());

        Util.assertFailed(prism.getProcessHelper().suspend(Util.URLS.SUSPEND_URL, bundle2.getProcessData()));
        Util.assertFailed(prism.getProcessHelper().suspend(Util.URLS.SUSPEND_URL, bundle1.getProcessData()));

        Util.assertFailed(server2.getProcessHelper().suspend(Util.URLS.SUSPEND_URL, bundle1.getProcessData()));
    }

    @Test()
    public void testSuspendSubmittedFeedOnBothColosWhen1ColoIsDown() throws Exception {
        restartRequired = true;
        bundle1.submitProcess();
        bundle2.submitProcess();

        Util.shutDownService(server1.getProcessHelper());

        Util.assertFailed(prism.getProcessHelper().suspend(Util.URLS.SUSPEND_URL, bundle1.getProcessData()));
        Util.assertFailed(prism.getProcessHelper().suspend(Util.URLS.SUSPEND_URL, bundle2.getProcessData()));
        Util.assertFailed(server2.getProcessHelper().suspend(Util.URLS.SUSPEND_URL, bundle2.getProcessData()));
    }
}
