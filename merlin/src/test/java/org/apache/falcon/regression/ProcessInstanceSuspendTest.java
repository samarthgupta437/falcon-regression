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

package org.apache.falcon.regression;

import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.generated.dependencies.Frequency.TimeUnit;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.helpers.PrismHelper;
import org.apache.falcon.regression.core.response.ProcessInstancesResult;
import org.apache.falcon.regression.core.response.ProcessInstancesResult.WorkflowStatus;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.core.util.Util.URLS;
import org.apache.oozie.client.CoordinatorAction;
import org.joda.time.DateTime;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

public class ProcessInstanceSuspendTest {
    PrismHelper prismHelper = new PrismHelper("prism.properties");
    ColoHelper ivoryqa1 = new ColoHelper("ivoryqa-1.config.properties");

    @BeforeClass(alwaysRun = true)
    public void createTestData() throws Exception {

        Util.print("in @BeforeClass");

        System.setProperty("java.security.krb5.realm", "");
        System.setProperty("java.security.krb5.kdc", "");


        Bundle b = new Bundle();
        b = (Bundle) Util.readELBundles()[0][0];
        b = new Bundle(b, ivoryqa1.getEnvFileName());
        String startDate = "2010-01-01T20:00Z";
        String endDate = "2010-01-03T01:04Z";

        b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
        String prefix = b.getFeedDataPathPrefix();
        Util.HDFSCleanup(ivoryqa1, prefix.substring(1));

        DateTime startDateJoda = new DateTime(InstanceUtil.oozieDateToDate(startDate));
        DateTime endDateJoda = new DateTime(InstanceUtil.oozieDateToDate(endDate));

        List<String> dataDates = Util.getMinuteDatesOnEitherSide(startDateJoda, endDateJoda, 20);

        for (int i = 0; i < dataDates.size(); i++)
            dataDates.set(i, prefix + dataDates.get(i));

        ArrayList<String> dataFolder = new ArrayList<String>();

        for (int i = 0; i < dataDates.size(); i++)
            dataFolder.add(dataDates.get(i));

        InstanceUtil.putDataInFolders(ivoryqa1, dataFolder);

    }


    @BeforeMethod(alwaysRun = true)
    public void testName(Method method) throws Exception {
        Util.print("test name: " + method.getName());
        Util.restartService(ivoryqa1.getClusterHelper());

    }

    @Test(groups = {"singleCluster"})
    public void testProcessInstanceSuspend_largeRange() throws Exception {
        Bundle b = new Bundle();

        try {

            b = (Bundle) Util.readELBundles()[0][0];
            b = new Bundle(b, ivoryqa1.getEnvFileName());
            b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
            b.setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:23Z");
            b.setProcessPeriodicity(5, TimeUnit.minutes);
            b.setProcessPeriodicity(5, TimeUnit.minutes);
            b.setOutputFeedLocationData(
                    "/examples/output-data/aggregator/aggregatedLogs/${YEAR}/${MONTH}/${DAY}/$" +
                            "{HOUR}/${MINUTE}");
            b.setProcessConcurrency(5);
            b.submitAndScheduleBundle(prismHelper);
            Thread.sleep(15000);
            ProcessInstancesResult r = prismHelper.getProcessHelper()
                    .getProcessInstanceStatus(Util.readEntityName(b.getProcessData()),
                            "?start=2010-01-02T01:00Z&end=2010-01-02T01:21Z");
            InstanceUtil.validateResponse(r, 5, 5, 0, 0, 0);
            r = prismHelper.getProcessHelper()
                    .getProcessInstanceSuspend(Util.readEntityName(b.getProcessData()),
                            "?start=2010-01-02T00:00Z&end=2010-01-02T01:30Z");
            r = prismHelper.getProcessHelper()
                    .getProcessInstanceStatus(Util.readEntityName(b.getProcessData()),
                            "?start=2010-01-02T00:00Z&end=2010-01-02T01:30Z");
            InstanceUtil.validateSuccessWithStatusCode(r, 400);
        } finally {
            b.deleteBundle(prismHelper);
        }
    }


    @Test(groups = {"singleCluster"})
    public void testProcessInstanceSuspend_succeeded() throws Exception {
        Bundle b = new Bundle();

        try {

            b = (Bundle) Util.readELBundles()[0][0];
            b = new Bundle(b, ivoryqa1.getEnvFileName());
            b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
            b.setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:04Z");
            b.setProcessPeriodicity(5, TimeUnit.minutes);
            b.setProcessPeriodicity(5, TimeUnit.minutes);
            b.setOutputFeedLocationData(
                    "/examples/output-data/aggregator/aggregatedLogs/${YEAR}/${MONTH}/${DAY}/$" +
                            "{HOUR}/${MINUTE}");
            b.setProcessConcurrency(1);
            b.submitAndScheduleBundle(prismHelper);
            //wait for instance to succeed
            for (int i = 0; i < 30; i++) {
                if (InstanceUtil
                        .getInstanceStatus(ivoryqa1, Util.getProcessName(b.getProcessData()), 0, 0)
                        .equals(CoordinatorAction.Status.SUCCEEDED))
                    break;

                Util.print("still in for loop: testProcessInstanceSuspend_succeeded");
                Thread.sleep(20000);
            }

            ProcessInstancesResult r = prismHelper.getProcessHelper()
                    .getProcessInstanceSuspend(Util.readEntityName(b.getProcessData()),
                            "?start=2010-01-02T01:00Z");
            InstanceUtil.validateSuccessWithStatusCode(r, 0);
        } finally {
            b.deleteBundle(prismHelper);
        }
    }


    @Test(groups = {"singleCluster"})
    public void testProcessInstanceSuspend_all() throws Exception {
        Bundle b = new Bundle();

        try {

            b = (Bundle) Util.readELBundles()[0][0];
            b = new Bundle(b, ivoryqa1.getEnvFileName());
            b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
            b.setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:23Z");
            b.setProcessPeriodicity(5, TimeUnit.minutes);
            b.setProcessPeriodicity(5, TimeUnit.minutes);
            b.setOutputFeedLocationData(
                    "/examples/output-data/aggregator/aggregatedLogs/${YEAR}/${MONTH}/${DAY}/$" +
                            "{HOUR}/${MINUTE}");
            b.setProcessConcurrency(5);
            b.submitAndScheduleBundle(prismHelper);
            Thread.sleep(15000);
            ProcessInstancesResult r = prismHelper.getProcessHelper()
                    .getProcessInstanceStatus(Util.readEntityName(b.getProcessData()),
                            "?start=2010-01-02T01:00Z&end=2010-01-02T01:20Z");
            InstanceUtil.validateResponse(r, 5, 5, 0, 0, 0);
            r = prismHelper.getProcessHelper()
                    .getProcessInstanceSuspend(Util.readEntityName(b.getProcessData()),
                            "?start=2010-01-02T01:00Z&end=2010-01-02T01:20Z");
            r = prismHelper.getProcessHelper()
                    .getProcessInstanceStatus(Util.readEntityName(b.getProcessData()),
                            "?start=2010-01-02T01:00Z&end=2010-01-02T01:20Z");
            InstanceUtil.validateResponse(r, 5, 0, 5, 0, 0);
        } finally {
            b.deleteBundle(prismHelper);

        }
    }


    @Test(groups = {"singleCluster"})
    public void testProcessInstanceSuspend_woParams() throws Exception {
        Bundle b = new Bundle();

        try {

            b = (Bundle) Util.readELBundles()[0][0];
            b = new Bundle(b, ivoryqa1.getEnvFileName());
            b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
            b.setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:22Z");
            b.setProcessPeriodicity(5, TimeUnit.minutes);
            b.setProcessPeriodicity(5, TimeUnit.minutes);
            b.setOutputFeedLocationData(
                    "/examples/output-data/aggregator/aggregatedLogs/${YEAR}/${MONTH}/${DAY}/$" +
                            "{HOUR}/${MINUTE}");
            b.setProcessConcurrency(2);
            b.submitAndScheduleBundle(prismHelper);
            Thread.sleep(15000);
            ProcessInstancesResult r = prismHelper.getProcessHelper()
                    .getProcessInstanceSuspend(Util.readEntityName(b.getProcessData()), null);
            InstanceUtil.validateSuccessWithStatusCode(r, 2);
        } finally {
            b.deleteBundle(prismHelper);

        }
    }


    @Test(groups = {"singleCluster"})
    public void testProcessInstanceSuspend_StartAndEnd() throws Exception {
        Bundle b = new Bundle();

        try {

            b = (Bundle) Util.readELBundles()[0][0];
            b = new Bundle(b, ivoryqa1.getEnvFileName());
            b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
            b.setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:23Z");
            b.setProcessPeriodicity(5, TimeUnit.minutes);
            b.setProcessPeriodicity(5, TimeUnit.minutes);
            b.setOutputFeedLocationData(
                    "/examples/output-data/aggregator/aggregatedLogs/${YEAR}/${MONTH}/${DAY}/$" +
                            "{HOUR}/${MINUTE}");
            b.setProcessConcurrency(3);
            b.submitAndScheduleBundle(prismHelper);
            Thread.sleep(15000);
            ProcessInstancesResult r = prismHelper.getProcessHelper()
                    .getProcessInstanceStatus(Util.readEntityName(b.getProcessData()),
                            "?start=2010-01-02T01:00Z&end=2010-01-02T01:22Z");
            InstanceUtil.validateResponse(r, 5, 3, 0, 2, 0);
            r = prismHelper.getProcessHelper()
                    .getProcessInstanceSuspend(Util.readEntityName(b.getProcessData()),
                            "?start=2010-01-02T01:00Z&end=2010-01-02T01:15Z");
            r = prismHelper.getProcessHelper()
                    .getProcessInstanceStatus(Util.readEntityName(b.getProcessData()),
                            "?start=2010-01-02T01:00Z&end=2010-01-02T01:22Z");
            InstanceUtil.validateResponse(r, 5, 0, 3, 2, 0);
        } finally {
            b.deleteBundle(prismHelper);

        }
    }


    @Test(groups = {"singleCluster"})
    public void testProcessInstanceSuspend_nonExistent() throws Exception {
        Bundle b = new Bundle();

        try {

            b = (Bundle) Util.readELBundles()[0][0];
            b = new Bundle(b, ivoryqa1.getEnvFileName());
            b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
            b.setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:23Z");
            b.setProcessPeriodicity(5, TimeUnit.minutes);
            b.setProcessPeriodicity(5, TimeUnit.minutes);
            b.setOutputFeedLocationData(
                    "/examples/output-data/aggregator/aggregatedLogs/${YEAR}/${MONTH}/${DAY}/$" +
                            "{HOUR}/${MINUTE}");
            b.setProcessConcurrency(5);
            b.submitAndScheduleBundle(prismHelper);
            Thread.sleep(15000);
            ProcessInstancesResult r =
                    prismHelper.getProcessHelper()
                            .getProcessInstanceSuspend("invalidName", "?start=2010-01-02T01:20Z");
            if ((r.getStatusCode() != 777))
                Assert.assertTrue(false);
        } finally {
            b.deleteBundle(prismHelper);

        }
    }


    @Test(groups = {"singleCluster"})
    public void testProcessInstanceSuspend_onlyStart() throws Exception {
        Bundle b = new Bundle();

        try {

            b = (Bundle) Util.readELBundles()[0][0];
            b = new Bundle(b, ivoryqa1.getEnvFileName());
            b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
            b.setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:11Z");
            b.setProcessPeriodicity(5, TimeUnit.minutes);
            b.setProcessPeriodicity(5, TimeUnit.minutes);
            b.setOutputFeedLocationData(
                    "/examples/output-data/aggregator/aggregatedLogs/${YEAR}/${MONTH}/${DAY}/$" +
                            "{HOUR}/${MINUTE}");
            b.setProcessConcurrency(3);
            b.submitAndScheduleBundle(prismHelper);
            Thread.sleep(15000);
            prismHelper.getProcessHelper()
                    .getRunningInstance(URLS.INSTANCE_RUNNING,
                            Util.readEntityName(b.getProcessData()));
            ProcessInstancesResult r = prismHelper.getProcessHelper()
                    .getProcessInstanceSuspend(Util.readEntityName(b.getProcessData()),
                            "?start=2010-01-02T01:00Z");
            InstanceUtil.validateSuccessOnlyStart(r, b, WorkflowStatus.SUSPENDED);
            r = prismHelper.getProcessHelper()
                    .getRunningInstance(URLS.INSTANCE_RUNNING,
                            Util.readEntityName(b.getProcessData()));
        } finally {
            b.deleteBundle(prismHelper);

        }
    }

    @Test(groups = {"singleCluster"})
    public void testProcessInstanceSuspend_suspendLast() throws Exception {
        Bundle b = new Bundle();

        try {

            b = (Bundle) Util.readELBundles()[0][0];
            b = new Bundle(b, ivoryqa1.getEnvFileName());
            b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
            b.setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:23Z");
            b.setProcessPeriodicity(5, TimeUnit.minutes);
            b.setProcessPeriodicity(5, TimeUnit.minutes);
            b.setOutputFeedLocationData(
                    "/examples/output-data/aggregator/aggregatedLogs/${YEAR}/${MONTH}/${DAY}/$" +
                            "{HOUR}/${MINUTE}");
            b.setProcessConcurrency(5);
            b.submitAndScheduleBundle(prismHelper);
            Thread.sleep(15000);
            ProcessInstancesResult r = prismHelper.getProcessHelper()
                    .getProcessInstanceStatus(Util.readEntityName(b.getProcessData()),
                            "?start=2010-01-02T01:00Z&end=2010-01-02T01:20Z");
            InstanceUtil.validateResponse(r, 5, 5, 0, 0, 0);
            r = prismHelper.getProcessHelper()
                    .getProcessInstanceSuspend(Util.readEntityName(b.getProcessData()),
                            "?start=2010-01-02T01:20Z");
            r = prismHelper.getProcessHelper()
                    .getProcessInstanceStatus(Util.readEntityName(b.getProcessData()),
                            "?start=2010-01-02T01:00Z&end=2010-01-02T01:20Z");
            InstanceUtil.validateResponse(r, 5, 4, 1, 0, 0);
        } finally {
            b.deleteBundle(prismHelper);

        }
    }

    @AfterClass(alwaysRun = true)
    public void deleteData() throws Exception {
        Util.print("in @AfterClass");

        System.setProperty("java.security.krb5.realm", "");
        System.setProperty("java.security.krb5.kdc", "");


        Bundle b = new Bundle();
        b = (Bundle) Util.readELBundles()[0][0];
        b = new Bundle(b, ivoryqa1.getEnvFileName());
        b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
        String prefix = b.getFeedDataPathPrefix();
        Util.HDFSCleanup(ivoryqa1, prefix.substring(1));
    }
}
