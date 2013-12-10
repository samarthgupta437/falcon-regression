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
import org.joda.time.DateTime;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

public class ProcessInstanceResumeTest {

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
        //Util.restartService(ivoryqa1.getClusterHelper());

    }


    @Test(groups = {"singleCluster"})
    public void testProcessInstanceResume_onlyEnd() throws Exception {
        Bundle b = new Bundle();

        try {

            b = (Bundle) Util.readELBundles()[0][0];
            b = new Bundle(b, ivoryqa1.getEnvFileName());
            b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
            b.setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:26Z");
            b.setProcessPeriodicity(5, TimeUnit.minutes);
            b.setOutputFeedPeriodicity(5, TimeUnit.minutes);
            b.setOutputFeedLocationData(
                    "/examples/output-data/aggregator/aggregatedLogs/${YEAR}/${MONTH}/${DAY}/$" +
                            "{HOUR}/${MINUTE}");
            b.setProcessConcurrency(6);
            b.submitAndScheduleBundle(prismHelper);
            Thread.sleep(15000);
            ProcessInstancesResult r = prismHelper.getProcessHelper()
                    .getProcessInstanceSuspend(Util.readEntityName(b.getProcessData()),
                            "?start=2010-01-02T01:05Z&end=2010-01-02T01:21Z");
            Thread.sleep(10000);
            r = prismHelper.getProcessHelper()
                    .getProcessInstanceStatus(Util.readEntityName(b.getProcessData()),
                            "?start=2010-01-02T01:00Z&end=2010-01-02T01:26Z");
            InstanceUtil.validateResponse(r, 6, 2, 4, 0, 0);
            r = prismHelper.getProcessHelper()
                    .getProcessInstanceResume(Util.readEntityName(b.getProcessData()),
                            "?end=2010-01-02T01:15Z");
            Assert.assertTrue(r.getMessage().contains("Parameter start is empty"),
                    "start instance was not mentioned, RESUME should have fialed");
            //instanceUtil.validateSuccessWithStatusCode(r, 2);
        } finally {
            b.deleteBundle(prismHelper);
        }
    }


    @Test(groups = {"singleCluster"})
    public void testProcessInstanceResume_resumeSome() throws Exception {
        Bundle b = new Bundle();

        try {

            b = (Bundle) Util.readELBundles()[0][0];
            b = new Bundle(b, ivoryqa1.getEnvFileName());
            b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
            b.setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:26Z");
            b.setProcessPeriodicity(5, TimeUnit.minutes);
            b.setOutputFeedPeriodicity(5, TimeUnit.minutes);
            b.setOutputFeedLocationData(
                    "/examples/output-data/aggregator/aggregatedLogs/${YEAR}/${MONTH}/${DAY}/$" +
                            "{HOUR}/${MINUTE}");
            b.setProcessConcurrency(6);
            b.submitAndScheduleBundle(prismHelper);
            Thread.sleep(15000);
            ProcessInstancesResult r = prismHelper.getProcessHelper()
                    .getProcessInstanceSuspend(Util.readEntityName(b.getProcessData()),
                            "?start=2010-01-02T01:05Z&end=2010-01-02T01:21Z");
            Thread.sleep(10000);
            r = prismHelper.getProcessHelper()
                    .getProcessInstanceStatus(Util.readEntityName(b.getProcessData()),
                            "?start=2010-01-02T01:00Z&end=2010-01-02T01:26Z");
            InstanceUtil.validateResponse(r, 6, 2, 4, 0, 0);
            prismHelper.getProcessHelper()
                    .getProcessInstanceResume(Util.readEntityName(b.getProcessData()),
                            "?start=2010-01-02T01:05Z&end=2010-01-02T01:16Z");
            r = prismHelper.getProcessHelper()
                    .getProcessInstanceStatus(Util.readEntityName(b.getProcessData()),
                            "?start=2010-01-02T01:00Z&end=2010-01-02T01:26Z");
            InstanceUtil.validateResponse(r, 6, 5, 1, 0, 0);
        } finally {
            b.deleteBundle(prismHelper);
        }
    }


    @Test(groups = {"singleCluster"})
    public void testProcessInstanceResume_resumeMany() throws Exception {
        Bundle b = new Bundle();

        try {

            b = (Bundle) Util.readELBundles()[0][0];
            b = new Bundle(b, ivoryqa1.getEnvFileName());
            b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
            b.setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:26Z");
            b.setProcessPeriodicity(5, TimeUnit.minutes);
            b.setOutputFeedPeriodicity(5, TimeUnit.minutes);
            b.setOutputFeedLocationData(
                    "/examples/output-data/aggregator/aggregatedLogs/${YEAR}/${MONTH}/${DAY}/$" +
                            "{HOUR}/${MINUTE}");
            b.setProcessConcurrency(6);
            b.submitAndScheduleBundle(prismHelper);
            Thread.sleep(15000);
            ProcessInstancesResult r = prismHelper.getProcessHelper()
                    .getProcessInstanceSuspend(Util.readEntityName(b.getProcessData()),
                            "?start=2010-01-02T01:05Z&end=2010-01-02T01:20Z");
            Thread.sleep(10000);
            r = prismHelper.getProcessHelper()
                    .getProcessInstanceStatus(Util.readEntityName(b.getProcessData()),
                            "?start=2010-01-02T01:00Z&end=2010-01-02T01:26Z");
            InstanceUtil.validateResponse(r, 6, 2, 4, 0, 0);
            prismHelper.getProcessHelper()
                    .getProcessInstanceResume(Util.readEntityName(b.getProcessData()),
                            "?start=2010-01-02T01:05Z&end=2010-01-02T01:20Z");
            r = prismHelper.getProcessHelper()
                    .getProcessInstanceStatus(Util.readEntityName(b.getProcessData()),
                            "?start=2010-01-02T01:00Z&end=2010-01-02T01:26Z");
            InstanceUtil.validateResponse(r, 6, 6, 0, 0, 0);
        } finally {
            b.deleteBundle(prismHelper);
        }
    }


    @Test(groups = {"singleCluster"})
    public void testProcessInstanceResume_single() throws Exception {
        Bundle b = new Bundle();
        try {

            b = (Bundle) Util.readELBundles()[0][0];
            b = new Bundle(b, ivoryqa1.getEnvFileName());
            b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
            b.setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:04Z");
            b.setProcessPeriodicity(5, TimeUnit.minutes);
            b.setOutputFeedPeriodicity(5, TimeUnit.minutes);
            b.setOutputFeedLocationData(
                    "/examples/output-data/aggregator/aggregatedLogs/${YEAR}/${MONTH}/${DAY}/$" +
                            "{HOUR}/${MINUTE}");
            b.setProcessConcurrency(1);
            b.submitAndScheduleBundle(prismHelper);
            Thread.sleep(5000);
            prismHelper.getProcessHelper()
                    .getProcessInstanceSuspend(Util.readEntityName(b.getProcessData()),
                            "?start=2010-01-02T01:00Z");
            Thread.sleep(5000);
            prismHelper.getProcessHelper()
                    .getProcessInstanceResume(Util.readEntityName(b.getProcessData()),
                            "?start=2010-01-02T01:00Z");
            Thread.sleep(5000);
            ProcessInstancesResult r = prismHelper.getProcessHelper()
                    .getProcessInstanceStatus(Util.readEntityName(b.getProcessData()),
                            "?start=2010-01-02T01:00Z");
            InstanceUtil.validateSuccessOnlyStart(r, WorkflowStatus.RUNNING);
        } finally {
            b.deleteBundle(prismHelper);
        }
    }


    @Test(groups = {"singleCluster"})
    public void testProcessInstanceResume_nonExistent() throws Exception {
        Bundle b = new Bundle();

        try {

            b = (Bundle) Util.readELBundles()[0][0];
            b = new Bundle(b, ivoryqa1.getEnvFileName());
            b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
            b.setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:26Z");
            b.setProcessPeriodicity(5, TimeUnit.minutes);
            b.setOutputFeedPeriodicity(5, TimeUnit.minutes);
            b.setOutputFeedLocationData(
                    "/examples/output-data/aggregator/aggregatedLogs/${YEAR}/${MONTH}/${DAY}/$" +
                            "{HOUR}/${MINUTE}");
            b.setProcessConcurrency(6);
            b.submitAndScheduleBundle(prismHelper);
            Thread.sleep(15000);
            ProcessInstancesResult r =
                    prismHelper.getProcessHelper()
                            .getProcessInstanceResume("invalidName", "?end=2010-01-02T01:15Z");
            InstanceUtil.validateSuccessWithStatusCode(r, 777);
        } finally {
            b.deleteBundle(prismHelper);
        }
    }


    @Test(groups = {"singleCluster"})
    public void testProcessInstanceResume_noParams() throws Exception {
        Bundle b = new Bundle();

        try {

            b = (Bundle) Util.readELBundles()[0][0];
            b = new Bundle(b, ivoryqa1.getEnvFileName());
            b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
            b.setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:26Z");
            b.setProcessPeriodicity(5, TimeUnit.minutes);
            b.setOutputFeedPeriodicity(5, TimeUnit.minutes);
            b.setOutputFeedLocationData(
                    "/examples/output-data/aggregator/aggregatedLogs/${YEAR}/${MONTH}/${DAY}/$" +
                            "{HOUR}/${MINUTE}");
            b.setProcessConcurrency(6);
            b.submitAndScheduleBundle(prismHelper);
            Thread.sleep(15000);
            ProcessInstancesResult r =
                    prismHelper.getProcessHelper().getProcessInstanceResume("invalidName", null);
            InstanceUtil.validateSuccessWithStatusCode(r, 777);
        } finally {
            b.deleteBundle(prismHelper);
        }
    }


    @Test(groups = {"singleCluster"})
    public void testProcessInstanceResume_deleted() throws Exception {
        Bundle b = new Bundle();

        try {

            b = (Bundle) Util.readELBundles()[0][0];
            b = new Bundle(b, ivoryqa1.getEnvFileName());
            b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
            b.setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:26Z");
            b.setProcessPeriodicity(5, TimeUnit.minutes);
            b.setOutputFeedPeriodicity(5, TimeUnit.minutes);
            b.setOutputFeedLocationData(
                    "/examples/output-data/aggregator/aggregatedLogs/${YEAR}/${MONTH}/${DAY}/$" +
                            "{HOUR}/${MINUTE}");
            b.setProcessConcurrency(6);
            b.submitAndScheduleBundle(prismHelper);
            Thread.sleep(15000);
            prismHelper.getProcessHelper().delete(URLS.DELETE_URL, b.getProcessData());
            Thread.sleep(5000);
            ProcessInstancesResult r = prismHelper.getProcessHelper()
                    .getProcessInstanceResume(Util.readEntityName(b.getProcessData()),
                            "?start=2010-01-02T01:05Z");
            InstanceUtil.validateSuccessWithStatusCode(r, 777);

        } finally {
            b.deleteBundle(prismHelper);
        }
    }


    @Test(groups = {"singleCluster"})
    public void testProcessInstanceResume_nonSuspended() throws Exception {
        Bundle b = new Bundle();

        try {

            b = (Bundle) Util.readELBundles()[0][0];
            b = new Bundle(b, ivoryqa1.getEnvFileName());
            b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
            b.setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:26Z");
            b.setProcessPeriodicity(5, TimeUnit.minutes);
            b.setOutputFeedPeriodicity(5, TimeUnit.minutes);
            b.setOutputFeedLocationData(
                    "/examples/output-data/aggregator/aggregatedLogs/${YEAR}/${MONTH}/${DAY}/$" +
                            "{HOUR}/${MINUTE}");
            b.setProcessConcurrency(6);
            b.submitAndScheduleBundle(prismHelper);
            Thread.sleep(15000);
            ProcessInstancesResult r = prismHelper.getProcessHelper()
                    .getProcessInstanceResume(Util.readEntityName(b.getProcessData()),
                            "?start=2010-01-02T01:05Z");
            Assert.assertTrue(true);
        } finally {
            b.deleteBundle(prismHelper);
        }
    }


    @Test(groups = {"singleCluster"})
    public void testProcessInstanceResume_lastInstance() throws Exception {
        Bundle b = new Bundle();

        try {

            b = (Bundle) Util.readELBundles()[0][0];
            b = new Bundle(b, ivoryqa1.getEnvFileName());
            b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
            b.setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:26Z");
            b.setProcessPeriodicity(5, TimeUnit.minutes);
            b.setOutputFeedPeriodicity(5, TimeUnit.minutes);
            b.setOutputFeedLocationData(
                    "/examples/output-data/aggregator/aggregatedLogs/${YEAR}/${MONTH}/${DAY}/$" +
                            "{HOUR}/${MINUTE}");
            b.setProcessConcurrency(6);
            b.submitAndScheduleBundle(prismHelper);
            Thread.sleep(15000);
            prismHelper.getProcessHelper()
                    .getProcessInstanceSuspend(Util.readEntityName(b.getProcessData()),
                            "?start=2010-01-02T01:25Z");
            Thread.sleep(10000);
            ProcessInstancesResult r = prismHelper.getProcessHelper()
                    .getProcessInstanceResume(Util.readEntityName(b.getProcessData()),
                            "?start=2010-01-02T01:25Z");
            r = prismHelper.getProcessHelper()
                    .getProcessInstanceStatus(Util.readEntityName(b.getProcessData()),
                            "?start=2010-01-02T01:00Z&end=2010-01-02T01:25Z");
            InstanceUtil.validateResponse(r, 6, 6, 0, 0, 0);
        } finally {
            b.deleteBundle(prismHelper);
        }
    }

    @Test(groups = {"singleCluster"})
    public void ap() throws Exception {
        Bundle b = new Bundle();

        try {

            b = (Bundle) Util.readELBundles()[0][0];
            b = new Bundle(b, ivoryqa1.getEnvFileName());
            b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
            b.setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:26Z");
            b.setProcessPeriodicity(5, TimeUnit.minutes);
            b.setOutputFeedPeriodicity(5, TimeUnit.minutes);
            b.setOutputFeedLocationData(
                    "/examples/output-data/aggregator/aggregatedLogs/${YEAR}/${MONTH}/${DAY}/$" +
                            "{HOUR}/${MINUTE}");
            b.setProcessConcurrency(6);
            b.submitAndScheduleBundle(prismHelper);
            Thread.sleep(15000);
            ProcessInstancesResult r = prismHelper.getProcessHelper()
                    .getProcessInstanceSuspend(Util.readEntityName(b.getProcessData()),
                            "?start=2010-01-02T01:05Z&end=2010-01-02T01:20Z");
            Thread.sleep(10000);
            r = prismHelper.getProcessHelper()
                    .getProcessInstanceStatus(Util.readEntityName(b.getProcessData()),
                            "?start=2010-01-02T01:00Z&end=2010-01-02T01:26Z");
            InstanceUtil.validateResponse(r, 6, 2, 4, 0, 0);
            prismHelper.getProcessHelper()
                    .getProcessInstanceResume(Util.readEntityName(b.getProcessData()),
                            "?start=2010-01-02T01:05Z&end=2010-01-02T01:20Z");
            r = prismHelper.getProcessHelper()
                    .getProcessInstanceStatus(Util.readEntityName(b.getProcessData()),
                            "?start=2010-01-02T01:00Z&end=2010-01-02T01:26Z");
            InstanceUtil.validateResponse(r, 6, 6, 0, 0, 0);

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
