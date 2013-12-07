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
import org.testng.AssertJUnit;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

public class ProcessInstanceRunningTest {

    PrismHelper prismHelper = new PrismHelper("prism.properties");
    ColoHelper ivoryqa1 = new ColoHelper("gs1001.config.properties");

    @BeforeClass(alwaysRun = true)
    public void createTestData() throws Exception {

        Util.print("in @BeforeClass");

        System.setProperty("java.security.krb5.realm", "");
        System.setProperty("java.security.krb5.kdc", "");


        Bundle b = new Bundle();
        b = (Bundle) Util.readELBundles()[0][0];
        b.generateUniqueBundle();
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
    public void testName(Method method) {
        Util.print("test name: " + method.getName());
    }

    @Test(groups = {"singleCluster"})
    public void getResumedProcessInstance() throws Exception {
        Bundle b = new Bundle();

        try {

            b = (Bundle) Util.readELBundles()[0][0];
            b = new Bundle(b, ivoryqa1.getEnvFileName());
            b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
            b.setProcessValidity("2010-01-02T01:00Z", "2010-01-02T02:30Z");
            b.setProcessPeriodicity(5, TimeUnit.minutes);
            b.setOutputFeedPeriodicity(5, TimeUnit.minutes);
            b.setOutputFeedLocationData(
                    "/examples/output-data/aggregator/aggregatedLogs/${YEAR}/${MONTH}/${DAY}/$" +
                            "{HOUR}/${MINUTE}");
            b.setProcessConcurrency(3);
            b.submitAndScheduleBundle(prismHelper);
            Thread.sleep(15000);
            prismHelper.getProcessHelper().suspend(URLS.SUSPEND_URL, b.getProcessData());
            Thread.sleep(15000);
            prismHelper.getProcessHelper().resume(URLS.RESUME_URL, b.getProcessData());
            Thread.sleep(15000);
            ProcessInstancesResult r = prismHelper.getProcessHelper()
                    .getRunningInstance(URLS.INSTANCE_RUNNING,
                            Util.readEntityName(b.getProcessData()));
            InstanceUtil.validateSuccess(r, b, WorkflowStatus.RUNNING);
        } finally {
            b.deleteBundle(prismHelper);
        }
    }


    @Test(groups = {"singleCluster"})
    public void getSuspendedProcessInstance() throws Exception {
        Bundle b = new Bundle();

        try {

            b = (Bundle) Util.readELBundles()[0][0];
            b = new Bundle(b, ivoryqa1.getEnvFileName());
            b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
            b.setProcessValidity("2010-01-02T01:00Z", "2010-01-02T02:30Z");
            b.setProcessPeriodicity(5, TimeUnit.minutes);
            b.setOutputFeedPeriodicity(5, TimeUnit.minutes);
            b.setOutputFeedLocationData(
                    "/examples/output-data/aggregator/aggregatedLogs/${YEAR}/${MONTH}/${DAY}/$" +
                            "{HOUR}/${MINUTE}");
            b.setProcessConcurrency(3);
            b.submitAndScheduleBundle(prismHelper);
            prismHelper.getProcessHelper().suspend(URLS.SUSPEND_URL, b.getProcessData());
            Thread.sleep(5000);
            ProcessInstancesResult r = prismHelper.getProcessHelper()
                    .getRunningInstance(URLS.INSTANCE_RUNNING,
                            Util.readEntityName(b.getProcessData()));
            InstanceUtil.validateSuccessWOInstances(r);
        } finally {
            b.deleteBundle(prismHelper);
        }
    }


    @Test(groups = {"singleCluster"})
    public void getRunningProcessInstance() throws Exception {
        Bundle b = new Bundle();
        try {

            b = (Bundle) Util.readELBundles()[0][0];
            b = new Bundle(b, ivoryqa1.getEnvFileName());
            b = new Bundle(b, ivoryqa1.getEnvFileName());
            b.setCLusterColo("ua2");
            b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
            b.setProcessValidity("2010-01-02T01:00Z", "2010-01-02T02:30Z");
            b.setProcessPeriodicity(5, TimeUnit.minutes);
            b.submitAndScheduleBundle(prismHelper);
            Thread.sleep(5000);
            ProcessInstancesResult r = prismHelper.getProcessHelper()
                    .getRunningInstance(URLS.INSTANCE_RUNNING,
                            Util.readEntityName(b.getProcessData()));
            InstanceUtil.validateSuccess(r, b, WorkflowStatus.RUNNING);
        } finally {
            b.deleteBundle(prismHelper);
        }
    }

    @Test(groups = {"singleCluster"})
    public void getNonExistenceProcessInstance() throws Exception {
        ProcessInstancesResult r =
                prismHelper.getProcessHelper()
                        .getRunningInstance(URLS.INSTANCE_RUNNING, "invalidName");
        if (!(r.getStatusCode() == 777))
            AssertJUnit.assertTrue(false);
    }


    @Test(groups = {"singleCluster"})
    public void getKilledProcessInstance() throws Exception {
        Bundle b = new Bundle();

        try {

            b = (Bundle) Util.readELBundles()[0][0];
            b = new Bundle(b, ivoryqa1.getEnvFileName());
            b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
            b.submitAndScheduleBundle(prismHelper);
            prismHelper.getProcessHelper().delete(URLS.DELETE_URL, b.getProcessData());
            Thread.sleep(5000);
            ProcessInstancesResult r = prismHelper.getProcessHelper()
                    .getRunningInstance(URLS.INSTANCE_RUNNING,
                            Util.readEntityName(b.getProcessData()));
            if (!(r.getStatusCode() == 777))
                AssertJUnit.assertTrue(false);
        } finally {
            b.deleteBundle(prismHelper);
        }
    }


    @Test(groups = {"singleCluster"})
    public void getSucceededProcessInstance() throws Exception {
        Bundle b = new Bundle();

        try {

            b = (Bundle) Util.readELBundles()[0][0];
            b = new Bundle(b, ivoryqa1.getEnvFileName());
            b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
            b.setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:11Z");
            b.submitAndScheduleBundle(prismHelper);
            org.apache.oozie.client.Job.Status s = null;
            for (int i = 0; i < 45; i++) {
                s = InstanceUtil.getDefaultCoordinatorStatus(ivoryqa1,
                        Util.getProcessName(b.getProcessData()), 0);
                if (s.equals(org.apache.oozie.client.Job.Status.SUCCEEDED))
                    break;
                Thread.sleep(45000);
            }

            if (!s.equals(org.apache.oozie.client.Job.Status.SUCCEEDED))
                Assert.assertTrue(false, "The job did not succeeded even in long time");

            ProcessInstancesResult r = prismHelper.getProcessHelper()
                    .getRunningInstance(URLS.INSTANCE_RUNNING,
                            Util.readEntityName(b.getProcessData()));
            InstanceUtil.validateSuccessWOInstances(r);
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
