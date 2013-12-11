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
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

/**
 * Process instance status tests.
 */
public class ProcessInstanceStatusTest {

    private final PrismHelper prismHelper = new PrismHelper("prism.properties");
    private final ColoHelper ivoryqa1 = new ColoHelper("ivoryqa-1.config.properties");

    @BeforeClass(alwaysRun = true)
    public void createTestData() throws Exception {

        Util.print("in @BeforeClass");

        System.setProperty("java.security.krb5.realm", "");
        System.setProperty("java.security.krb5.kdc", "");


        Bundle b = (Bundle) Util.readELBundles()[0][0];
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

        for (String dataDate : dataDates) {
            dataFolder.add(dataDate);
        }

        InstanceUtil.putDataInFolders(ivoryqa1, dataFolder);
    }


    @BeforeMethod(alwaysRun = true)
    public void testName(Method method) {
        Util.print("test name: " + method.getName());
    }

    @Test(groups = {"singleCluster"})
    public void testProcessInstanceStatus_StartAndEnd_checkNoInstanceAfterEndDate()
    throws Exception {
        Bundle b = new Bundle();

        try {

            //time out is set as 3 minutes .... getStatus is for a large range in past.
            //6 instance should be materialized and one in running and other in waiting

            b = (Bundle) Util.readELBundles()[0][0];
            b = new Bundle(b, ivoryqa1.getEnvFileName());
            b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
            b.setProcessValidity("2010-01-02T01:00Z", "2010-01-03T10:22Z");
            b.setProcessTimeOut(3, TimeUnit.minutes);
            b.setProcessPeriodicity(1, TimeUnit.minutes);
            b.setProcessConcurrency(1);
            b.submitAndScheduleBundle(prismHelper);
            Thread.sleep(5000);
            ProcessInstancesResult r = prismHelper.getProcessHelper()
                    .getProcessInstanceStatus(Util.readEntityName(b.getProcessData()),
                            "?start=2010-01-02T01:00Z&end=2010-01-02T10:20Z");
            InstanceUtil.validateSuccess(r, b, WorkflowStatus.RUNNING);
            InstanceUtil.validateResponse(r, 6, 1, 0, 5, 0);
        } finally {
            b.deleteBundle(prismHelper);
        }
    }


    @Test(groups = {"singleCluster"})
    public void testProcessInstanceStatus_onlyStartAfterMat() throws Exception {
        Bundle b = new Bundle();

        try {

            //time out is set as 3 minutes .... getStatus is for a large range in past.
            //6 instance should be materialized and one in running and other in waiting

            b = (Bundle) Util.readELBundles()[0][0];
            b = new Bundle(b, ivoryqa1.getEnvFileName());
            b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
            b.setProcessValidity("2010-01-02T01:00Z", "2010-01-03T10:22Z");
            b.setProcessTimeOut(3, TimeUnit.minutes);
            b.setProcessPeriodicity(1, TimeUnit.minutes);
            b.setProcessConcurrency(1);
            b.submitAndScheduleBundle(prismHelper);
            Thread.sleep(5000);
            ProcessInstancesResult r = prismHelper.getProcessHelper()
                    .getProcessInstanceStatus(Util.readEntityName(b.getProcessData()),
                            "?start=2010-01-02T05:00Z");
            Util.assertSucceeded(r);
            Assert.assertEquals(r.getInstances(), null);
        } finally {
            b.deleteBundle(prismHelper);
        }
    }


    @Test(groups = {"singleCluster"})
    public void testProcessInstanceStatus_EndOutOfRange() throws Exception {
        Bundle b = new Bundle();

        try {

            b = (Bundle) Util.readELBundles()[0][0];
            b = new Bundle(b, ivoryqa1.getEnvFileName());
            b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
            b.setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:22Z");
            b.setProcessPeriodicity(5, TimeUnit.minutes);
            b.setOutputFeedPeriodicity(5, TimeUnit.minutes);
            b.submitAndScheduleBundle(prismHelper);
            Thread.sleep(15000);
            ProcessInstancesResult r = prismHelper.getProcessHelper()
                    .getProcessInstanceStatus(Util.readEntityName(b.getProcessData()),
                            "?start=2010-01-02T01:00Z&end=2010-01-02T01:30Z");
            InstanceUtil.validateSuccessWithStatusCode(r, 400);
        } finally {
            b.deleteBundle(prismHelper);
        }
    }


    @Test(groups = {"singleCluster"})
    public void testProcessInstanceStatus_dateEmpty() {
        Bundle b = new Bundle();

        try {

            b = (Bundle) Util.readELBundles()[0][0];
            b = new Bundle(b, ivoryqa1.getEnvFileName());
            b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
            b.setProcessValidity("2010-01-02T01:00Z", "2010-01-02T02:30Z");
            b.setProcessPeriodicity(5, TimeUnit.minutes);
            b.submitAndScheduleBundle(prismHelper);
            ProcessInstancesResult r = prismHelper.getProcessHelper()
                    .getProcessInstanceStatus(Util.readEntityName(b.getProcessData()), "");
            InstanceUtil.validateSuccessWithStatusCode(r, 2);
        } catch (Exception e) {
            if (!e.getMessage().contains("Expected BEGIN_OBJECT but was STRING at line 1 column"))
                Assert.assertTrue(false);
        } finally {
            try {
                b.deleteBundle(prismHelper);
            } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    @Test(groups = {"singleCluster"})
    public void testProcessInstanceStatus_StartAndEnd() throws Exception {
        Bundle b = new Bundle();

        try {

            b = (Bundle) Util.readELBundles()[0][0];
            b = new Bundle(b, ivoryqa1.getEnvFileName());
            b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
            b.setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:22Z");
            b.setProcessPeriodicity(5, TimeUnit.minutes);
            b.submitAndScheduleBundle(prismHelper);
            Thread.sleep(5000);
            ProcessInstancesResult r = prismHelper.getProcessHelper()
                    .getProcessInstanceStatus(Util.readEntityName(b.getProcessData()),
                            "?start=2010-01-02T01:00Z&end=2010-01-02T01:20Z");
            InstanceUtil.validateSuccess(r, b, WorkflowStatus.RUNNING);
        } finally {
            b.deleteBundle(prismHelper);
        }
    }


    @Test(groups = {"singleCluster"})
    public void testProcessInstanceStatus_StartOutOfRange() throws Exception {
        Bundle b = new Bundle();

        try {

            b = (Bundle) Util.readELBundles()[0][0];
            b = new Bundle(b, ivoryqa1.getEnvFileName());
            b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
            b.setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:22Z");
            b.setProcessPeriodicity(5, TimeUnit.minutes);
            b.setOutputFeedPeriodicity(5, TimeUnit.minutes);
            b.submitAndScheduleBundle(prismHelper);
            Thread.sleep(5000);
            ProcessInstancesResult r = prismHelper.getProcessHelper()
                    .getProcessInstanceStatus(Util.readEntityName(b.getProcessData()),
                            "?start=2010-01-02T00:00Z&end=2010-01-02T01:20Z");
            InstanceUtil.validateSuccessWithStatusCode(r, 400);
        } finally {
            b.deleteBundle(prismHelper);
        }
    }


    @Test(groups = {"singleCluster"})
    public void testProcessInstanceStatus_killed() throws Exception {
        Bundle b = new Bundle();

        try {

            b = (Bundle) Util.readELBundles()[0][0];
            b = new Bundle(b, ivoryqa1.getEnvFileName());
            b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
            b.setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:22Z");
            b.setProcessPeriodicity(5, TimeUnit.minutes);
            b.submitAndScheduleBundle(prismHelper);
            Thread.sleep(15000);
            prismHelper.getProcessHelper().delete(URLS.DELETE_URL, b.getProcessData());
            Thread.sleep(15000);
            ProcessInstancesResult r = prismHelper.getProcessHelper()
                    .getProcessInstanceStatus(Util.readEntityName(b.getProcessData()),
                            "?start=2010-01-02T01:00Z&end=2010-01-02T01:20Z");
            if ((r.getStatusCode() != 777))
                Assert.assertTrue(false);
        } finally {
            b.deleteBundle(prismHelper);
        }
    }


    @Test(groups = {"singleCluster"})
    public void testProcessInstanceStatus_onlyStartSuspended() throws Exception {
        Bundle b = new Bundle();

        try {

            b = (Bundle) Util.readELBundles()[0][0];
            b = new Bundle(b, ivoryqa1.getEnvFileName());
            b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
            b.setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:22Z");
            b.setProcessPeriodicity(5, TimeUnit.minutes);
            b.submitAndScheduleBundle(prismHelper);
            Thread.sleep(15000);
            prismHelper.getProcessHelper().suspend(URLS.SUSPEND_URL, b.getProcessData());
            Thread.sleep(15000);
            ProcessInstancesResult r = prismHelper.getProcessHelper()
                    .getProcessInstanceStatus(Util.readEntityName(b.getProcessData()),
                            "?start=2010-01-02T01:00Z");
            InstanceUtil.validateSuccessOnlyStart(r, WorkflowStatus.SUSPENDED);
        } finally {
            b.deleteBundle(prismHelper);
        }
    }


    @Test(groups = {"singleCluster"})
    public void testProcessInstanceStatus_reverseDateRange() throws Exception {
        Bundle b = new Bundle();

        try {

            b = (Bundle) Util.readELBundles()[0][0];
            b = new Bundle(b, ivoryqa1.getEnvFileName());
            b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
            b.setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:22Z");
            b.setProcessPeriodicity(5, TimeUnit.minutes);
            b.submitAndScheduleBundle(prismHelper);
            Thread.sleep(15000);
            ProcessInstancesResult r = prismHelper.getProcessHelper()
                    .getProcessInstanceStatus(Util.readEntityName(b.getProcessData()),
                            "?start=2010-01-02T01:20Z&end=2010-01-02T01:07Z");
            InstanceUtil.validateSuccessWithStatusCode(r, 400);
        } finally {
            b.deleteBundle(prismHelper);
        }
    }


    @Test(groups = {"singleCluster"})
    public void testProcessInstanceStatus_StartEndOutOfRange() throws Exception {
        Bundle b = new Bundle();

        try {

            b = (Bundle) Util.readELBundles()[0][0];
            b = new Bundle(b, ivoryqa1.getEnvFileName());
            b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
            b.setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:22Z");
            b.setProcessPeriodicity(5, TimeUnit.minutes);
            b.setOutputFeedPeriodicity(5, TimeUnit.minutes);
            b.setOutputFeedLocationData(
                    "/examples/output-data/aggregator/aggregatedLogs/${YEAR}/${MONTH}/${DAY}/$" +
                            "{HOUR}/${MINUTE}");
            b.setProcessConcurrency(2);
            b.submitAndScheduleBundle(prismHelper);
            Thread.sleep(15000);
            ProcessInstancesResult r = prismHelper.getProcessHelper()
                    .getProcessInstanceStatus(Util.readEntityName(b.getProcessData()),
                            "?start=2010-01-02T00:00Z&end=2010-01-02T01:30Z");
            InstanceUtil.validateSuccessWithStatusCode(r, 400);
        } finally {
            b.deleteBundle(prismHelper);
        }
    }


    @Test(groups = {"singleCluster"})
    public void testProcessInstanceStatus_resumed() throws Exception {
        Bundle b = new Bundle();

        try {

            b = (Bundle) Util.readELBundles()[0][0];
            b = new Bundle(b, ivoryqa1.getEnvFileName());
            b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
            b.setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:22Z");
            b.setProcessPeriodicity(5, TimeUnit.minutes);
            b.setOutputFeedPeriodicity(5, TimeUnit.minutes);
            b.setOutputFeedLocationData(
                    "/examples/output-data/aggregator/aggregatedLogs/${YEAR}/${MONTH}/${DAY}/$" +
                            "{HOUR}/${MINUTE}");
            b.setProcessConcurrency(2);
            b.submitAndScheduleBundle(prismHelper);
            Thread.sleep(15000);
            prismHelper.getProcessHelper().suspend(URLS.SUSPEND_URL, b.getProcessData());
            Thread.sleep(15000);
            prismHelper.getProcessHelper().resume(URLS.RESUME_URL, b.getProcessData());
            Thread.sleep(15000);
            ProcessInstancesResult r = prismHelper.getProcessHelper()
                    .getProcessInstanceStatus(Util.readEntityName(b.getProcessData()),
                            "?start=2010-01-02T01:00Z&end=2010-01-02T01:22Z");
            InstanceUtil.validateSuccess(r, b, WorkflowStatus.RUNNING);
        } finally {
            b.deleteBundle(prismHelper);
        }
    }

    @Test(groups = {"singleCluster"})
    public void testProcessInstanceStatus_onlyStart() throws Exception {
        Bundle b = new Bundle();

        try {
            b = (Bundle) Util.readELBundles()[0][0];
            b = new Bundle(b, ivoryqa1.getEnvFileName());
            b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
            b.setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:22Z");
            b.setProcessPeriodicity(5, TimeUnit.minutes);
            b.submitAndScheduleBundle(prismHelper);
            Thread.sleep(15000);
            ProcessInstancesResult r = prismHelper.getProcessHelper()
                    .getProcessInstanceStatus(Util.readEntityName(b.getProcessData()),
                            "?start=2010-01-02T01:00Z");
            InstanceUtil.validateSuccessOnlyStart(r, WorkflowStatus.RUNNING);
        } finally {
            b.deleteBundle(prismHelper);
        }
    }


    @Test(groups = {"singleCluster"})
    public void testProcessInstanceStatus_invalidName() throws Exception {
        Bundle b = new Bundle();

        try {

            b = (Bundle) Util.readELBundles()[0][0];
            b = new Bundle(b, ivoryqa1.getEnvFileName());
            b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
            b.setProcessValidity("2010-01-02T01:00Z", "2010-01-02T02:30Z");
            b.setProcessPeriodicity(5, TimeUnit.minutes);
            b.submitAndScheduleBundle(prismHelper);
            ProcessInstancesResult r = prismHelper.getProcessHelper()
                    .getProcessInstanceStatus("invalidProcess", "?start=2010-01-01T01:00Z");
            if (!(r.getStatusCode() == 777))
                Assert.assertTrue(false);
        } finally {
            b.deleteBundle(prismHelper);
        }
    }


    @Test(groups = {"singleCluster"})
    public void testProcessInstanceStatus_suspended() throws Exception {
        Bundle b = new Bundle();

        try {

            b = (Bundle) Util.readELBundles()[0][0];
            b = new Bundle(b, ivoryqa1.getEnvFileName());

            b = new Bundle(b, ivoryqa1.getEnvFileName());
            b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
            b.setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:22Z");
            b.setProcessPeriodicity(5, TimeUnit.minutes);
            //b.setCLusterColo("ua2");

            for (int i = 0; i < b.getClusters().size(); i++)

                Util.print("cluster to be submitted: " + i + "  " + b.getClusters().get(i));

            b.submitAndScheduleBundle(prismHelper);
            Thread.sleep(5000);
            prismHelper.getProcessHelper().suspend(URLS.SUSPEND_URL, b.getProcessData());
            Thread.sleep(5000);
            //		ProcessInstancesResult r  = ivoryqa1.getProcessHelper()
            // .getProcessInstanceStatus(Util
            // .readEntityName(b.getProcessData()),
            // "?start=2010-01-02T01:00Z&end=2010-01-02T01:20Z");
            ProcessInstancesResult r = prismHelper.getProcessHelper()
                    .getProcessInstanceStatus(Util.readEntityName(b.getProcessData()),
                            "?start=2010-01-02T01:00Z&end=2010-01-02T01:20Z");

            InstanceUtil.validateSuccess(r, b, WorkflowStatus.SUSPENDED);
        } finally {
            prismHelper.getProcessHelper().delete(URLS.DELETE_URL, b.getProcessData());
            prismHelper.getFeedHelper().delete(URLS.DELETE_URL, b.getDataSets().get(0));
            prismHelper.getFeedHelper().delete(URLS.DELETE_URL, b.getDataSets().get(1));
            prismHelper.getClusterHelper().delete(URLS.DELETE_URL, b.getClusters().get(0));
        }
    }

    @Test(groups = {"singleCluster"})
    public void testProcessInstanceStatus_woParams() throws Exception {
        Bundle b = new Bundle();

        try {

            b = (Bundle) Util.readELBundles()[0][0];
            b = new Bundle(b, ivoryqa1.getEnvFileName());
            b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
            b.setProcessValidity("2010-01-02T01:00Z", "2010-01-02T02:30Z");
            b.setProcessPeriodicity(5, TimeUnit.minutes);
            b.submitAndScheduleBundle(prismHelper);
            ProcessInstancesResult r = prismHelper.getProcessHelper()
                    .getProcessInstanceStatus(Util.readEntityName(b.getProcessData()), null);
            InstanceUtil.validateSuccessWithStatusCode(r, 2);
        } finally {
            b.deleteBundle(prismHelper);
        }
    }

    @Test(groups = {"singleCluster"})
    public void testProcessInstanceStatus_timedOut() throws Exception {
        Bundle b = new Bundle();

        //submit 
        try {

            b = (Bundle) Util.readELBundles()[0][0];
            b = new Bundle(b, ivoryqa1.getEnvFileName());
            b.setInputFeedDataPath(
                    "/samarthData/timedoutStatus/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");

            b.setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:11Z");
            b.setProcessPeriodicity(5, TimeUnit.minutes);
            b.setProcessTimeOut(2, TimeUnit.minutes);
            b.setOutputFeedPeriodicity(5, TimeUnit.minutes);
            b.setOutputFeedLocationData(
                    "/examples/samarth/output-data/aggregator/aggregatedLogs/${YEAR}/${MONTH}/$" +
                            "{DAY}/${HOUR}/${MINUTE" +
                            "}");
            b.setProcessConcurrency(3);
            b.submitAndScheduleBundle(prismHelper);
            //Thread.sleep(240000);
            org.apache.oozie.client.CoordinatorAction.Status s = null;
            while (!org.apache.oozie.client.CoordinatorAction.Status.TIMEDOUT.equals(s)) {
                s = InstanceUtil
                        .getInstanceStatus(ivoryqa1, Util.readEntityName(b.getProcessData()), 0, 0);
                Thread.sleep(15000);
            }
            ProcessInstancesResult r = prismHelper.getProcessHelper()
                    .getProcessInstanceStatus(Util.readEntityName(b.getProcessData()),
                            "?start=2010-01-02T01:00Z&end=2010-01-02T01:11Z");
            InstanceUtil.validateSuccessWithStatusCode(r, 2);
        } finally {
            b.deleteBundle(prismHelper);
        }
    }
}
