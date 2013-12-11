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

/**
 * Process instance kill tests.
 */
public class ProcessInstanceKillsTest {

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
    public void testProcessInstanceKill_single() throws Exception {

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
            Thread.sleep(15000);
            ProcessInstancesResult r = prismHelper.getProcessHelper()
                    .getProcessInstanceKill(Util.readEntityName(b.getProcessData()),
                            "?start=2010-01-02T01:00Z");
            InstanceUtil.validateSuccess(r, b, WorkflowStatus.KILLED);
        } finally {
            b.deleteBundle(prismHelper);

        }
    }


    @Test(groups = {"singleCluster"})
    public void testProcessInstanceKill_startAndEndSame() throws Exception {
        Bundle b = new Bundle();
        try {

            b = (Bundle) Util.readELBundles()[0][0];
            b = new Bundle(b, ivoryqa1.getEnvFileName());
            b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");

            b.setProcessValidity("2010-01-02T00:00Z", "2010-01-02T04:00Z");
            b.setProcessConcurrency(2);
            b.setProcessTimeOut(3, TimeUnit.minutes);
            b.setProcessPeriodicity(1, TimeUnit.minutes);
            b.setOutputFeedPeriodicity(5, TimeUnit.minutes);
            b.setOutputFeedLocationData(
                    "/examples/output-data/aggregator/aggregatedLogs/${YEAR}/${MONTH}/${DAY}/$" +
                            "{HOUR}/${MINUTE}");
            b.setProcessConcurrency(10);
            b.submitAndScheduleBundle(prismHelper);
            Thread.sleep(15000);

            ProcessInstancesResult r = prismHelper.getProcessHelper()
                    .getProcessInstanceKill(Util.readEntityName(b.getProcessData()),
                            "?start=2010-01-02T00:03Z&end=2010-01-02T00:03Z");
            InstanceUtil.validateResponse(r, 1, 0, 0, 0, 1);

        } finally {
            b.deleteBundle(prismHelper);

        }
    }


    @Test(groups = {"singleCluster"})
    public void testProcessInstanceKill_killNonMatrelized() throws Exception {
        Bundle b = new Bundle();
        try {

            b = (Bundle) Util.readELBundles()[0][0];
            b = new Bundle(b, ivoryqa1.getEnvFileName());
            b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");

            b.setProcessValidity("2010-01-02T00:00Z", "2010-01-02T04:00Z");
            b.setProcessConcurrency(2);
            b.setProcessTimeOut(3, TimeUnit.minutes);
            b.setProcessPeriodicity(1, TimeUnit.minutes);
            b.setOutputFeedPeriodicity(5, TimeUnit.minutes);
            b.setOutputFeedLocationData(
                    "/examples/output-data/aggregator/aggregatedLogs/${YEAR}/${MONTH}/${DAY}/$" +
                            "{HOUR}/${MINUTE}");
            b.setProcessConcurrency(10);
            b.submitAndScheduleBundle(prismHelper);
            Thread.sleep(15000);

            ProcessInstancesResult r = prismHelper.getProcessHelper()
                    .getProcessInstanceKill(Util.readEntityName(b.getProcessData()),
                            "?start=2010-01-02T00:03Z&end=2010-01-02T00:30Z");

            InstanceUtil.validateResponse(r, 3, 0, 0, 0, 3);
            Thread.sleep(15000);
            Util.print(r.toString());

        } finally {
            b.deleteBundle(prismHelper);

        }
    }


    @Test(groups = {"singleCluster"})
    public void testProcessInstanceKill_bothStartAndEndInFuture01() throws Exception {
        Bundle b = new Bundle();
        try {

            b = (Bundle) Util.readELBundles()[0][0];
            b = new Bundle(b, ivoryqa1.getEnvFileName());
            b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");

            String startTime = InstanceUtil.getTimeWrtSystemTime(-20);
            String endTime = InstanceUtil.getTimeWrtSystemTime(400);

            String startTimeData = InstanceUtil.getTimeWrtSystemTime(-150);
            String endTimeData = InstanceUtil.getTimeWrtSystemTime(50);

            InstanceUtil.createDataWithinDatesAndPrefix(ivoryqa1,
                    InstanceUtil.oozieDateToDate(startTimeData),
                    InstanceUtil.oozieDateToDate(endTimeData), "/samarthData/", 1);

            b.setProcessValidity(startTime, endTime);
            b.setProcessPeriodicity(5, TimeUnit.minutes);
            b.setOutputFeedPeriodicity(5, TimeUnit.minutes);
            b.setOutputFeedLocationData(
                    "/examples/output-data/aggregator/aggregatedLogs/${YEAR}/${MONTH}/${DAY}/$" +
                            "{HOUR}/${MINUTE}");
            b.setProcessConcurrency(6);
            b.submitAndScheduleBundle(prismHelper);
            Thread.sleep(15000);
            String startTimeRequest = InstanceUtil.getTimeWrtSystemTime(-17);
            String endTimeRequest = InstanceUtil.getTimeWrtSystemTime(23);
            ProcessInstancesResult r = prismHelper.getProcessHelper()
                    .getProcessInstanceKill(Util.readEntityName(b.getProcessData()),
                            "?start=" + startTimeRequest + "&end=" + endTimeRequest);

            Thread.sleep(15000);
            Util.print(r.toString());

        } finally {
            b.deleteBundle(prismHelper);

        }
    }


    @Test(groups = {"singleCluster"})
    public void testProcessInstanceKill_bothStartAndEndInFuture() throws Exception {
        Bundle b = new Bundle();
        try {

            b = (Bundle) Util.readELBundles()[0][0];
            b = new Bundle(b, ivoryqa1.getEnvFileName());
            b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
            b.setProcessValidity("2010-01-02T01:00Z", "2099-01-02T01:21Z");
            b.setProcessPeriodicity(5, TimeUnit.minutes);
            b.setOutputFeedPeriodicity(5, TimeUnit.minutes);
            b.setOutputFeedLocationData(
                    "/examples/output-data/aggregator/aggregatedLogs/${YEAR}/${MONTH}/${DAY}/$" +
                            "{HOUR}/${MINUTE}");
            b.setProcessConcurrency(6);
            b.submitAndScheduleBundle(prismHelper);
            Thread.sleep(15000);
            String startTime = InstanceUtil.getTimeWrtSystemTime(10);
            String endTime = InstanceUtil.getTimeWrtSystemTime(400);
            ProcessInstancesResult r = prismHelper.getProcessHelper()
                    .getProcessInstanceKill(Util.readEntityName(b.getProcessData()),
                            "?start=" + startTime + "&end=" + endTime);
            //instanceUtil.validateSuccess(r, b, WorkflowStatus.KILLED);
            Thread.sleep(15000);
            Util.print(r.getMessage());
            Assert.assertEquals(r.getInstances(), null);

        } finally {
            b.deleteBundle(prismHelper);

        }
    }


    @Test(groups = {"singleCluster"})
    public void testProcessInstanceKill_multipleInstance() throws Exception {
        Bundle b = new Bundle();
        try {

            b = (Bundle) Util.readELBundles()[0][0];
            b = new Bundle(b, ivoryqa1.getEnvFileName());
            b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
            b.setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:21Z");
            b.setProcessPeriodicity(5, TimeUnit.minutes);
            b.setOutputFeedPeriodicity(5, TimeUnit.minutes);
            b.setOutputFeedLocationData(
                    "/examples/output-data/aggregator/aggregatedLogs/${YEAR}/${MONTH}/${DAY}/$" +
                            "{HOUR}/${MINUTE}");
            b.setProcessConcurrency(6);
            b.submitAndScheduleBundle(prismHelper);
            Thread.sleep(15000);
            prismHelper.getProcessHelper()
                    .getProcessInstanceKill(Util.readEntityName(b.getProcessData()),
                            "?start=2010-01-02T01:05Z&end=2010-01-02T01:15Z");
            //instanceUtil.validateSuccess(result, b, WorkflowStatus.KILLED);
            Thread.sleep(15000);
            ProcessInstancesResult result = prismHelper.getProcessHelper()
                    .getProcessInstanceStatus(Util.readEntityName(b.getProcessData()),
                            "?start=2010-01-02T01:00Z&end=2010-01-02T01:20Z");
            InstanceUtil.validateResponse(result, 5, 2, 0, 0, 3);
        } finally {
            b.deleteBundle(prismHelper);

        }
    }


    @Test(groups = {"singleCluster"})
    public void testProcessInstanceKill_lastInstance() throws Exception {

        Bundle b = new Bundle();
        try {

            b = (Bundle) Util.readELBundles()[0][0];
            b = new Bundle(b, ivoryqa1.getEnvFileName());
            b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
            b.setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:21Z");
            b.setProcessPeriodicity(5, TimeUnit.minutes);
            b.setOutputFeedPeriodicity(5, TimeUnit.minutes);
            b.setOutputFeedLocationData(
                    "/examples/output-data/aggregator/aggregatedLogs/${YEAR}/${MONTH}/${DAY}/$" +
                            "{HOUR}/${MINUTE}");
            b.setProcessConcurrency(6);
            b.submitAndScheduleBundle(prismHelper);
            Thread.sleep(15000);
            prismHelper.getProcessHelper()
                    .getProcessInstanceKill(Util.readEntityName(b.getProcessData()),
                            "?start=2010-01-02T01:20Z");
            //instanceUtil.validateSuccess(r, b, WorkflowStatus.KILLED);
            Thread.sleep(15000);
            ProcessInstancesResult result = prismHelper.getProcessHelper()
                    .getProcessInstanceStatus(Util.readEntityName(b.getProcessData()),
                            "?start=2010-01-02T01:00Z&end=2010-01-02T01:20Z");
            InstanceUtil.validateResponse(result, 5, 4, 0, 0, 1);
        } finally {
            b.deleteBundle(prismHelper);

        }
    }


    @Test(groups = {"singleCluster"})
    public void testProcessInstanceKill_suspended() throws Exception {

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
            Thread.sleep(15000);
            prismHelper.getProcessHelper()
                    .getProcessInstanceSuspend(Util.readEntityName(b.getProcessData()),
                            "?start=2010-01-02T01:00Z");
            Thread.sleep(15000);
            ProcessInstancesResult r = prismHelper.getProcessHelper()
                    .getProcessInstanceKill(Util.readEntityName(b.getProcessData()),
                            "?start=2010-01-02T01:00Z");
            InstanceUtil.validateSuccess(r, b, WorkflowStatus.KILLED);
        } finally {
            b.deleteBundle(prismHelper);

        }
    }


    @Test(groups = {"singleCluster"})
    public void testProcessInstanceKill_succeeded() throws Exception {

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
            for (int i = 0; i < 30; i++) {
                if (InstanceUtil
                        .getInstanceStatus(ivoryqa1, Util.readEntityName(b.getProcessData()), 0, 0)
                        .equals(CoordinatorAction.Status.SUCCEEDED))
                    break;
                Thread.sleep(30000);
            }
            ProcessInstancesResult r = prismHelper.getProcessHelper()
                    .getProcessInstanceKill(Util.readEntityName(b.getProcessData()),
                            "?start=2010-01-02T01:00Z");
            InstanceUtil.validateSuccess(r, b, WorkflowStatus.SUCCEEDED);
        } finally {
            b.deleteBundle(prismHelper);

        }
    }


    @AfterClass(alwaysRun = true)
    public void deleteData() throws Exception {
        Util.print("in @AfterClass");

        System.setProperty("java.security.krb5.realm", "");
        System.setProperty("java.security.krb5.kdc", "");


        Bundle b = (Bundle) Util.readELBundles()[0][0];
        b = new Bundle(b, ivoryqa1.getEnvFileName());

        b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
        String prefix = b.getFeedDataPathPrefix();
        Util.HDFSCleanup(ivoryqa1, prefix.substring(1));
    }

}
