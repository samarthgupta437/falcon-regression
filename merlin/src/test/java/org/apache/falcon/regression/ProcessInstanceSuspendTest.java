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
import org.apache.falcon.regression.core.response.ProcessInstancesResult;
import org.apache.falcon.regression.core.response.ProcessInstancesResult.WorkflowStatus;
import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.core.util.Util.URLS;
import org.apache.falcon.regression.testHelper.BaseSingleClusterTests;
import org.apache.oozie.client.CoordinatorAction;
import org.joda.time.DateTime;
import org.testng.Assert;
import org.testng.annotations.*;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

/**
 * Process instance suspend tests.
 */
public class ProcessInstanceSuspendTest extends BaseSingleClusterTests {

    private String feedInputPath = "/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}";
    private String feedOutputPath = "/examples/output-data/aggregator/aggregatedLogs/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}";
    private Bundle b = new Bundle();

    public ProcessInstanceSuspendTest() throws IOException {
        super();
    }

    @BeforeClass(alwaysRun = true)
    public void createTestData() throws Exception {
        Util.print("in @BeforeClass");
        System.setProperty("java.security.krb5.realm", "");
        System.setProperty("java.security.krb5.kdc", "");

        Bundle bundle = (Bundle) Util.readELBundles()[0][0];
        bundle = new Bundle(bundle, server1.getEnvFileName());
        String startDate = "2010-01-01T20:00Z";
        String endDate = "2010-01-03T01:04Z";
        bundle.setInputFeedDataPath(feedInputPath);
        String prefix = bundle.getFeedDataPathPrefix();
        HadoopUtil.deleteDirIfExists(prefix.substring(1), server1FS);

        DateTime startDateJoda = new DateTime(InstanceUtil.oozieDateToDate(startDate));
        DateTime endDateJoda = new DateTime(InstanceUtil.oozieDateToDate(endDate));
        List<String> dataDates = Util.getMinuteDatesOnEitherSide(startDateJoda, endDateJoda, 20);
        for (int i = 0; i < dataDates.size(); i++)
            dataDates.set(i, prefix + dataDates.get(i));

        ArrayList<String> dataFolder = new ArrayList<String>();
        for (String dataDate : dataDates) {
            dataFolder.add(dataDate);
        }
        HadoopUtil.flattenAndPutDataInFolder(server1FS, "src/test/resources/OozieExampleInputData/normalInput", dataFolder);
    }

    @BeforeMethod(alwaysRun = true)
    public void setup(Method method) throws Exception {
        Util.print("test name: " + method.getName());
        Util.restartService(server1.getClusterHelper());
        //System.out.println("Waiting 20 seconds...");
        //Thread.sleep(20000);
        b = (Bundle) Util.readELBundles()[0][0];
        b = new Bundle(b, server1.getEnvFileName());
        b.setInputFeedDataPath(feedInputPath);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws Exception {
        b.deleteBundle(prism);
    }

    @Test(groups = {"singleCluster"})
    public void testProcessInstanceSuspend_largeRange() throws Exception {
        b.setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:23Z");
        b.setProcessPeriodicity(5, TimeUnit.minutes);
        b.setProcessPeriodicity(5, TimeUnit.minutes);
        b.setOutputFeedLocationData(feedOutputPath);
        b.setProcessConcurrency(5);
        b.submitAndScheduleBundle(prism);
        Thread.sleep(15000);
        ProcessInstancesResult result = prism.getProcessHelper()
                .getProcessInstanceStatus(Util.readEntityName(b.getProcessData()),
                        "?start=2010-01-02T01:00Z&end=2010-01-02T01:21Z");
        InstanceUtil.validateResponse(result, 5, 5, 0, 0, 0);
        prism.getProcessHelper()
                .getProcessInstanceSuspend(Util.readEntityName(b.getProcessData()),
                        "?start=2010-01-02T00:00Z&end=2010-01-02T01:30Z");
        result = prism.getProcessHelper()
                .getProcessInstanceStatus(Util.readEntityName(b.getProcessData()),
                        "?start=2010-01-02T00:00Z&end=2010-01-02T01:30Z");
        InstanceUtil.validateSuccessWithStatusCode(result, 400);
    }


    @Test(groups = {"singleCluster"})
    public void testProcessInstanceSuspend_succeeded() throws Exception {
        b.setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:04Z");
        b.setProcessPeriodicity(5, TimeUnit.minutes);
        b.setProcessPeriodicity(5, TimeUnit.minutes);
        b.setOutputFeedLocationData(feedOutputPath);
        b.setProcessConcurrency(1);
        b.submitAndScheduleBundle(prism);
        //wait for instance to succeed
        for (int i = 0; i < 30; i++) {
            if (InstanceUtil
                    .getInstanceStatus(server1, Util.getProcessName(b.getProcessData()), 0, 0)
                    .equals(CoordinatorAction.Status.SUCCEEDED))
                break;

            Util.print("still in for loop: testProcessInstanceSuspend_succeeded");
            Thread.sleep(20000);
        }
        ProcessInstancesResult r = prism.getProcessHelper()
                .getProcessInstanceSuspend(Util.readEntityName(b.getProcessData()),
                        "?start=2010-01-02T01:00Z");
        InstanceUtil.validateSuccessWithStatusCode(r, 0);
    }


    @Test(groups = {"singleCluster"})
    public void testProcessInstanceSuspend_all() throws Exception {
        b.setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:23Z");
        b.setProcessPeriodicity(5, TimeUnit.minutes);
        b.setProcessPeriodicity(5, TimeUnit.minutes);
        b.setOutputFeedLocationData(feedOutputPath);
        b.setProcessConcurrency(5);
        b.submitAndScheduleBundle(prism);
        Thread.sleep(15000);
        ProcessInstancesResult result = prism.getProcessHelper()
                .getProcessInstanceStatus(Util.readEntityName(b.getProcessData()),
                        "?start=2010-01-02T01:00Z&end=2010-01-02T01:20Z");
        InstanceUtil.validateResponse(result, 5, 5, 0, 0, 0);
        prism.getProcessHelper()
                .getProcessInstanceSuspend(Util.readEntityName(b.getProcessData()),
                        "?start=2010-01-02T01:00Z&end=2010-01-02T01:20Z");
        result = prism.getProcessHelper()
                .getProcessInstanceStatus(Util.readEntityName(b.getProcessData()),
                        "?start=2010-01-02T01:00Z&end=2010-01-02T01:20Z");
        InstanceUtil.validateResponse(result, 5, 0, 5, 0, 0);
    }


    @Test(groups = {"singleCluster"})
    public void testProcessInstanceSuspend_woParams() throws Exception {
        b.setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:22Z");
        b.setProcessPeriodicity(5, TimeUnit.minutes);
        b.setProcessPeriodicity(5, TimeUnit.minutes);
        b.setOutputFeedLocationData(feedOutputPath);
        b.setProcessConcurrency(2);
        b.submitAndScheduleBundle(prism);
        Thread.sleep(15000);
        ProcessInstancesResult r = prism.getProcessHelper()
                .getProcessInstanceSuspend(Util.readEntityName(b.getProcessData()), null);
        InstanceUtil.validateSuccessWithStatusCode(r, 2);
    }


    @Test(groups = {"singleCluster"})
    public void testProcessInstanceSuspend_StartAndEnd() throws Exception {
        b.setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:23Z");
        b.setProcessPeriodicity(5, TimeUnit.minutes);
        b.setProcessPeriodicity(5, TimeUnit.minutes);
        b.setOutputFeedLocationData(feedOutputPath);
        b.setProcessConcurrency(3);
        b.submitAndScheduleBundle(prism);
        Thread.sleep(15000);
        ProcessInstancesResult result = prism.getProcessHelper()
                .getProcessInstanceStatus(Util.readEntityName(b.getProcessData()),
                        "?start=2010-01-02T01:00Z&end=2010-01-02T01:22Z");
        InstanceUtil.validateResponse(result, 5, 3, 0, 2, 0);
        prism.getProcessHelper()
                .getProcessInstanceSuspend(Util.readEntityName(b.getProcessData()),
                        "?start=2010-01-02T01:00Z&end=2010-01-02T01:15Z");
        result = prism.getProcessHelper()
                .getProcessInstanceStatus(Util.readEntityName(b.getProcessData()),
                        "?start=2010-01-02T01:00Z&end=2010-01-02T01:22Z");
        InstanceUtil.validateResponse(result, 5, 0, 3, 2, 0);
    }


    @Test(groups = {"singleCluster"})
    public void testProcessInstanceSuspend_nonExistent() throws Exception {
        b.setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:23Z");
        b.setProcessPeriodicity(5, TimeUnit.minutes);
        b.setProcessPeriodicity(5, TimeUnit.minutes);
        b.setOutputFeedLocationData(feedOutputPath);
        b.setProcessConcurrency(5);
        b.submitAndScheduleBundle(prism);
        Thread.sleep(15000);
        ProcessInstancesResult r =
                prism.getProcessHelper()
                        .getProcessInstanceSuspend("invalidName", "?start=2010-01-02T01:20Z");
        if ((r.getStatusCode() != 777))
            Assert.assertTrue(false);
    }


    @Test(groups = {"singleCluster"})
    public void testProcessInstanceSuspend_onlyStart() throws Exception {
        b.setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:11Z");
        b.setProcessPeriodicity(5, TimeUnit.minutes);
        b.setProcessPeriodicity(5, TimeUnit.minutes);
        b.setOutputFeedLocationData(feedOutputPath);
        b.setProcessConcurrency(3);
        b.submitAndScheduleBundle(prism);
        Thread.sleep(15000);
        prism.getProcessHelper()
                .getRunningInstance(URLS.INSTANCE_RUNNING,
                        Util.readEntityName(b.getProcessData()));
        ProcessInstancesResult r = prism.getProcessHelper()
                .getProcessInstanceSuspend(Util.readEntityName(b.getProcessData()),
                        "?start=2010-01-02T01:00Z");
        InstanceUtil.validateSuccessOnlyStart(r, WorkflowStatus.SUSPENDED);
        prism.getProcessHelper()
                .getRunningInstance(URLS.INSTANCE_RUNNING,
                        Util.readEntityName(b.getProcessData()));
    }

    @Test(groups = {"singleCluster"})
    public void testProcessInstanceSuspend_suspendLast() throws Exception {
        b.setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:23Z");
        b.setProcessPeriodicity(5, TimeUnit.minutes);
        b.setProcessPeriodicity(5, TimeUnit.minutes);
        b.setOutputFeedLocationData(feedOutputPath);
        b.setProcessConcurrency(5);
        b.submitAndScheduleBundle(prism);
        Thread.sleep(15000);
        ProcessInstancesResult result = prism.getProcessHelper()
                .getProcessInstanceStatus(Util.readEntityName(b.getProcessData()),
                        "?start=2010-01-02T01:00Z&end=2010-01-02T01:20Z");
        InstanceUtil.validateResponse(result, 5, 5, 0, 0, 0);
        prism.getProcessHelper()
                .getProcessInstanceSuspend(Util.readEntityName(b.getProcessData()),
                        "?start=2010-01-02T01:20Z");
        result = prism.getProcessHelper()
                .getProcessInstanceStatus(Util.readEntityName(b.getProcessData()),
                        "?start=2010-01-02T01:00Z&end=2010-01-02T01:20Z");
        InstanceUtil.validateResponse(result, 5, 4, 1, 0, 0);
    }

    @AfterClass(alwaysRun = true)
    public void deleteData() throws Exception {
        Util.print("in @AfterClass");
        System.setProperty("java.security.krb5.realm", "");
        System.setProperty("java.security.krb5.kdc", "");

        Bundle b = (Bundle) Util.readELBundles()[0][0];
        b = new Bundle(b, server1.getEnvFileName());
        b.setInputFeedDataPath(feedInputPath);
        String prefix = b.getFeedDataPathPrefix();
        HadoopUtil.deleteDirIfExists(prefix.substring(1), server1FS);
    }
}
