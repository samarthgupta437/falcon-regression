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
import org.apache.falcon.regression.core.response.ProcessInstancesResult;
import org.apache.falcon.regression.core.response.ProcessInstancesResult.WorkflowStatus;
import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.core.util.Util.URLS;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.hadoop.fs.FileSystem;
import org.joda.time.DateTime;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

/**
 * Process instance resume tests.
 */
@Test(groups = "standalone")
public class ProcessInstanceResumeTest extends BaseTestClass {

    ColoHelper cluster;
    FileSystem clusterFS;
    Bundle b = new Bundle();
    private Bundle bundle;
    String baseTestHDFSDir = baseHDFSDir + "/ProcessInstanceResumeTest";
    String feedInputPath = baseTestHDFSDir + "/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}";
    String feedOutputPath = baseTestHDFSDir + "/output-data/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}";

    public ProcessInstanceResumeTest() {
        super();
        cluster = servers.get(0);
        clusterFS = serverFS.get(0);
    }

    @BeforeClass(alwaysRun = true)
    public void createTestData() throws Exception {

        Util.print("in @BeforeClass");

        System.setProperty("java.security.krb5.realm", "");
        System.setProperty("java.security.krb5.kdc", "");


        Bundle b = (Bundle) Util.readELBundles()[0][0];
        b = new Bundle(b, cluster.getEnvFileName(), cluster.getPrefix());
        b = new Bundle(b, cluster.getEnvFileName(), cluster.getPrefix());

        String startDate = "2010-01-01T20:00Z";
        String endDate = "2010-01-03T01:04Z";

        b.setInputFeedDataPath(feedInputPath);
        String prefix = b.getFeedDataPathPrefix();
        Util.HDFSCleanup(clusterFS, prefix.substring(1));

        DateTime startDateJoda = new DateTime(InstanceUtil.oozieDateToDate(startDate));
        DateTime endDateJoda = new DateTime(InstanceUtil.oozieDateToDate(endDate));

        List<String> dataDates = Util.getMinuteDatesOnEitherSide(startDateJoda, endDateJoda, 20);

        ArrayList<String> dataFolder = new ArrayList<String>();
        int i = 0;
        for (String dataDate : dataDates) {
            dataFolder.add(i, prefix + dataDate);
            i++;
        }
        HadoopUtil.flattenAndPutDataInFolder(clusterFS, "src/test/resources/OozieExampleInputData/normalInput", dataFolder);
    }

    @BeforeMethod(alwaysRun = true)
    public void setup(Method method) throws Exception {
        Util.print("setup " + method.getName());

        bundle = (Bundle) Util.readELBundles()[0][0];
        b = (Bundle) Util.readELBundles()[0][0];
        b = new Bundle(b, cluster.getEnvFileName(), cluster.getPrefix());
        b.setInputFeedDataPath(feedInputPath);
        b.setOutputFeedLocationData(feedOutputPath);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown(Method method) throws Exception {
        Util.print("tearDown " + method.getName());
        if (bundle != null) {
            bundle.deleteBundle(cluster);
        }
        b.deleteBundle(prism);
    }

    @Test(groups = {"singleCluster"})
    public void testProcessInstanceResume_onlyEnd() throws Exception {
        b.setProcessConcurrency(6);
        b.setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:26Z");
        b.setProcessPeriodicity(5, TimeUnit.minutes);
        b.setOutputFeedPeriodicity(5, TimeUnit.minutes);
        b.submitAndScheduleBundle(prism);
        Thread.sleep(15000);
        prism.getProcessHelper()
                .getProcessInstanceSuspend(Util.readEntityName(b.getProcessData()),
                        "?start=2010-01-02T01:05Z&end=2010-01-02T01:21Z");
        Thread.sleep(10000);
        ProcessInstancesResult result = prism.getProcessHelper()
                .getProcessInstanceStatus(Util.readEntityName(b.getProcessData()),
                        "?start=2010-01-02T01:00Z&end=2010-01-02T01:26Z");
        InstanceUtil.validateResponse(result, 6, 2, 4, 0, 0);

        result = prism.getProcessHelper()
                .getProcessInstanceResume(Util.readEntityName(b.getProcessData()),
                        "?end=2010-01-02T01:15Z");
        Assert.assertNotNull(result, "result should not be null");
        Assert.assertNotNull(result.getMessage(), "result's message should not be null");
        Assert.assertTrue(result.getMessage().contains("Parameter start is empty"),
                "start instance was not mentioned, RESUME should have failed");
    }


    @Test(groups = {"singleCluster"})
    public void testProcessInstanceResume_resumeSome() throws Exception {
        b.setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:26Z");
        b.setProcessPeriodicity(5, TimeUnit.minutes);
        b.setOutputFeedPeriodicity(5, TimeUnit.minutes);
        b.setProcessConcurrency(6);
        b.submitAndScheduleBundle(prism);
        Thread.sleep(15000);
        prism.getProcessHelper()
                .getProcessInstanceSuspend(Util.readEntityName(b.getProcessData()),
                        "?start=2010-01-02T01:05Z&end=2010-01-02T01:21Z");
        Thread.sleep(10000);
        ProcessInstancesResult result = prism.getProcessHelper()
                .getProcessInstanceStatus(Util.readEntityName(b.getProcessData()),
                        "?start=2010-01-02T01:00Z&end=2010-01-02T01:26Z");
        InstanceUtil.validateResponse(result, 6, 2, 4, 0, 0);

        prism.getProcessHelper()
                .getProcessInstanceResume(Util.readEntityName(b.getProcessData()),
                        "?start=2010-01-02T01:05Z&end=2010-01-02T01:16Z");
        result = prism.getProcessHelper()
                .getProcessInstanceStatus(Util.readEntityName(b.getProcessData()),
                        "?start=2010-01-02T01:00Z&end=2010-01-02T01:26Z");
        InstanceUtil.validateResponse(result, 6, 5, 1, 0, 0);
    }


    @Test(groups = {"singleCluster"})
    public void testProcessInstanceResume_resumeMany() throws Exception {
        b.setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:26Z");
        b.setProcessPeriodicity(5, TimeUnit.minutes);
        b.setOutputFeedPeriodicity(5, TimeUnit.minutes);
        b.setProcessConcurrency(6);
        b.submitAndScheduleBundle(prism);
        Thread.sleep(15000);
        prism.getProcessHelper()
                .getProcessInstanceSuspend(Util.readEntityName(b.getProcessData()),
                        "?start=2010-01-02T01:05Z&end=2010-01-02T01:20Z");
        Thread.sleep(10000);
        ProcessInstancesResult result = prism.getProcessHelper()
                .getProcessInstanceStatus(Util.readEntityName(b.getProcessData()),
                        "?start=2010-01-02T01:00Z&end=2010-01-02T01:26Z");
        InstanceUtil.validateResponse(result, 6, 2, 4, 0, 0);

        prism.getProcessHelper()
                .getProcessInstanceResume(Util.readEntityName(b.getProcessData()),
                        "?start=2010-01-02T01:05Z&end=2010-01-02T01:20Z");
        result = prism.getProcessHelper()
                .getProcessInstanceStatus(Util.readEntityName(b.getProcessData()),
                        "?start=2010-01-02T01:00Z&end=2010-01-02T01:26Z");
        InstanceUtil.validateResponse(result, 6, 6, 0, 0, 0);
    }


    @Test(groups = {"singleCluster"})
    public void testProcessInstanceResume_single() throws Exception {
        b.setProcessPeriodicity(5, TimeUnit.minutes);
        b.setOutputFeedPeriodicity(5, TimeUnit.minutes);
        b.setProcessConcurrency(1);
        b.submitAndScheduleBundle(prism);
        Thread.sleep(5000);
        prism.getProcessHelper()
                .getProcessInstanceSuspend(Util.readEntityName(b.getProcessData()),
                        "?start=2010-01-02T01:00Z");
        Thread.sleep(5000);
        prism.getProcessHelper()
                .getProcessInstanceResume(Util.readEntityName(b.getProcessData()),
                        "?start=2010-01-02T01:00Z");
        Thread.sleep(5000);
        ProcessInstancesResult r = prism.getProcessHelper()
                .getProcessInstanceStatus(Util.readEntityName(b.getProcessData()),
                        "?start=2010-01-02T01:00Z");
        InstanceUtil.validateSuccessOnlyStart(r, WorkflowStatus.RUNNING);
    }


    @Test(groups = {"singleCluster"})
    public void testProcessInstanceResume_nonExistent() throws Exception {
        b.setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:26Z");
        b.setProcessPeriodicity(5, TimeUnit.minutes);
        b.setOutputFeedPeriodicity(5, TimeUnit.minutes);
        b.setProcessConcurrency(6);
        b.submitAndScheduleBundle(prism);
        Thread.sleep(15000);
        ProcessInstancesResult r =
                prism.getProcessHelper()
                        .getProcessInstanceResume("invalidName", "?end=2010-01-02T01:15Z");
        InstanceUtil.validateSuccessWithStatusCode(r, 777);
    }


    @Test(groups = {"singleCluster"})
    public void testProcessInstanceResume_noParams() throws Exception {
        b.setProcessPeriodicity(5, TimeUnit.minutes);
        b.setOutputFeedPeriodicity(5, TimeUnit.minutes);
        b.setProcessConcurrency(6);
        b.submitAndScheduleBundle(prism);
        Thread.sleep(15000);
        ProcessInstancesResult r =
                prism.getProcessHelper().getProcessInstanceResume("invalidName", null);
        InstanceUtil.validateSuccessWithStatusCode(r, 777);
    }


    @Test(groups = {"singleCluster"})
    public void testProcessInstanceResume_deleted() throws Exception {
        b.setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:26Z");
        b.setProcessPeriodicity(5, TimeUnit.minutes);
        b.setOutputFeedPeriodicity(5, TimeUnit.minutes);
        b.setProcessConcurrency(6);
        b.submitAndScheduleBundle(prism);
        Thread.sleep(15000);
        prism.getProcessHelper().delete(URLS.DELETE_URL, b.getProcessData());
        Thread.sleep(5000);
        ProcessInstancesResult r = prism.getProcessHelper()
                .getProcessInstanceResume(Util.readEntityName(b.getProcessData()),
                        "?start=2010-01-02T01:05Z");
        InstanceUtil.validateSuccessWithStatusCode(r, 777);
    }


    @Test(groups = {"singleCluster"})
    public void testProcessInstanceResume_nonSuspended() throws Exception {
        b.setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:26Z");
        b.setProcessPeriodicity(5, TimeUnit.minutes);
        b.setOutputFeedPeriodicity(5, TimeUnit.minutes);
        b.setProcessConcurrency(6);
        b.submitAndScheduleBundle(prism);
        Thread.sleep(15000);
        prism.getProcessHelper()
                .getProcessInstanceResume(Util.readEntityName(b.getProcessData()),
                        "?start=2010-01-02T01:05Z");
    }


    @Test(groups = {"singleCluster"})
    public void testProcessInstanceResume_lastInstance() throws Exception {
        b.setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:26Z");
        b.setProcessPeriodicity(5, TimeUnit.minutes);
        b.setOutputFeedPeriodicity(5, TimeUnit.minutes);
        b.setProcessConcurrency(6);
        b.submitAndScheduleBundle(prism);
        Thread.sleep(15000);
        prism.getProcessHelper()
                .getProcessInstanceSuspend(Util.readEntityName(b.getProcessData()),
                        "?start=2010-01-02T01:25Z");
        Thread.sleep(10000);
        prism.getProcessHelper()
                .getProcessInstanceResume(Util.readEntityName(b.getProcessData()),
                        "?start=2010-01-02T01:25Z");
        ProcessInstancesResult result = prism.getProcessHelper()
                .getProcessInstanceStatus(Util.readEntityName(b.getProcessData()),
                        "?start=2010-01-02T01:00Z&end=2010-01-02T01:25Z");
        InstanceUtil.validateResponse(result, 6, 6, 0, 0, 0);
    }

    @Test(groups = {"singleCluster"})
    public void ap() throws Exception {
        b.setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:26Z");
        b.setProcessPeriodicity(5, TimeUnit.minutes);
        b.setOutputFeedPeriodicity(5, TimeUnit.minutes);
        b.setProcessConcurrency(6);
        b.submitAndScheduleBundle(prism);
        Thread.sleep(15000);
        prism.getProcessHelper()
                .getProcessInstanceSuspend(Util.readEntityName(b.getProcessData()),
                        "?start=2010-01-02T01:05Z&end=2010-01-02T01:20Z");
        Thread.sleep(10000);
        ProcessInstancesResult result = prism.getProcessHelper()
                .getProcessInstanceStatus(Util.readEntityName(b.getProcessData()),
                        "?start=2010-01-02T01:00Z&end=2010-01-02T01:26Z");
        InstanceUtil.validateResponse(result, 6, 2, 4, 0, 0);

        prism.getProcessHelper()
                .getProcessInstanceResume(Util.readEntityName(b.getProcessData()),
                        "?start=2010-01-02T01:05Z&end=2010-01-02T01:20Z");
        result = prism.getProcessHelper()
                .getProcessInstanceStatus(Util.readEntityName(b.getProcessData()),
                        "?start=2010-01-02T01:00Z&end=2010-01-02T01:26Z");
        InstanceUtil.validateResponse(result, 6, 6, 0, 0, 0);
    }


    @AfterClass(alwaysRun = true)
    public void deleteData() throws Exception {
        Util.print("in @AfterClass");

        System.setProperty("java.security.krb5.realm", "");
        System.setProperty("java.security.krb5.kdc", "");

        Bundle b = (Bundle) Util.readELBundles()[0][0];
        b = new Bundle(b, cluster.getEnvFileName(), cluster.getPrefix());
        b.setInputFeedDataPath(feedInputPath);
        String prefix = b.getFeedDataPathPrefix();
        HadoopUtil.deleteDirIfExists(prefix.substring(1), clusterFS);
    }
}
