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
import org.apache.falcon.regression.core.generated.process.EngineType;
import org.apache.falcon.regression.core.generated.process.Process;
import org.apache.falcon.regression.core.generated.process.Properties;
import org.apache.falcon.regression.core.generated.process.Property;
import org.apache.falcon.regression.core.response.ProcessInstancesResult;
import org.apache.falcon.regression.core.response.ProcessInstancesResult.WorkflowStatus;
import org.apache.falcon.regression.core.response.ServiceResponse;
import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.core.util.Util.URLS;
import org.apache.falcon.regression.testHelper.BaseSingleClusterTests;
import org.apache.oozie.client.Job;
import org.joda.time.DateTime;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.*;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

/**
 * Embedded pig script test.
 */
public class EmbeddedPigScriptTest extends BaseSingleClusterTests {

    private Bundle bundle;
    private String prefix;

    @BeforeClass(alwaysRun = true)
    public void createTestData() throws Exception {

        Util.print("in @BeforeClass");

        System.setProperty("java.security.krb5.realm", "");
        System.setProperty("java.security.krb5.kdc", "");

        Bundle bundle = Util.readELBundles()[0][0];
        bundle.generateUniqueBundle();
        bundle = new Bundle(bundle, server1.getEnvFileName(), server1.getPrefix());

        String startDate = "2010-01-01T20:00Z";
        String endDate = "2010-01-03T01:04Z";

        bundle.setInputFeedDataPath(baseHDFSDir + "/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
        prefix = bundle.getFeedDataPathPrefix();
        HadoopUtil.deleteDirIfExists(prefix.substring(1), server1FS);

        DateTime startDateJoda = new DateTime(InstanceUtil.oozieDateToDate(startDate));
        DateTime endDateJoda = new DateTime(InstanceUtil.oozieDateToDate(endDate));

        List<String> dataDates = Util.getMinuteDatesOnEitherSide(startDateJoda, endDateJoda, 20);

        for (int i = 0; i < dataDates.size(); i++)
            dataDates.set(i, prefix + dataDates.get(i));

        ArrayList<String> dataFolder = new ArrayList<String>();

        for (String dataDate : dataDates) dataFolder.add(dataDate);

        HadoopUtil.flattenAndPutDataInFolder(server1FS, "src/test/resources/OozieExampleInputData/normalInput", dataFolder);
    }

    @BeforeMethod(alwaysRun = true)
    public void setUp(Method method) throws Exception {
        Util.print("test name: " + method.getName());
        bundle = Util.readELBundles()[0][0];
        bundle = new Bundle(bundle, server1.getEnvFileName(), server1.getPrefix());
        bundle.setInputFeedDataPath(baseHDFSDir + "/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
        bundle.setOutputFeedLocationData(baseHDFSDir + "/output-data/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
        bundle.setProcessWorkflow("/examples/apps/pig/id.pig");
        bundle.setProcessData(bundle.setProcessInputNames(bundle.getProcessData(), "INPUT"));
        bundle.setProcessData(bundle.setProcessOutputNames(bundle.getProcessData(), "OUTPUT"));
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws Exception {
        bundle.deleteBundle(prism);
    }

    @Test(groups = {"singleCluster"})
    public void getResumedProcessInstance() throws Exception {
        bundle.setProcessValidity("2010-01-02T01:00Z", "2010-01-02T02:30Z");
        bundle.setProcessPeriodicity(5, TimeUnit.minutes);
        bundle.setOutputFeedPeriodicity(5, TimeUnit.minutes);
        bundle.setProcessConcurrency(3);

        final Process processElement = InstanceUtil.getProcessElement(bundle);
        final Properties properties = new Properties();
        final Property property = new Property();
        property.setName("queueName");
        property.setValue("default");
        properties.addProperty(property);
        processElement.setProperties(properties);
        processElement.getWorkflow().setEngine(EngineType.PIG);
        InstanceUtil.writeProcessElement(bundle, processElement);

        bundle.submitAndScheduleBundle(prism);
        Thread.sleep(15000);
        prism.getProcessHelper().suspend(URLS.SUSPEND_URL, bundle.getProcessData());
        Thread.sleep(15000);
        ServiceResponse status =
                prism.getProcessHelper().getStatus(URLS.STATUS_URL, bundle.getProcessData());
        Assert.assertTrue(status.getMessage().contains("SUSPENDED"), "Process not suspended.");
        prism.getProcessHelper().resume(URLS.RESUME_URL, bundle.getProcessData());
        Thread.sleep(15000);
        ProcessInstancesResult r = prism.getProcessHelper()
                .getRunningInstance(URLS.INSTANCE_RUNNING, Util.readEntityName(bundle.getProcessData()));
        InstanceUtil.validateSuccess(r, bundle, WorkflowStatus.RUNNING);
    }

    @Test(groups = {"singleCluster"})
    public void getSuspendedProcessInstance() throws Exception {
        bundle.setProcessValidity("2010-01-02T01:00Z", "2010-01-02T02:30Z");
        bundle.setProcessPeriodicity(5, TimeUnit.minutes);
        bundle.setOutputFeedPeriodicity(5, TimeUnit.minutes);
        bundle.setProcessConcurrency(3);

        final Process processElement = InstanceUtil.getProcessElement(bundle);
        final Properties properties = new Properties();
        final Property property = new Property();
        property.setName("queueName");
        property.setValue("default");
        properties.addProperty(property);
        processElement.setProperties(properties);
        processElement.getWorkflow().setEngine(EngineType.PIG);
        InstanceUtil.writeProcessElement(bundle, processElement);

        bundle.submitAndScheduleBundle(prism);
        prism.getProcessHelper().suspend(URLS.SUSPEND_URL, bundle.getProcessData());
        Thread.sleep(5000);
        ProcessInstancesResult r = prism.getProcessHelper()
                .getRunningInstance(URLS.INSTANCE_RUNNING, Util.readEntityName(bundle.getProcessData()));
        InstanceUtil.validateSuccessWOInstances(r);
    }

    @Test(groups = {"singleCluster"})
    public void getRunningProcessInstance() throws Exception {
        bundle.setCLusterColo("ua2");
        bundle.setProcessValidity("2010-01-02T01:00Z", "2010-01-02T02:30Z");
        bundle.setProcessPeriodicity(5, TimeUnit.minutes);

        final Process processElement = InstanceUtil.getProcessElement(bundle);
        final Properties properties = new Properties();
        final Property property = new Property();
        property.setName("queueName");
        property.setValue("default");
        properties.addProperty(property);
        processElement.setProperties(properties);
        processElement.getWorkflow().setEngine(EngineType.PIG);
        InstanceUtil.writeProcessElement(bundle, processElement);

        bundle.submitAndScheduleBundle(prism);
        Thread.sleep(5000);
        ProcessInstancesResult r = prism.getProcessHelper()
                .getRunningInstance(URLS.INSTANCE_RUNNING, Util.readEntityName(bundle.getProcessData()));
        InstanceUtil.validateSuccess(r, bundle, WorkflowStatus.RUNNING);
    }

    @Test(groups = {"singleCluster"})
    public void getKilledProcessInstance() throws Exception {
        final Process processElement = InstanceUtil.getProcessElement(bundle);
        final Properties properties = new Properties();
        final Property property = new Property();
        property.setName("queueName");
        property.setValue("default");
        properties.addProperty(property);
        processElement.setProperties(properties);
        processElement.getWorkflow().setEngine(EngineType.PIG);
        InstanceUtil.writeProcessElement(bundle, processElement);

        bundle.submitAndScheduleBundle(prism);
        prism.getProcessHelper().delete(URLS.DELETE_URL, bundle.getProcessData());
        Thread.sleep(5000);
        ProcessInstancesResult r = prism.getProcessHelper()
                .getRunningInstance(URLS.INSTANCE_RUNNING, Util.readEntityName(bundle.getProcessData()));
        AssertJUnit.assertTrue(r.getStatusCode() == 777);
    }

    @Test(groups = {"singleCluster"})
    public void getSucceededProcessInstance() throws Exception {
        bundle.setProcessValidity("2010-01-02T01:00Z", "2010-01-02T02:30Z");
        bundle.setProcessPeriodicity(5, TimeUnit.minutes);

        final Process processElement = InstanceUtil.getProcessElement(bundle);
        final Properties properties = new Properties();
        final Property property = new Property();
        property.setName("queueName");
        property.setValue("default");
        properties.addProperty(property);
        processElement.setProperties(properties);
        processElement.getWorkflow().setEngine(EngineType.PIG);
        InstanceUtil.writeProcessElement(bundle, processElement);

        bundle.submitAndScheduleBundle(prism);
        Thread.sleep(5000);
        ProcessInstancesResult r = prism.getProcessHelper()
                .getRunningInstance(URLS.INSTANCE_RUNNING, Util.readEntityName(bundle.getProcessData()));
        InstanceUtil.validateSuccess(r, bundle, WorkflowStatus.RUNNING);

        Job.Status status = null;
        for (int i = 0; i < 60; i++) {
            status = InstanceUtil.getDefaultCoordinatorStatus(server1, Util.getProcessName(bundle.getProcessData()), 0);
            if (status == Job.Status.SUCCEEDED) {
                break;
            }
            Thread.sleep(30000);
        }

        Assert.assertTrue(status != Job.Status.SUCCEEDED, "The job did not succeeded even in long time");

        Thread.sleep(5000);
        r = prism.getProcessHelper()
                .getRunningInstance(URLS.INSTANCE_STATUS, Util.readEntityName(bundle.getProcessData()));
        InstanceUtil.validateSuccessWOInstances(r);
    }

    @AfterClass(alwaysRun = true)
    public void deleteData() throws Exception {
        Util.print("in @AfterClass");
        HadoopUtil.deleteDirIfExists(prefix.substring(1), server1FS);
    }
}