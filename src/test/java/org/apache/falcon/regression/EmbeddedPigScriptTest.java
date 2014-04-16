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
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.response.ProcessInstancesResult;
import org.apache.falcon.regression.core.response.ProcessInstancesResult.WorkflowStatus;
import org.apache.falcon.regression.core.response.ResponseKeys;
import org.apache.falcon.regression.core.response.ServiceResponse;
import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.core.util.Util.URLS;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
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
@Test(groups = "embedded")
public class EmbeddedPigScriptTest extends BaseTestClass {

    ColoHelper cluster = servers.get(0);
    FileSystem clusterFS = serverFS.get(0);
    private String prefix;
    String pigTestDir = baseHDFSDir + "/EmbeddedPigScriptTest";
    String pigScriptDir = pigTestDir + "/EmbeddedPigScriptTest/pig";
    String pigScriptLocation = pigScriptDir + "/id.pig";
    private static final Logger logger = Logger.getLogger(EmbeddedPigScriptTest.class);

    @BeforeClass(alwaysRun = true)
    public void createTestData() throws Exception {

        logger.info("in @BeforeClass");
        //copy pig script
        HadoopUtil.uploadDir(clusterFS, pigScriptDir, OSUtil.RESOURCES + "pig");

        Bundle bundle = Util.readELBundles()[0][0];
        bundle.generateUniqueBundle();
        bundle = new Bundle(bundle, cluster);

        String startDate = "2010-01-01T20:00Z";
        String endDate = "2010-01-03T01:04Z";

        bundle.setInputFeedDataPath(pigTestDir + "/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
        prefix = bundle.getFeedDataPathPrefix();
        HadoopUtil.deleteDirIfExists(prefix.substring(1), clusterFS);

        DateTime startDateJoda = new DateTime(InstanceUtil.oozieDateToDate(startDate));
        DateTime endDateJoda = new DateTime(InstanceUtil.oozieDateToDate(endDate));

        List<String> dataDates = Util.getMinuteDatesOnEitherSide(startDateJoda, endDateJoda, 20);

        for (int i = 0; i < dataDates.size(); i++)
            dataDates.set(i, prefix + dataDates.get(i));

        ArrayList<String> dataFolder = new ArrayList<String>();

        for (String dataDate : dataDates) dataFolder.add(dataDate);

        HadoopUtil.flattenAndPutDataInFolder(clusterFS, OSUtil.NORMAL_INPUT, dataFolder);
    }

    @BeforeMethod(alwaysRun = true)
    public void setUp(Method method) throws Exception {
        logger.info("test name: " + method.getName());
        bundles[0] = Util.readELBundles()[0][0];
        bundles[0] = new Bundle(bundles[0], cluster);
        bundles[0].setInputFeedDataPath(pigTestDir + "/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
        bundles[0].setOutputFeedLocationData(pigTestDir + "/output-data/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
        bundles[0].setProcessWorkflow(pigScriptLocation);
        bundles[0].setProcessData(bundles[0].setProcessInputNames(bundles[0].getProcessData(), "INPUT"));
        bundles[0].setProcessData(bundles[0].setProcessOutputNames(bundles[0].getProcessData(), "OUTPUT"));
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        removeBundles();
    }

    @Test(groups = {"singleCluster"})
    public void getResumedProcessInstance() throws Exception {
        bundles[0].setProcessValidity("2010-01-02T01:00Z", "2010-01-02T02:30Z");
        bundles[0].setProcessPeriodicity(5, TimeUnit.minutes);
        bundles[0].setOutputFeedPeriodicity(5, TimeUnit.minutes);
        bundles[0].setProcessConcurrency(3);

        final Process processElement = InstanceUtil.getProcessElement(bundles[0]);
        final Properties properties = new Properties();
        final Property property = new Property();
        property.setName("queueName");
        property.setValue("default");
        properties.addProperty(property);
        processElement.setProperties(properties);
        processElement.getWorkflow().setEngine(EngineType.PIG);
        InstanceUtil.writeProcessElement(bundles[0], processElement);

        bundles[0].submitAndScheduleBundle(prism);
        Thread.sleep(15000);
        prism.getProcessHelper().suspend(URLS.SUSPEND_URL, bundles[0].getProcessData());
        Thread.sleep(15000);
        ServiceResponse status =
                prism.getProcessHelper().getStatus(URLS.STATUS_URL, bundles[0].getProcessData());
        Assert.assertTrue(status.getMessage().contains("SUSPENDED"), "Process not suspended.");
        prism.getProcessHelper().resume(URLS.RESUME_URL, bundles[0].getProcessData());
        Thread.sleep(15000);
        ProcessInstancesResult r = prism.getProcessHelper()
                .getRunningInstance(URLS.INSTANCE_RUNNING, Util.readEntityName(bundles[0].getProcessData()));
        InstanceUtil.validateSuccess(r, bundles[0], WorkflowStatus.RUNNING);
    }

    @Test(groups = {"singleCluster"})
    public void getSuspendedProcessInstance() throws Exception {
        bundles[0].setProcessValidity("2010-01-02T01:00Z", "2010-01-02T02:30Z");
        bundles[0].setProcessPeriodicity(5, TimeUnit.minutes);
        bundles[0].setOutputFeedPeriodicity(5, TimeUnit.minutes);
        bundles[0].setProcessConcurrency(3);

        final Process processElement = InstanceUtil.getProcessElement(bundles[0]);
        final Properties properties = new Properties();
        final Property property = new Property();
        property.setName("queueName");
        property.setValue("default");
        properties.addProperty(property);
        processElement.setProperties(properties);
        processElement.getWorkflow().setEngine(EngineType.PIG);
        InstanceUtil.writeProcessElement(bundles[0], processElement);

        bundles[0].submitAndScheduleBundle(prism);
        prism.getProcessHelper().suspend(URLS.SUSPEND_URL, bundles[0].getProcessData());
        Thread.sleep(10000);
        ProcessInstancesResult r = prism.getProcessHelper()
                .getRunningInstance(URLS.INSTANCE_RUNNING, Util.readEntityName(bundles[0].getProcessData()));
        InstanceUtil.validateSuccessWOInstances(r);
    }

    @Test(groups = {"singleCluster"})
    public void getRunningProcessInstance() throws Exception {
        bundles[0].setCLusterColo("ua2");
        bundles[0].setProcessValidity("2010-01-02T01:00Z", "2010-01-02T02:30Z");
        bundles[0].setProcessPeriodicity(5, TimeUnit.minutes);

        final Process processElement = InstanceUtil.getProcessElement(bundles[0]);
        final Properties properties = new Properties();
        final Property property = new Property();
        property.setName("queueName");
        property.setValue("default");
        properties.addProperty(property);
        processElement.setProperties(properties);
        processElement.getWorkflow().setEngine(EngineType.PIG);
        InstanceUtil.writeProcessElement(bundles[0], processElement);

        bundles[0].submitAndScheduleBundle(prism);
        Thread.sleep(5000);
        ProcessInstancesResult r = prism.getProcessHelper()
                .getRunningInstance(URLS.INSTANCE_RUNNING, Util.readEntityName(bundles[0].getProcessData()));
        InstanceUtil.validateSuccess(r, bundles[0], WorkflowStatus.RUNNING);
    }

    @Test(groups = {"singleCluster"})
    public void getKilledProcessInstance() throws Exception {
        final Process processElement = InstanceUtil.getProcessElement(bundles[0]);
        final Properties properties = new Properties();
        final Property property = new Property();
        property.setName("queueName");
        property.setValue("default");
        properties.addProperty(property);
        processElement.setProperties(properties);
        processElement.getWorkflow().setEngine(EngineType.PIG);
        InstanceUtil.writeProcessElement(bundles[0], processElement);

        bundles[0].submitAndScheduleBundle(prism);
        prism.getProcessHelper().delete(URLS.DELETE_URL, bundles[0].getProcessData());
        Thread.sleep(5000);
        ProcessInstancesResult r = prism.getProcessHelper()
                .getRunningInstance(URLS.INSTANCE_RUNNING, Util.readEntityName(bundles[0].getProcessData()));
        AssertJUnit.assertTrue(r.getStatusCode() == ResponseKeys.PROCESS_NOT_FOUND);
    }

    @Test(groups = {"singleCluster"})
    public void getSucceededProcessInstance() throws Exception {
        bundles[0].setProcessValidity("2010-01-02T01:00Z", "2010-01-02T02:30Z");
        bundles[0].setProcessPeriodicity(5, TimeUnit.minutes);

        final Process processElement = InstanceUtil.getProcessElement(bundles[0]);
        final Properties properties = new Properties();
        final Property property = new Property();
        property.setName("queueName");
        property.setValue("default");
        properties.addProperty(property);
        processElement.setProperties(properties);
        processElement.getWorkflow().setEngine(EngineType.PIG);
        InstanceUtil.writeProcessElement(bundles[0], processElement);

        bundles[0].submitAndScheduleBundle(prism);
        Thread.sleep(5000);
        ProcessInstancesResult r = prism.getProcessHelper()
                .getRunningInstance(URLS.INSTANCE_RUNNING, Util.readEntityName(bundles[0].getProcessData()));
        InstanceUtil.validateSuccess(r, bundles[0], WorkflowStatus.RUNNING);

        Job.Status status = null;
        int counter = 100;
        // increase the wait for windows
        if (OSUtil.IS_WINDOWS) {
            counter = 200;
        }
        for (int i = 0; i < counter; i++) {
            status = InstanceUtil.getDefaultCoordinatorStatus(cluster, Util.getProcessName(bundles[0].getProcessData()), 0);
            if (status == Job.Status.SUCCEEDED) {
                break;
            }
            Thread.sleep(30000);
        }

        Assert.assertEquals(status, Job.Status.SUCCEEDED,
                "The job did not succeeded even in long time");

        Thread.sleep(5000);
        r = prism.getProcessHelper()
                .getRunningInstance(URLS.INSTANCE_STATUS, Util.readEntityName(bundles[0].getProcessData()));
        InstanceUtil.validateSuccessWOInstances(r);
    }

    @AfterClass(alwaysRun = true)
    public void deleteData() throws Exception {
        logger.info("in @AfterClass");
        HadoopUtil.deleteDirIfExists(prefix.substring(1), clusterFS);
    }
}
