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
import org.apache.falcon.regression.core.generated.dependencies.Frequency.TimeUnit;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.response.ServiceResponse;
import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.core.util.Util.URLS;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.hadoop.fs.FileSystem;
import org.apache.oozie.client.Job.Status;
import org.joda.time.DateTime;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;


@Test(groups = "standalone")
public class RescheduleProcessInFinalStates extends BaseTestClass {

    private Bundle bundle;
    ColoHelper cluster1;
    FileSystem cluster1FS;

    public RescheduleProcessInFinalStates(){
        super();
        cluster1 = servers.get(0);
        cluster1FS = serverFS.get(0);
    }

    @BeforeClass(alwaysRun = true)
    public void createTestData() throws Exception {

        Util.print("in @BeforeClass");

        System.setProperty("java.security.krb5.realm", "");
        System.setProperty("java.security.krb5.kdc", "");


        Bundle b = Util.readELBundles()[0][0];
        b.generateUniqueBundle();
        b = new Bundle(b, cluster1.getEnvFileName(), cluster1.getPrefix());

        String startDate = "2010-01-01T20:00Z";
        String endDate = "2010-01-03T01:04Z";

        b.setInputFeedDataPath(baseHDFSDir +"/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
        String prefix = b.getFeedDataPathPrefix();
        HadoopUtil.deleteDirIfExists(prefix.substring(1), cluster1FS);

        DateTime startDateJoda = new DateTime(InstanceUtil.oozieDateToDate(startDate));
        DateTime endDateJoda = new DateTime(InstanceUtil.oozieDateToDate(endDate));

        List<String> dataDates = Util.getMinuteDatesOnEitherSide(startDateJoda, endDateJoda, 20);

        for (int i = 0; i < dataDates.size(); i++)
            dataDates.set(i, prefix + dataDates.get(i));

        ArrayList<String> dataFolder = new ArrayList<String>();

        for (int i = 0; i < dataDates.size(); i++)
            dataFolder.add(dataDates.get(i));

        HadoopUtil.flattenAndPutDataInFolder(cluster1FS, "src/test/resources/OozieExampleInputData/normalInput", dataFolder);
    }


    @BeforeMethod(alwaysRun = true)
    public void setUp(Method method) throws Exception {
        Util.print("test name: " + method.getName());
        bundle = Util.readELBundles()[0][0];
        bundle = new Bundle(bundle, cluster1.getEnvFileName(), cluster1.getPrefix());
        bundle.setInputFeedDataPath(baseHDFSDir + "/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
        bundle.setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:15Z");
        bundle.setProcessPeriodicity(5, TimeUnit.minutes);
        bundle.setOutputFeedPeriodicity(5, TimeUnit.minutes);
        bundle.setOutputFeedLocationData(baseHDFSDir + "/output-data/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
        bundle.setProcessConcurrency(6);
        bundle.submitAndScheduleBundle(prism);

    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws Exception {
        bundle.deleteBundle(prism);
    }


    // DWE mean Done With Error In Oozie
    @Test(enabled = false)
    public void rescheduleSucceeded() throws Exception {
        InstanceUtil.waitForBundleToReachState(cluster1, bundle.getProcessName(), Status.SUCCEEDED, 20);
        Thread.sleep(20000);

        //delete the process
        prism.getProcessHelper().delete(URLS.DELETE_URL, bundle.getProcessData());

        //check ... get definition should return process not found
        ServiceResponse r = prism.getProcessHelper()
                .getEntityDefinition(URLS.GET_ENTITY_DEFINITION, bundle.getProcessData());
        Assert.assertTrue(r.getMessage().contains("(process) not found"));
        Util.assertFailed(r);

        //submit and schedule process again
        r = prism.getProcessHelper().submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, bundle.getProcessData());
        Util.assertSucceeded(r);
        Thread.sleep(20000);
        InstanceUtil.waitForBundleToReachState(cluster1, bundle.getProcessName(), Status.SUCCEEDED, 20);

    }

    @Test(enabled = false)
    public void rescheduleFailed() throws Exception {
        InstanceUtil.waitForBundleToReachState(cluster1, bundle.getProcessName(), Status.SUCCEEDED, 20);
        Thread.sleep(20000);

        //delete the process
        prism.getProcessHelper().delete(URLS.DELETE_URL, bundle.getProcessData());

        //check ... get definition should return process not found
        ServiceResponse r = prism.getProcessHelper()
                        .getEntityDefinition(URLS.GET_ENTITY_DEFINITION, bundle.getProcessData());
        Assert.assertTrue(r.getMessage().contains("(process) not found"));
        Util.assertFailed(r);

        //submit and schedule process again
        r = prism.getProcessHelper().submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, bundle.getProcessData());
        Util.assertSucceeded(r);
        Thread.sleep(20000);
        InstanceUtil.waitForBundleToReachState(cluster1, bundle.getProcessName(), Status.SUCCEEDED, 20);
    }

    @Test(enabled = false)
    public void rescheduleDWE() throws Exception {
        Thread.sleep(20000);

        prism.getProcessHelper()
                .getProcessInstanceKill(Util.readEntityName(bundle.getProcessData()), "?start=2010-01-02T01:05Z");

        InstanceUtil.waitForBundleToReachState(cluster1, bundle.getProcessName(), Status.DONEWITHERROR, 20);

        Thread.sleep(20000);

        //delete the process
        prism.getProcessHelper().delete(URLS.DELETE_URL, bundle.getProcessData());

        //check ... get definition should return process not found
        ServiceResponse r = prism.getProcessHelper()
                        .getEntityDefinition(URLS.GET_ENTITY_DEFINITION, bundle.getProcessData());
        Assert.assertTrue(r.getMessage().contains("(process) not found"));
        Util.assertFailed(r);

        //submit and schedule process again
        r = prism.getProcessHelper().submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, bundle.getProcessData());
        Util.assertSucceeded(r);
        Thread.sleep(20000);
        InstanceUtil.waitForBundleToReachState(cluster1, bundle.getProcessName(), Status.SUCCEEDED, 20);

    }

    //TODO: rescheduleSucceededAndDWEOnSomeColo()
    //TODO: rescheduleOnlyOnOneAndSucceeded()
    //TODO: rescheduleOnlyOnOneAndKilled()

    @Test(enabled = false)
    public void rescheduleKilled() throws Exception {
        Thread.sleep(15000);

        prism.getProcessHelper().delete(URLS.DELETE_URL, bundle.getProcessData());


        InstanceUtil.waitForBundleToReachState(cluster1, bundle.getProcessName(), Status.KILLED, 20);

        Thread.sleep(20000);

        //check ... get definition should return process not found
        ServiceResponse r = prism.getProcessHelper()
                        .getEntityDefinition(URLS.GET_ENTITY_DEFINITION, bundle.getProcessData());
        Assert.assertTrue(r.getMessage().contains("(process) not found"));
        Util.assertFailed(r);

        //submit and schedule process again
        r = prism.getProcessHelper().submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, bundle.getProcessData());
        Util.assertSucceeded(r);
        Thread.sleep(20000);
        InstanceUtil.waitForBundleToReachState(cluster1, bundle.getProcessName(), Status.SUCCEEDED, 20);
    }
}
