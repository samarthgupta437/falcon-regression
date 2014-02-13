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
import org.apache.falcon.regression.core.generated.process.Property;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.supportClasses.ENTITY_TYPE;
import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.hadoop.fs.FileSystem;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.OozieClient;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.concurrent.TimeUnit;


public class ProcessPartitionExpVariableTest extends BaseTestClass {

    ColoHelper cluster1;
    FileSystem cluster1FS;
    OozieClient cluster1OC;
    private Bundle bundle;
    private String inputPath = "/samarthData/input";

    public ProcessPartitionExpVariableTest(){
        super();
        cluster1 = servers.get(0);
        cluster1FS = serverFS.get(0);
        cluster1OC = serverOC.get(0);
    }

    @BeforeMethod(alwaysRun = true)
    public void setUp(Method method) throws Exception {
        Util.print("test name: " + method.getName());
        bundle = Util.readELBundles()[0][0];
        bundle = new Bundle(bundle, cluster1.getEnvFileName(), cluster1.getPrefix());
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws Exception {
        bundle.deleteBundle(prism);
        HadoopUtil.deleteDirIfExists(inputPath, cluster1FS);

    }

    @Test(enabled = true)
    public void ProcessPartitionExpVariableTest_OptionalCompulsaryPartition() throws Exception {
        String startTime = InstanceUtil.getTimeWrtSystemTime(-4);
        String endTime = InstanceUtil.getTimeWrtSystemTime(30);

        bundle = bundle.getRequiredBundle(bundle, 1, 2, 1, inputPath, 1, startTime, endTime);
        bundle.setProcessData(bundle.setProcessInputNames(bundle.getProcessData(), "inputData0", "inputData"));
        Property p = new Property();
        p.setName("var1");
        p.setValue("hardCoded");

        bundle.setProcessData(bundle.addProcessProperty(bundle.getProcessData(), p));

        bundle.setProcessData(bundle.setProcessInputPartition(bundle.getProcessData(), "${var1}", "${fileTime}"));


        for (int i = 0; i < bundle.getDataSets().size(); i++)
            Util.print(bundle.getDataSets().get(i));

        Util.print(bundle.getProcessData());

        InstanceUtil.createDataWithinDatesAndPrefix(cluster1,
                InstanceUtil.oozieDateToDate(InstanceUtil.addMinsToTime(startTime, -25)),
                InstanceUtil.oozieDateToDate(InstanceUtil.addMinsToTime(endTime, 25)),
                inputPath + "/input1/", 1);

        bundle.submitAndScheduleBundle(bundle, prism, false);
        TimeUnit.SECONDS.sleep(20);

        InstanceUtil.waitTillInstanceReachState(cluster1OC,
                Util.getProcessName(bundle.getProcessData()), 2,
                CoordinatorAction.Status.SUCCEEDED, 20, ENTITY_TYPE.PROCESS);
    }

    //TODO: ProcessPartitionExpVariableTest_OptionalPartition()
    //TODO: ProcessPartitionExpVariableTest_CompulsaryPartition()
    //TODO: ProcessPartitionExpVariableTest_moreThanOnceVariable()
}
