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
import org.apache.falcon.regression.core.helpers.PrismHelper;
import org.apache.falcon.regression.core.supportClasses.ENTITY_TYPE;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.Util;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;


public class ProcessPartitionExpVariableTest {

    PrismHelper prismHelper = new PrismHelper("prism.properties");
    ColoHelper ivoryqa1 = new ColoHelper("ivoryqa-1.config.properties");

    @BeforeMethod(alwaysRun = true)
    public void testName(Method method) {
        Util.print("test name: " + method.getName());
    }


    @Test(enabled = true)
    public void ProcessPartitionExpVariableTest_OptionalCompulsaryPartition() throws Exception {
        Bundle b = new Bundle();

        try {

            String startTime = InstanceUtil.getTimeWrtSystemTime(-4);
            String endTime = InstanceUtil.getTimeWrtSystemTime(30);


            b = (Bundle) Util.readELBundles()[0][0];
            b = new Bundle(b, ivoryqa1.getEnvFileName());

            b = b.getRequiredBundle(b, 1, 2, 1, "/samarthData/input", 1, startTime, endTime);


            b.setProcessData(b.setProcessInputNames(b.getProcessData(), "inputData0", "inputData"));

            Property p = new Property();
            p.setName("var1");
            p.setValue("hardCoded");

            b.setProcessData(b.addProcessProperty(b.getProcessData(), p));

            b.setProcessData(
                    b.setProcessInputPartition(b.getProcessData(), "${var1}", "${fileTime}"));


            for (int i = 0; i < b.getClusters().size(); i++)
                Util.print(b.getDataSets().get(i));

            for (int i = 0; i < b.getDataSets().size(); i++)
                Util.print(b.getDataSets().get(i));

            Util.print(b.getProcessData());

            InstanceUtil.createDataWithinDatesAndPrefix(ivoryqa1,
                    InstanceUtil.oozieDateToDate(InstanceUtil.addMinsToTime(startTime, -25)),
                    InstanceUtil.oozieDateToDate(InstanceUtil.addMinsToTime(endTime, 25)),
                    "/samarthData/input/input1/",
                    1);
            //instanceUtil.createEmptyDirWithinDatesAndPrefix(ivoryqa1,
            // instanceUtil.oozieDateToDate(instanceUtil
            // .addMinsToTime(startTime, -25)), instanceUtil.oozieDateToDate(instanceUtil
            // .addMinsToTime(endTime,
            // 25)), "/samarthData/input/input0/", 1);


            b.submitAndScheduleBundle(b, prismHelper, false);

            Thread.sleep(20000);


            InstanceUtil
                    .waitTillInstanceReachState(ivoryqa1, Util.getProcessName(b.getProcessData()),
                            2,
                            org.apache.oozie.client.CoordinatorAction.Status.SUCCEEDED, 20,
                            ENTITY_TYPE.PROCESS);


        } finally {
            b.deleteBundle(prismHelper);
            Util.HDFSCleanup(ivoryqa1, "/samarthData/input/");

        }
    }

    @Test(enabled = false)
    public void ProcessPartitionExpVariableTest_OptionalPartition() {

    }

    @Test(enabled = false)
    public void ProcessPartitionExpVariableTest_CompulsaryPartition() {

    }


    @Test(enabled = false)
    public void ProcessPartitionExpVariableTest_moreThanOnceVariable() {

    }
}
