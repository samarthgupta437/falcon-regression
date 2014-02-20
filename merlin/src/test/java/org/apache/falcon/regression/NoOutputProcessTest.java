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
import org.apache.falcon.regression.core.supportClasses.Consumer;
import org.apache.falcon.regression.core.supportClasses.ENTITY_TYPE;
import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.hadoop.fs.FileSystem;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.OozieClient;
import org.joda.time.DateTime;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;


/**
 * Null output process tests.
 */
@Test(groups = "standalone")
public class NoOutputProcessTest extends BaseTestClass {

    ColoHelper cluster;
    FileSystem clusterFS;
    OozieClient clusterOC;
    private Bundle bundle;

    public NoOutputProcessTest(){
        super();
        cluster = servers.get(0);
        clusterFS = serverFS.get(0);
        clusterOC = serverOC.get(0);
    }

    @BeforeClass(alwaysRun = true)
    public void createTestData() throws Exception {

        Util.print("in @BeforeClass");

        System.setProperty("java.security.krb5.realm", "");
        System.setProperty("java.security.krb5.kdc", "");


        Bundle b = Util.readELBundles()[0][0];
        b.generateUniqueBundle();
        b = new Bundle(b, cluster.getEnvFileName(), cluster.getPrefix());

        String startDate = "2010-01-03T00:00Z";
        String endDate = "2010-01-03T03:00Z";

        b.setInputFeedDataPath(baseHDFSDir + "/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
        String prefix = b.getFeedDataPathPrefix();
        HadoopUtil.deleteDirIfExists(prefix.substring(1), clusterFS);

        DateTime startDateJoda = new DateTime(InstanceUtil.oozieDateToDate(startDate));
        DateTime endDateJoda = new DateTime(InstanceUtil.oozieDateToDate(endDate));

        List<String> dataDates = Util.getMinuteDatesOnEitherSide(startDateJoda, endDateJoda, 20);

        for (int i = 0; i < dataDates.size(); i++)
            dataDates.set(i, prefix + dataDates.get(i));

        ArrayList<String> dataFolder = new ArrayList<String>();

        for (String dataDate : dataDates) {
            dataFolder.add(dataDate);
        }

        HadoopUtil.flattenAndPutDataInFolder(clusterFS, "src/test/resources/OozieExampleInputData/normalInput", dataFolder);
    }


    @BeforeMethod(alwaysRun = true)
    public void testName(Method method) throws Exception {
        Util.print("test name: " + method.getName());
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws Exception {
        bundle.deleteBundle(prism);
    }


    @Test(enabled = true, groups = {"singleCluster"})
    public void checkForJMSMsgWhenNoOutput() throws Exception {

        bundle = Util.readNoOutputBundles()[0][0];
        setBundleProperties();

        Util.print("attaching consumer to:   " + "FALCON.ENTITY.TOPIC");
        Consumer consumer =
                new Consumer("FALCON.ENTITY.TOPIC", cluster.getClusterHelper().getActiveMQ());
        consumer.start();
        Thread.sleep(15000);

        //wait for all the instances to complete
        InstanceUtil.waitTillInstanceReachState(clusterOC, bundle.getProcessName(), 3,
                CoordinatorAction.Status.SUCCEEDED, 20, ENTITY_TYPE.PROCESS);

        Assert.assertEquals(consumer.getMessageData().size(), 3,
                " Message for all the 3 instance not found");

        consumer.stop();

        Util.dumpConsumerData(consumer);

    }


    @Test(enabled = true, groups = {"singleCluster"})
    public void rm() throws Exception {

        bundle = Util.readELBundles()[0][0];
        setBundleProperties();

        Consumer consumerInternalMsg =
                new Consumer("FALCON.ENTITY.TOPIC", cluster.getClusterHelper().getActiveMQ());
        Consumer consumerProcess =
                new Consumer("FALCON." + bundle.getProcessName(), cluster.getClusterHelper().getActiveMQ());

        consumerInternalMsg.start();
        consumerProcess.start();

        Thread.sleep(15000);

        //wait for all the instances to complete

        InstanceUtil.waitTillInstanceReachState(clusterOC, bundle.getProcessName(), 3,
                CoordinatorAction.Status.SUCCEEDED, 20, ENTITY_TYPE.PROCESS);

        Assert.assertEquals(consumerInternalMsg.getMessageData().size(), 3,
                " Message for all the 3 instance not found");
        Assert.assertEquals(consumerProcess.getMessageData().size(), 3,
                " Message for all the 3 instance not found");

        consumerInternalMsg.stop();
        consumerProcess.stop();

        Util.dumpConsumerData(consumerInternalMsg);
        Util.dumpConsumerData(consumerProcess);
    }

    private void setBundleProperties() throws Exception {
        bundle = new Bundle(bundle, cluster.getEnvFileName(), cluster.getPrefix());
        bundle.setInputFeedDataPath(baseHDFSDir + "/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
        bundle.setProcessValidity("2010-01-03T02:30Z", "2010-01-03T02:45Z");
        bundle.setProcessPeriodicity(5, TimeUnit.minutes);
        bundle.submitAndScheduleBundle(prism);
    }
}
