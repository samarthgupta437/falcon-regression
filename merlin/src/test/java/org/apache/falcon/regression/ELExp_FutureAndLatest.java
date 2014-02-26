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
import org.apache.falcon.regression.core.supportClasses.ENTITY_TYPE;
import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.hadoop.fs.FileSystem;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.OozieClient;
import org.joda.time.DateTime;
import org.testng.annotations.*;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

/**
 * EL Expression test.
 */
@Test(groups = "embedded")
public class ELExp_FutureAndLatest extends BaseTestClass {

    ColoHelper cluster = servers.get(0);
    FileSystem clusterFS = serverFS.get(0);
    OozieClient clusterOC = serverOC.get(0);
    private String prefix;

    @BeforeClass(alwaysRun = true)
    public void createTestData() throws Exception {
        Util.print("in @BeforeClass");

        System.setProperty("java.security.krb5.realm", "");
        System.setProperty("java.security.krb5.kdc", "");

        Bundle b = Util.readELBundles()[0][0];
        b.generateUniqueBundle();
        b = new Bundle(b, cluster.getEnvFileName(), cluster.getPrefix());

        String startDate = InstanceUtil.getTimeWrtSystemTime(-150);
        String endDate = InstanceUtil.getTimeWrtSystemTime(100);

        b.setInputFeedDataPath(baseHDFSDir + "/ELExp_latest/testData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
        prefix = b.getFeedDataPathPrefix();
        HadoopUtil.deleteDirIfExists(prefix.substring(1), clusterFS);

        DateTime startDateJoda = new DateTime(InstanceUtil.oozieDateToDate(startDate));
        DateTime endDateJoda = new DateTime(InstanceUtil.oozieDateToDate(endDate));

        List<String> dataDates = Util.getMinuteDatesOnEitherSide(startDateJoda, endDateJoda, 1);

        for (int i = 0; i < dataDates.size(); i++)
            dataDates.set(i, prefix + dataDates.get(i));

        List<String> dataFolder = new ArrayList<String>();

        for (String dataDate : dataDates) {
            dataFolder.add(dataDate);
        }
        HadoopUtil.flattenAndPutDataInFolder(clusterFS, "src/test/resources/OozieExampleInputData/normalInput", dataFolder);
    }

    @BeforeMethod(alwaysRun = true)
    public void setUp(Method method) throws Exception {
        Util.print("test name: " + method.getName());
        bundles[0] = Util.readELBundles()[0][0];
        bundles[0] = new Bundle(bundles[0], cluster.getEnvFileName(), cluster.getPrefix());
        bundles[0].setInputFeedDataPath(baseHDFSDir + "/ELExp_latest/testData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
        bundles[0].setInputFeedPeriodicity(5, TimeUnit.minutes);
        bundles[0].setInputFeedValidity("2010-04-01T00:00Z", "2015-04-01T00:00Z");
        String processStart = InstanceUtil.getTimeWrtSystemTime(-3);
        String processEnd = InstanceUtil.getTimeWrtSystemTime(8);
        Util.print("processStart: " + processStart + " processEnd: " + processEnd);
        bundles[0].setProcessValidity(processStart, processEnd);
        bundles[0].setProcessPeriodicity(5, TimeUnit.minutes);
    }

    @Test(groups = {"singleCluster"})
    public void latestTest() throws Exception {
        bundles[0].setDatasetInstances("latest(-3)", "latest(0)");
        bundles[0].submitAndScheduleBundle(prism);
        InstanceUtil.waitTillInstanceReachState(clusterOC, bundles[0].getProcessName(), 3,
                CoordinatorAction.Status.SUCCEEDED, 20, ENTITY_TYPE.PROCESS);
    }

    @Test(groups = {"singleCluster"})
    public void futureTest() throws Exception {
        bundles[0].setDatasetInstances("future(0,10)", "future(3,10)");
        bundles[0].submitAndScheduleBundle(prism);
        InstanceUtil.waitTillInstanceReachState(clusterOC, bundles[0].getProcessName(), 3,
                CoordinatorAction.Status.SUCCEEDED, 20, ENTITY_TYPE.PROCESS);
    }

    @AfterClass(alwaysRun = true)
    public void deleteData() throws Exception {
        Util.print("in @AfterClass");
        HadoopUtil.deleteDirIfExists(prefix.substring(1), clusterFS);
    }
}
