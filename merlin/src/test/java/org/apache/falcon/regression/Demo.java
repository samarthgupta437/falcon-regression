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

import com.jcraft.jsch.JSchException;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.helpers.PrismHelper;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.XOozieClient;
import org.apache.oozie.client.rest.RestConstants;
import org.testng.annotations.Test;

import java.io.IOException;
import java.text.ParseException;
import java.util.Properties;


/**
 * Demo.
 */
public class Demo extends BaseTestClass{

    public static void main(String[] args) {
        String file = "prism.properties";
        Properties properties = Util.getPropertiesObj(file);
        System.out.println("properties = " + properties);
    }


    @Test
    public void test_sleepTill() throws ParseException, JSchException, IOException {

      InstanceUtil.sleepTill(servers.get(0),"2014-01-31T07:35Z");

    }

//    IEntityManagerHelper dataHelper = EntityHelperFactory.getEntityHelper(ENTITY_TYPE.DATA);
//    IEntityManagerHelper processHelper = EntityHelperFactory.getEntityHelper(ENTITY_TYPE.PROCESS);

    PrismHelper prismHelper = new PrismHelper("prism.properties", "");
    ColoHelper gs1001 = new ColoHelper("gs1001.config.properties", "");

/*
    @Test(dataProvider = "demo-DP")
    public void demoBundle(Bundle bundle) throws Exception {

        try {
            bundle.submitAndScheduleBundle();

        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getMessage());
        } finally {
            bundle.deleteBundle();
        }
    }

    @DataProvider(name = "demo-DP")
    public static Object[][] getTestData(Method m) throws Exception {

        return Util.readDemoBundle();
    }
*/

    @Test(enabled = false)
    public void test() throws Exception {


        XOozieClient oozieClient =
                new XOozieClient(Util.readPropertiesFile(gs1001.getEnvFileName(), "oozie_url"));
//        Bundle b = new Bundle();
//        try {
/*
            b = (Bundle) Util.readELBundles()[0][0];
            b = new Bundle(b, gs1001.getEnvFileName());
            b.setInputFeedDataPath("/samarthData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
            String startTime = instanceUtil.getTimeWrtSystemTime(-20);
            String endTime = instanceUtil.getTimeWrtSystemTime(200);

            b.setProcessValidity(startTime, endTime);
            b.setProcessPeriodicity(5, TimeUnit.minutes);
            b.setOutputFeedPeriodicity(5, TimeUnit.minutes);
            b.setOutputFeedLocationData("/examples/samarth/output-data/aggregator/aggregatedLogs/"
                    + "${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
            b.setProcessConcurrency(3);
            b.setProcessTimeOut(1, TimeUnit.minutes);
            b.submitAndScheduleBundle(prismHelper);
*/

            //	String coordID = instanceUtil.getLatestCoordinator(gs1001, b.getProcessName(),
            // ENTITY_TYPE.PROCESS);
            CoordinatorJob coord =
                    oozieClient.getCoordJobInfo("0000806-130523172649012-oozie-oozi-C");

            String actionId_1 = coord.getId() + "@" + 1;
//            String actionId_2 = coord.getId() + "@" + 2;
//            String actionId_3 = coord.getId() + "@" + 3;
            CoordinatorAction coordActionInfo_1 = oozieClient.getCoordActionInfo(actionId_1);
//            CoordinatorAction coordActionInfo_2 = oozieClient.getCoordActionInfo(actionId_2);
//            CoordinatorAction coordActionInfo_3 = oozieClient.getCoordActionInfo(actionId_3);
//            String status_1 = coordActionInfo_1.getStatus().name();
//            String status_2 = coordActionInfo_2.getStatus().name();
//
//            String status_3 = coordActionInfo_3.getStatus().name();

            Util.print(coordActionInfo_1.getId());
            Util.print(coordActionInfo_1.getJobId());
            Util.print(coordActionInfo_1.getExternalId());
            Util.print(coordActionInfo_1.getCreatedConf());
            Util.print(coordActionInfo_1.getRunConf());
            Util.print(coord.getConf());

            //Properties jobprops = OozieUtils.toProperties(coordActionInfo_1.getCreatedConf());
            // Configuration conf = new Configuration();
            //Properties jobprops = new Properties();
            //  for (Map.Entry<String, String> entry : conf) {
            //     jobprops.put(entry.getKey(), entry.getValue());
            //}
            //Properties jobprops = OozieUtils.toProperties(coordActionInfo_1.getRunConf());
            //Properties jobprops = OozieUtils.toProperties(coord.getConf());

            //	WorkflowJob jobInfo = oozieClient.getJobInfo(coordActionInfo_1.getExternalId());
            //	Properties jobprops = OozieUtils.toProperties(jobInfo.getConf());
            Util.print("-----------------");

            //	Util.print(jobInfo.getConf());
            //	Util.print("parentid: "+jobInfo.getParentId());

            //	jobprops.put(OozieClient.RERUN_FAIL_NODES, "false");
            //	jobprops.remove(OozieClient.COORDINATOR_APP_PATH);
            //	jobprops.remove(OozieClient.BUNDLE_APP_PATH);
            //oozieClient.reRun(actionId_1, jobprops);
            //oozieClient.reRun(coordActionInfo_1.getExternalId(), jobprops);
            oozieClient.reRunCoord("0000806-130523172649012-oozie-oozi-C",
                    RestConstants.JOB_COORD_RERUN_ACTION,
                    Integer.toString(1), true, true);
            System.out.println("rerun done");

//        } finally {
//            	b.deleteBundle(prismHelper);
//        }
    }
}
