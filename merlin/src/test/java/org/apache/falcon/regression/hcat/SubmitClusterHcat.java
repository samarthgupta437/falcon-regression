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

package org.apache.falcon.regression.hcat;

import org.apache.hcatalog.api.HCatClient;
import org.apache.falcon.regression.core.util.HCatUtil;
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.response.ServiceResponse;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.core.util.Util.URLS;
import org.apache.falcon.regression.testHelper.BaseSingleClusterTests;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.testng.annotations.Test;

import java.io.IOException;

public class SubmitClusterHcat extends BaseTestClass {

    public static HCatClient cli;
    public SubmitClusterHcat() throws IOException {
        super();
    }

    @Test(enabled = true, timeOut = 1800000)
    public void SubmitCluster_hcat() {
        String cluster = "";
        String feed01 = "";
        //String feed02 = "";
        //String process = "";

        cli=HCatUtil.getHCatClient();
        Bundle b = Util.getBundle(servers.get(0),"hcat_2");
        try {

            cluster = b.getClusters().get(0);
            feed01 = b.getDataSets().get(0);
            HCatUtil.createEmptyTable(cli,"default","mytablepart3");
            /*
            feed02 = b.getDataSets().get(1);
            process = b.getProcessData(); */

            //client = getHcatClient();

/*			String dbName = "falconTest" ;
            String tableName = "falconTable" ;
			createDB(dbName);
			createSampleTable(dbName,tableName);
*/
            System.out.println("Cluster: " + cluster);
            ServiceResponse r =
                    prism.getClusterHelper().submitEntity(URLS.SUBMIT_URL, cluster);
            Util.assertSucceeded(r);

            System.out.println("Feed: " + feed01);
            r = prism.getFeedHelper().submitEntity(URLS.SUBMIT_URL, feed01);
            Util.assertSucceeded(r);

            /*
            System.out.println("Feed: " + feed02);
            r = prism.getFeedHelper().submitEntity(URLS.SUBMIT_URL, feed02);
            Util.assertSucceeded(r);

            System.out.println("process: " + process);
            r = prism.getProcessHelper().submitEntity(URLS.SUBMIT_URL, process);
            Util.assertSucceeded(r);

            r = prism.getProcessHelper().schedule(URLS.SCHEDULE_URL, process);
            Util.assertSucceeded(r);
            */


        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                //prism.getProcessHelper().delete(URLS.DELETE_URL, process);
                prism.getFeedHelper().delete(URLS.DELETE_URL, feed01);
                //prism.getFeedHelper().delete(URLS.DELETE_URL, feed02);
                prism.getClusterHelper().delete(URLS.DELETE_URL, cluster);
                HCatUtil.deleteTable(cli, "default", "mytablepart3");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}