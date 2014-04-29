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

import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.response.ServiceResponse;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.util.Util.URLS;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.log4j.Logger;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

@Test(groups = "embedded")
public class SubmitClusterHcatTest extends BaseTestClass {

    ColoHelper cluster = servers.get(0);
    private static Logger logger = Logger.getLogger(SubmitClusterHcatTest.class);

    // private HCatClient client;

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        removeBundles();
    }

    @Test(enabled = true, timeOut = 1800000)
    public void SubmitCluster_hcat() {
        String clusterData = "";
        String feedData01 = "";
        String feedData02 = "";
        String processData = "";

        bundles[0] = BundleUtil.getBundle(cluster, "");
        try {

            clusterData = bundles[0].getClusters().get(0);
            feedData01 = bundles[0].getDataSets().get(0);
            feedData02 = bundles[0].getDataSets().get(1);
            processData = bundles[0].getProcessData();

            //client = getHcatClient();

/*			String dbName = "falconTest" ;
            String tableName = "falconTable" ;
			createDB(dbName);
			createSampleTable(dbName,tableName);
*/
            logger.info("Cluster: " + clusterData);
            ServiceResponse r =
                    prism.getClusterHelper().submitEntity(URLS.SUBMIT_URL, clusterData);
            AssertUtil.assertSucceeded(r);

            logger.info("Feed: " + feedData01);
            r = prism.getFeedHelper().submitEntity(URLS.SUBMIT_URL, feedData01);
            AssertUtil.assertSucceeded(r);

            logger.info("Feed: " + feedData02);
            r = prism.getFeedHelper().submitEntity(URLS.SUBMIT_URL, feedData02);
            AssertUtil.assertSucceeded(r);

            logger.info("process: " + processData);
            r = prism.getProcessHelper().submitEntity(URLS.SUBMIT_URL, processData);
            AssertUtil.assertSucceeded(r);

            r = prism.getProcessHelper().schedule(URLS.SCHEDULE_URL, processData);
            AssertUtil.assertSucceeded(r);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                prism.getProcessHelper().delete(URLS.DELETE_URL, processData);
                prism.getFeedHelper().delete(URLS.DELETE_URL, feedData01);
                prism.getFeedHelper().delete(URLS.DELETE_URL, feedData02);
                prism.getClusterHelper().delete(URLS.DELETE_URL, clusterData);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

/*	private HCatClient getHcatClient() {
        try {
			HiveConf hcatConf = new HiveConf();
			hcatConf.set("hive.metastore.local", "false");
			hcatConf.setVar(HiveConf.ConfVars.METASTOREURIS, "thrift://10.14.118.32:14003");
			hcatConf.setIntVar(HiveConf.ConfVars.METASTORETHRIFTRETRIES, 3);
			hcatConf.set(HiveConf.ConfVars.SEMANTIC_ANALYZER_HOOK.varname,
					HCatSemanticAnalyzer.class.getName());
			hcatConf.set(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "false");

			hcatConf.set(HiveConf.ConfVars.PREEXECHOOKS.varname, "");
			hcatConf.set(HiveConf.ConfVars.POSTEXECHOOKS.varname, "");

			return HCatClient.create(hcatConf);
		} catch (HCatException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return client;
	}

	private void createSampleTable(String dbName, String tableName) {
		try {
			ArrayList<HCatFieldSchema> cols = new ArrayList<HCatFieldSchema>();

			cols.add(new HCatFieldSchema("id", HCatFieldSchema.Type.INT, "id comment"));
			cols.add(new HCatFieldSchema("value", HCatFieldSchema.Type.STRING, "value comment"));

			List<HCatFieldSchema> partitionSchema = Arrays.asList(
					new HCatFieldSchema("ds", HCatFieldSchema.Type.STRING, ""),
					new HCatFieldSchema("region", HCatFieldSchema.Type.STRING, "")
					);

			HCatCreateTableDesc tableDesc = HCatCreateTableDesc
					.create(dbName, tableName, cols)
					.fileFormat("rcfile")
					.ifNotExists(true)
					.comments("falcon integration test")
					.partCols(new ArrayList<HCatFieldSchema>(partitionSchema))
					.build();
			client.createTable(tableDesc);
		}
		catch (Exception e){
			e.printStackTrace();

		}


	}

	private void createDB(String dbName) {
		HCatCreateDBDesc dbDesc;
		try {
			dbDesc = HCatCreateDBDesc.create(dbName)
					.ifNotExists(true).build();
			client.createDatabase(dbDesc);
		} catch (HCatException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}*/
}
