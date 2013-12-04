package com.inmobi.qa.falcon.hcat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hcatalog.api.HCatClient;
import org.apache.hcatalog.api.HCatCreateDBDesc;
import org.apache.hcatalog.api.HCatCreateTableDesc;
import org.apache.hcatalog.cli.SemanticAnalysis.HCatSemanticAnalyzer;
import org.apache.hcatalog.common.HCatException;
import org.apache.hcatalog.data.schema.HCatFieldSchema;
import org.testng.annotations.Test;

import com.inmobi.qa.falcon.bundle.Bundle;
import com.inmobi.qa.falcon.helpers.ColoHelper;
import com.inmobi.qa.falcon.helpers.PrismHelper;
import com.inmobi.qa.falcon.response.ServiceResponse;
import com.inmobi.qa.falcon.util.Util;
import com.inmobi.qa.falcon.util.Util.URLS;

public class SubmitClusterHcat {

	PrismHelper prismHelper=new PrismHelper("prism.properties");
	ColoHelper ua4 = new ColoHelper("ua4.properties");

	private HCatClient client ; 

	@Test(enabled=true,timeOut=1800000)
	public void SubmitCluster_hcat() {
		String cluster = "";
		String feed01 = "";
		String feed02 = "";
		String process = "";

		Bundle b = new Bundle();
		try {
			b = (Bundle) Bundle.readBundle("src/test/resources/hcat_2")[0][0];
			b.generateUniqueBundle();
			b  = new Bundle(b,ua4.getEnvFileName());

			cluster =  b.getClusters().get(0);
			feed01 = b.getDataSets().get(0);
			feed02 = b.getDataSets().get(1);
			process = b.getProcessData();

			//client = getHcatClient();

/*			String dbName = "falconTest" ;
			String tableName = "falconTable" ;
			createDB(dbName);
			createSampleTable(dbName,tableName);
*/
			System.out.println("Cluster: "+cluster);
			ServiceResponse r = prismHelper.getClusterHelper().submitEntity(URLS.SUBMIT_URL, cluster);
			Util.assertSucceeded(r);

			System.out.println("Feed: "+feed01);
			r = prismHelper.getFeedHelper().submitEntity(URLS.SUBMIT_URL, feed01);
			Util.assertSucceeded(r);
			
			System.out.println("Feed: "+feed02);
			r = prismHelper.getFeedHelper().submitEntity(URLS.SUBMIT_URL, feed02);
			Util.assertSucceeded(r);
			
			System.out.println("process: "+process);
			r = prismHelper.getProcessHelper().submitEntity(URLS.SUBMIT_URL, process);
			Util.assertSucceeded(r);
			
			r = prismHelper.getProcessHelper().schedule(URLS.SCHEDULE_URL, process);
			Util.assertSucceeded(r);
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		finally {
			try {
				prismHelper.getProcessHelper().delete(URLS.DELETE_URL, process);
				prismHelper.getFeedHelper().delete(URLS.DELETE_URL, feed01);
				prismHelper.getFeedHelper().delete(URLS.DELETE_URL, feed02);
				prismHelper.getClusterHelper().delete(URLS.DELETE_URL, cluster);
			} catch (Exception e) {
				// TODO Auto-generated catch block
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
