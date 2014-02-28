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

package org.apache.falcon.regression.core.util;


import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.hcatalog.api.*;
import org.apache.hive.hcatalog.cli.SemanticAnalysis.HCatSemanticAnalyzer;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HCatUtil {

    public static HCatClient client;

    public static HCatClient getHCatClient() {
        try {
            HiveConf hcatConf = new HiveConf();
            hcatConf.set("hive.metastore.local", "false");
            hcatConf.setVar(HiveConf.ConfVars.METASTOREURIS, "thrift://localhost:14000");
            hcatConf.setIntVar(HiveConf.ConfVars.METASTORETHRIFTCONNECTIONRETRIES, 3);
            hcatConf.set(HiveConf.ConfVars.SEMANTIC_ANALYZER_HOOK.varname,
                    HCatSemanticAnalyzer.class.getName());
            hcatConf.set(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "false");

            hcatConf.set(HiveConf.ConfVars.PREEXECHOOKS.varname, "");
            hcatConf.set(HiveConf.ConfVars.POSTEXECHOOKS.varname, "");
            return HCatClient.create(hcatConf);

        } catch (HCatException e) {
            e.printStackTrace();
        }
        return client;
    }

    private static void createDB(HCatClient cli, String dbName) {
        HCatCreateDBDesc dbDesc;
        try {
            dbDesc = HCatCreateDBDesc.create(dbName)
                    .ifNotExists(true).build();
            cli.createDatabase(dbDesc);
        } catch (HCatException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    public static void createEmptyTable(HCatClient cli, String dbName, String tabName){
        try
        {
            ArrayList<HCatFieldSchema> cols = new ArrayList<HCatFieldSchema>();
            cols.add(new HCatFieldSchema("id", HCatFieldSchema.Type.STRING,"id comment"));
            HCatCreateTableDesc tableDesc = HCatCreateTableDesc
                    .create(dbName, tabName, cols)
                    .fileFormat("rcfile")
                    .ifNotExists(true)
                    .isTableExternal(true)
                    .build();
            cli.createTable(tableDesc);

        }catch(Exception e){
            e.printStackTrace();
        }
    }

    public static void deleteTable(HCatClient cli, String dbName, String tabName){
        try{
            cli.dropTable(dbName, tabName, true);
        }catch (Exception e){
            e.printStackTrace();
        }
    }



    public static void createPartitionedTable(String dataType, String dbName, String tableName, HCatClient client, String tableLoc){
        try{

            ArrayList<HCatFieldSchema> cols = new ArrayList<HCatFieldSchema>();
            ArrayList<HCatFieldSchema> ptnCols = new ArrayList<HCatFieldSchema>();

            //client.dropDatabase("sample_db", true, HCatClient.DropDBMode.CASCADE);

            cols.add(new HCatFieldSchema("id", HCatFieldSchema.Type.STRING,"id comment"));
            cols.add(new HCatFieldSchema("value", HCatFieldSchema.Type.STRING,"value comment"));

            //Stating partition names
            if (dataType.equalsIgnoreCase("minutely")){
                ptnCols.add(new HCatFieldSchema("year", HCatFieldSchema.Type.STRING, "year prt"));
                ptnCols.add(new HCatFieldSchema("month", HCatFieldSchema.Type.STRING, "month prt"));
                ptnCols.add(new HCatFieldSchema("day", HCatFieldSchema.Type.STRING, "day prt"));
                ptnCols.add(new HCatFieldSchema("hour", HCatFieldSchema.Type.STRING, "hour prt"));
                ptnCols.add(new HCatFieldSchema("minute", HCatFieldSchema.Type.STRING, "min prt"));
            }
            else if (dataType.equalsIgnoreCase("hourly")){
                ptnCols.add(new HCatFieldSchema("year", HCatFieldSchema.Type.STRING, "year prt"));
                ptnCols.add(new HCatFieldSchema("month", HCatFieldSchema.Type.STRING, "month prt"));
                ptnCols.add(new HCatFieldSchema("day", HCatFieldSchema.Type.STRING, "day prt"));
                ptnCols.add(new HCatFieldSchema("hour", HCatFieldSchema.Type.STRING, "hour prt"));
            }
            else if (dataType.equalsIgnoreCase("daily")){
                ptnCols.add(new HCatFieldSchema("year", HCatFieldSchema.Type.STRING, "year prt"));
                ptnCols.add(new HCatFieldSchema("month", HCatFieldSchema.Type.STRING, "month prt"));
                ptnCols.add(new HCatFieldSchema("day", HCatFieldSchema.Type.STRING, "day prt"));
            }
            else if (dataType.equalsIgnoreCase("monthly")){
                ptnCols.add(new HCatFieldSchema("year", HCatFieldSchema.Type.STRING, "year prt"));
                ptnCols.add(new HCatFieldSchema("month", HCatFieldSchema.Type.STRING, "month prt"));
            }
            else if (dataType.equalsIgnoreCase("yearly")){
                ptnCols.add(new HCatFieldSchema("year", HCatFieldSchema.Type.STRING, "year prt"));
            }

            HCatCreateTableDesc tableDesc = HCatCreateTableDesc
                    .create(dbName, tableName, cols)
                    .fileFormat("rcfile")
                    .ifNotExists(true)
                    .partCols(ptnCols)
                    .isTableExternal(true)
                    .location(tableLoc)
                    .build();
            client.createTable(tableDesc);

        }catch(HCatException e){
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public static void createHCatTestData(HCatClient cli, FileSystem fs, String dataType, String dbName, String tableName, ArrayList<String> dataFolder, String prefix) throws Exception {

        HCatUtil.addPartitionsToExternalTable(cli, dataType, dbName, tableName, dataFolder);
    }

    public static void addPartitionsToExternalTable( HCatClient client, String dataType, String dbName, String tableName, ArrayList<String> dataFolder){
        //Adding specific partitions that map to an external location

        Map<String, String> ptn = new HashMap<String, String>();
        for(int i=0; i<dataFolder.size(); ++i){
            String[] parts = dataFolder.get(i).split("/");
            int s = parts.length-1;
            //The last element in the array will be the minute directory

            if (dataType.equalsIgnoreCase("minutely")){
                ptn.put("year", parts[s-4]);
                ptn.put("month", parts[s-3]);
                ptn.put("day", parts[s-2]);
                ptn.put("hour", parts[s-1]);
                ptn.put("minute", parts[s]);
            }
            else if (dataType.equalsIgnoreCase("hourly")){
                ptn.put("year", parts[s-3]);
                ptn.put("month", parts[s-2]);
                ptn.put("day", parts[s-1]);
                ptn.put("hour", parts[s]);
            }
            else if (dataType.equalsIgnoreCase("daily")){
                ptn.put("year", parts[s-2]);
                ptn.put("month", parts[s-1]);
                ptn.put("day", parts[s]);
            }
            else if (dataType.equalsIgnoreCase("monthly")){
                ptn.put("year", parts[s-1]);
                ptn.put("month", parts[s]);
            }
            else if (dataType.equalsIgnoreCase("yearly")){
                ptn.put("year", parts[s]);
            }
            try{
                //Each HCat partition maps to a directory, not to a file
                HCatAddPartitionDesc addPtn = HCatAddPartitionDesc.create(dbName,
                        tableName, dataFolder.get(i), ptn).build();
                client.addPartition(addPtn);
                ptn.clear();
            }
            catch(HCatException e){
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

        }
    }
}