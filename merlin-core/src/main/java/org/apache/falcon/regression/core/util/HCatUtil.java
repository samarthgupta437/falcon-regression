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


import org.apache.falcon.regression.core.enumsAndConstants.FEED_TYPE;
import org.apache.falcon.regression.core.enumsAndConstants.MerlinConstants;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.hcatalog.api.HCatAddPartitionDesc;
import org.apache.hive.hcatalog.api.HCatClient;
import org.apache.hive.hcatalog.api.HCatCreateTableDesc;
import org.apache.hive.hcatalog.cli.SemanticAnalysis.HCatSemanticAnalyzer;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class HCatUtil {

    public static HCatClient getHCatClient(String hCatEndPoint, String hiveMetaStorePrinciple)
    throws HCatException {
        HiveConf hcatConf = new HiveConf();
        hcatConf.set("hive.metastore.local", "false");
        hcatConf.setVar(HiveConf.ConfVars.METASTOREURIS, hCatEndPoint);
        hcatConf.setVar(HiveConf.ConfVars.METASTORE_KERBEROS_PRINCIPAL, hiveMetaStorePrinciple);
        hcatConf.setBoolVar(HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL, MerlinConstants.IS_SECURE);
        hcatConf.setIntVar(HiveConf.ConfVars.METASTORETHRIFTCONNECTIONRETRIES, 3);
        hcatConf.set(HiveConf.ConfVars.SEMANTIC_ANALYZER_HOOK.varname,
                HCatSemanticAnalyzer.class.getName());
        hcatConf.set(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "false");
        hcatConf.set(HiveConf.ConfVars.PREEXECHOOKS.varname, "");
        hcatConf.set(HiveConf.ConfVars.POSTEXECHOOKS.varname, "");
        return HCatClient.create(hcatConf);
    }

    public static void deleteTable(HCatClient cli, String dbName, String tabName)
            throws HCatException {
        cli.dropTable(dbName, tabName, true);
    }


    public static void createHCatTestData(HCatClient cli, FileSystem fs, FEED_TYPE dataType,
                                          String dbName, String tableName,
                                          ArrayList<String> dataFolder) throws HCatException {
        HCatUtil.addPartitionsToExternalTable(cli, dataType, dbName, tableName, dataFolder);
    }

    public static void addPartitionsToExternalTable(HCatClient client, FEED_TYPE dataType,
                                                    String dbName, String tableName,
                                                    ArrayList<String> dataFolder)
            throws HCatException {
        //Adding specific partitions that map to an external location
        Map<String, String> ptn = new HashMap<String, String>();
        for (String aDataFolder : dataFolder) {
            String[] parts = aDataFolder.split("/");
            int s = parts.length - 1;
            int subtractValue = 0;

            switch (dataType) {
                case MINUTELY:
                    ptn.put("minute", parts[s]);
                    ++subtractValue;
                case HOURLY:
                    ptn.put("hour", parts[s - subtractValue]);
                    ++subtractValue;
                case DAILY:
                    ptn.put("day", parts[s - subtractValue]);
                    ++subtractValue;
                case MONTHLY:
                    ptn.put("month", parts[s - subtractValue]);
                    ++subtractValue;
                case YEARLY:
                    ptn.put("year", parts[s - subtractValue]);
                default:
                    break;
            }
            //Each HCat partition maps to a directory, not to a file
            HCatAddPartitionDesc addPtn = HCatAddPartitionDesc.create(dbName,
                    tableName, aDataFolder, ptn).build();
            client.addPartition(addPtn);
            ptn.clear();
        }
    }
}