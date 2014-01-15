/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.falcon.regression.testHelper;

import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.helpers.PrismHelper;
import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.hadoop.fs.FileSystem;
import org.apache.oozie.client.OozieClient;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class BaseTestClass {
    private static List<String> serverNames;

    static {
        try {
            prepareProperties();
        } catch (Exception e) {
            System.out.println(e.getMessage());  //To change body of catch statement use
            // File | Settings |
            // File Templates.
        }
    }

    public PrismHelper prism;
    public List<ColoHelper> servers;
    public List<FileSystem> serverFS;
    public List<OozieClient> serverOC;
    public String baseHDFSDir = "/tmp/falcon-regression";
    public static final String PRISM_PROPERTIES = "prism.properties";
    public static final String MERLIN_PROPERTIES = "Merlin.properties";

    public BaseTestClass() {

        prism = new PrismHelper(PRISM_PROPERTIES);
        servers = getServers();
        serverFS = new ArrayList<FileSystem>();
        serverOC = new ArrayList<OozieClient>();
        for (ColoHelper server : servers) {
            try {
                serverFS.add(server.getClusterHelper().getHadoopFS());
            } catch (IOException e) {
                e.printStackTrace();
                System.exit(0);
            }
            serverOC.add(server.getClusterHelper().getOozieClient());
            try {
                HadoopUtil.createDir(baseHDFSDir, serverFS.get(serverFS.size() - 1));
            } catch (IOException e) {
                e.printStackTrace();
                System.exit(0);
            }
        }
    }

    private static void prepareProperties() throws Exception {
        //read Merlin.properties and create prism/servers and PrismServer.properties
        Properties merlinProp = Util.getPropertiesObj(MERLIN_PROPERTIES);
        serverNames = new ArrayList<String>(Arrays.asList(merlinProp.getProperty("servers").split
                (",")));
        for (int i = 0; i < serverNames.size(); i++)
            serverNames.set(i, serverNames.get(i).trim() + ".properties");

        serverNames.add(PRISM_PROPERTIES);
        for (String server : serverNames) {
            Properties serverPorp = new Properties();
            serverPorp.setProperty("oozie_url", merlinProp.getProperty(server + ".oozie_url"));
            serverPorp.setProperty("oozie_location",
                    merlinProp.getProperty(server + ".oozie_location"));
            serverPorp.setProperty("username", merlinProp.getProperty(server + ".username"));
            serverPorp.setProperty("qa_host", merlinProp.getProperty(server + ".qa_host"));
            serverPorp.setProperty("password", merlinProp.getProperty(server + ".password"));
            serverPorp.setProperty("hadoop_url", merlinProp.getProperty(server + ".hadoop_url"));
            serverPorp.setProperty("hadoop_location",
                    merlinProp.getProperty(server + ".hadoop_location"));
            serverPorp.setProperty("ivory_hostname",
                    merlinProp.getProperty(server + ".ivory_hostname"));
            serverPorp.setProperty("cluster_readonly",
                    merlinProp.getProperty(server + ".cluster_readonly"));
            serverPorp.setProperty("cluster_execute",
                    merlinProp.getProperty(server + ".cluster_execute"));
            serverPorp.setProperty("cluster_write",
                    merlinProp.getProperty(server + ".cluster_write"));
            serverPorp
                    .setProperty("activemq_url", merlinProp.getProperty(server + ".activemq_url"));
            serverPorp.setProperty("storeLocation",
                    merlinProp.getProperty(server + ".storeLocation"));
            serverPorp.setProperty("colo", merlinProp.getProperty(server + ".colo"));
            serverPorp.setProperty("oozie_url", merlinProp.getProperty(server + ".oozie_url"));

            serverPorp.store(new FileOutputStream(new File("merlin/src/main/resources/" + server)),
                    null);

        }

    }

    private List<ColoHelper> getServers() {
        ArrayList<ColoHelper> returnList = new ArrayList<ColoHelper>();
        for (int i = 0; i < serverNames.size() - 1; i++)
            returnList.add(new ColoHelper(serverNames.get(i)));

        return returnList;
    }

}
