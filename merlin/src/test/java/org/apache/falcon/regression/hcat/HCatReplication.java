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

import org.apache.commons.lang.StringUtils;
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.enumsAndConstants.ENTITY_TYPE;
import org.apache.falcon.regression.core.generated.cluster.Interfacetype;
import org.apache.falcon.regression.core.generated.dependencies.Frequency;
import org.apache.falcon.regression.core.generated.feed.ActionType;
import org.apache.falcon.regression.core.generated.feed.ClusterType;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.core.util.XmlUtil;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hive.hcatalog.api.HCatAddPartitionDesc;
import org.apache.hive.hcatalog.api.HCatClient;
import org.apache.hive.hcatalog.api.HCatCreateTableDesc;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.log4j.Logger;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.OozieClient;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HCatReplication extends BaseTestClass {

    private static Logger logger = Logger.getLogger(HCatReplication.class);
    ColoHelper cluster = servers.get(0);
    FileSystem clusterFS = serverFS.get(0);
    HCatClient clusterHC;

    ColoHelper cluster2 = servers.get(1);
    FileSystem cluster2FS = serverFS.get(1);
    OozieClient cluster2OC = serverOC.get(1);
    HCatClient cluster2HC;

    ColoHelper cluster3 = servers.get(2);
    FileSystem cluster3FS = serverFS.get(2);
    OozieClient cluster3OC = serverOC.get(2);
    HCatClient cluster3HC;

    final String baseTestHDFSDir = baseHDFSDir + "/HCatReplication";

    final String dbName = "default";
    private static final String localHCatData = OSUtil.getPath(OSUtil.RESOURCES, "hcat", "data");
    int defaultTimeout = OSUtil.IS_WINDOWS ? 10 : 5;

    @BeforeClass
    public void beforeClass() throws IOException {
        clusterHC = cluster.getClusterHelper().getHCatClient();
        cluster2HC = cluster2.getClusterHelper().getHCatClient();
        cluster3HC = cluster3.getClusterHelper().getHCatClient();
        // create the base dir on all clusters.
        // method will delete the dir if it exists.
        HadoopUtil.createDir(baseTestHDFSDir, clusterFS, cluster2FS, cluster3FS);
    }

    @BeforeMethod
    public void setUp() throws Exception {
        Bundle bundle = Util.readHCatBundle();
        bundles[0] = new Bundle(bundle, cluster.getEnvFileName(), cluster.getPrefix());
        bundles[0].generateUniqueBundle();
        bundles[0].setClusterInterface(Interfacetype.REGISTRY,
                cluster.getClusterHelper().getHCatEndpoint());

        bundles[1] = new Bundle(bundle, cluster2.getEnvFileName(), cluster2.getPrefix());
        bundles[1].generateUniqueBundle();
        bundles[1].setClusterInterface(Interfacetype.REGISTRY, cluster2.getClusterHelper()
                .getHCatEndpoint());

        bundles[2] = new Bundle(bundle, cluster3.getEnvFileName(), cluster3.getPrefix());
        bundles[2].generateUniqueBundle();
        bundles[2].setClusterInterface(Interfacetype.REGISTRY, cluster3.getClusterHelper()
                .getHCatEndpoint());

    }

    @DataProvider
    public String[][] generateSeparators() {
        //disabling till FALCON-372 is fixed
        //return new String[][] {{"-"}, {"/"}};
        return new String[][]{{"-"},};
    }

    // make sure oozie changes mentioned FALCON-389 are done on the clusters. Otherwise the test
    // will fail.
    // Noticed with hive 0.13 we need the following issues resolved to work HIVE-6848 and
    // HIVE-6868. Also oozie share libs need to have hive jars that have these jira's resolved and
    // the maven depenendcy you are using to run the tests has to have hcat that has these fixed.
    @Test(dataProvider = "generateSeparators")
    public void oneSourceOneTarget(String separator) throws Exception {
        String tcName = "HCatReplication_oneSourceOneTarget";
        if (separator.equals("-")) {
            tcName += "_hyphen";
        } else {
            tcName += "_slash";
        }
        String tblName = tcName;
        String testHdfsDir = baseTestHDFSDir + "/" + tcName;
        HadoopUtil.createDir(testHdfsDir, clusterFS, cluster2FS);
        final String tableUriPartitionFragment = StringUtils
                .join(new String[]{"#dt=${YEAR}", "${MONTH}", "${DAY}", "${HOUR}"}, separator);
        String tableUri = "catalog:" + dbName + ":" + tblName + tableUriPartitionFragment;
        final String startDate = "2010-01-01T20:00Z";
        final String endDate = "2010-01-02T04:00Z";
        final String datePattern =
                StringUtils.join(new String[]{"yyyy", "MM", "dd", "HH"}, separator);
        List<String> dataDates = getDatesList(startDate, endDate, datePattern, 60);

        final ArrayList<String> dataset =
                createPeriodicDataset(dataDates, localHCatData, clusterFS, testHdfsDir);
        final String col1Name = "id";
        final String col2Name = "value";
        final String partitionColumn = "dt";

        ArrayList<HCatFieldSchema> cols = new ArrayList<HCatFieldSchema>();
        cols.add(new HCatFieldSchema(col1Name, HCatFieldSchema.Type.STRING, col1Name + " comment"));
        cols.add(new HCatFieldSchema(col2Name, HCatFieldSchema.Type.STRING, col2Name + " comment"));
        ArrayList<HCatFieldSchema> partitionCols = new ArrayList<HCatFieldSchema>();

        // create table on cluster 1 and add data to it.
        partitionCols.add(new HCatFieldSchema(partitionColumn, HCatFieldSchema.Type.STRING,
                partitionColumn + " partition"));
        createTable(clusterHC, dbName, tblName, cols, partitionCols, testHdfsDir);
        addPartitionsToTable(dataDates, dataset, "dt", dbName, tblName, clusterHC);

        // create table on target cluster.
        createTable(cluster2HC, dbName, tblName, cols, partitionCols, testHdfsDir);

        Bundle.submitCluster(bundles[0], bundles[1]);

        bundles[0].setInputFeedPeriodicity(1, Frequency.TimeUnit.hours);
        bundles[0].setInputFeedValidity(startDate, "2099-01-01T00:00Z");
        bundles[0].setInputFeedTableUri(tableUri);

        String feed = bundles[0].getDataSets().get(0);
        // set the cluster 2 as the target.
        feed = InstanceUtil.setFeedClusterWithTable(feed,
                XmlUtil.createValidity(startDate, endDate),
                XmlUtil.createRtention("months(9000)", ActionType.DELETE),
                Util.readClusterName(bundles[1].getClusters().get(0)), ClusterType.TARGET, null,
                tableUri, null);

        AssertUtil.assertSucceeded(
                prism.getFeedHelper().submitAndSchedule(Util.URLS.SUBMIT_AND_SCHEDULE_URL,
                        feed)
        );
        Thread.sleep(15000);
        //check if all coordinators exist
        Assert.assertEquals(InstanceUtil
                .checkIfFeedCoordExist(cluster2.getFeedHelper(), Util.readDatasetName(feed),
                        "REPLICATION"), 1);

        //replication should start, wait while it ends
        InstanceUtil.waitTillInstanceReachState(cluster2OC, Util.readEntityName(feed), 1,
                CoordinatorAction.Status.SUCCEEDED, defaultTimeout, ENTITY_TYPE.FEED);


        //TODO: Add mode checks. Currently job fails because of HIVE-6848
    }

    // make sure oozie changes mentioned FALCON-389 are done on the clusters. Otherwise the test
    // will fail.
    // Noticed with hive 0.13 we need the following issues resolved to work HIVE-6848 and
    // HIVE-6868. Also oozie share libs need to have hive jars that have these jira's resolved
    // and the maven depenendcy you are using to run the tests has to have hcat that has these
    // fixed.
    @Test(dataProvider = "generateSeparators")
    public void oneSourceTwoTarget(String separator) throws Exception {
        String tcName = "HCatReplication_oneSourceTwoTarget";
        if (separator.equals("-")) {
            tcName += "_hyphen";
        } else {
            tcName += "_slash";
        }
        String tblName = tcName;
        String testHdfsDir = baseTestHDFSDir + "/" + tcName;
        HadoopUtil.createDir(testHdfsDir, clusterFS, cluster2FS, cluster3FS);
        final String tableUriPartitionFragment = StringUtils
                .join(new String[]{"#dt=${YEAR}", "${MONTH}", "${DAY}", "${HOUR}"}, separator);
        String tableUri = "catalog:" + dbName + ":" + tblName + tableUriPartitionFragment;
        final String startDate = "2010-01-01T20:00Z";
        final String endDate = "2010-01-02T04:00Z";
        final String datePattern =
                StringUtils.join(new String[]{"yyyy", "MM", "dd", "HH"}, separator);
        List<String> dataDates = getDatesList(startDate, endDate, datePattern, 60);

        final ArrayList<String> dataset =
                createPeriodicDataset(dataDates, localHCatData, clusterFS, testHdfsDir);
        final String col1Name = "id";
        final String col2Name = "value";
        final String partitionColumn = "dt";

        ArrayList<HCatFieldSchema> cols = new ArrayList<HCatFieldSchema>();
        cols.add(new HCatFieldSchema(col1Name, HCatFieldSchema.Type.STRING, col1Name + " comment"));
        cols.add(new HCatFieldSchema(col2Name, HCatFieldSchema.Type.STRING, col2Name + " comment"));
        ArrayList<HCatFieldSchema> partitionCols = new ArrayList<HCatFieldSchema>();

        // create table on cluster 1 and add data to it.
        partitionCols.add(new HCatFieldSchema(partitionColumn, HCatFieldSchema.Type.STRING,
                partitionColumn + " partition"));
        createTable(clusterHC, dbName, tblName, cols, partitionCols, testHdfsDir);
        addPartitionsToTable(dataDates, dataset, "dt", dbName, tblName, clusterHC);

        // create table on target cluster.
        createTable(cluster2HC, dbName, tblName, cols, partitionCols, testHdfsDir);
        createTable(cluster3HC, dbName, tblName, cols, partitionCols, testHdfsDir);

        Bundle.submitCluster(bundles[0], bundles[1], bundles[2]);

        bundles[0].setInputFeedPeriodicity(1, Frequency.TimeUnit.hours);
        bundles[0].setInputFeedValidity(startDate, "2099-01-01T00:00Z");
        bundles[0].setInputFeedTableUri(tableUri);

        String feed = bundles[0].getDataSets().get(0);
        // set the cluster 2 as the target.
        feed = InstanceUtil.setFeedClusterWithTable(feed,
                XmlUtil.createValidity(startDate, endDate),
                XmlUtil.createRtention("months(9000)", ActionType.DELETE),
                Util.readClusterName(bundles[1].getClusters().get(0)), ClusterType.TARGET, null,
                tableUri, null);

        // set the cluster 2 as the target.
        feed = InstanceUtil.setFeedClusterWithTable(feed,
                XmlUtil.createValidity(startDate, endDate),
                XmlUtil.createRtention("months(9000)", ActionType.DELETE),
                Util.readClusterName(bundles[2].getClusters().get(0)), ClusterType.TARGET, null,
                tableUri, null);

        AssertUtil.assertSucceeded(
                prism.getFeedHelper().submitAndSchedule(Util.URLS.SUBMIT_AND_SCHEDULE_URL,
                        feed)
        );
        Thread.sleep(15000);
        //check if all coordinators exist
        Assert.assertEquals(InstanceUtil
                .checkIfFeedCoordExist(cluster2.getFeedHelper(), Util.readDatasetName(feed),
                        "REPLICATION"), 1);

        //check if all coordinators exist
        Assert.assertEquals(InstanceUtil
                .checkIfFeedCoordExist(cluster3.getFeedHelper(), Util.readDatasetName(feed),
                        "REPLICATION"), 1);

        //replication should start, wait while it ends
        InstanceUtil.waitTillInstanceReachState(cluster2OC, Util.readEntityName(feed), 1,
                CoordinatorAction.Status.SUCCEEDED, defaultTimeout, ENTITY_TYPE.FEED);


        //replication should start, wait while it ends
        InstanceUtil.waitTillInstanceReachState(cluster3OC, Util.readEntityName(feed), 1,
                CoordinatorAction.Status.SUCCEEDED, defaultTimeout, ENTITY_TYPE.FEED);


        //TODO: Add mode checks. Currently job fails because of HIVE-6848
    }

    private void addPartitionsToTable(List<String> partitions, List<String> partitionLocations,
                                      String partitionCol,
                                      String dbName, String tableName, HCatClient hc) throws
    HCatException {
        Assert.assertEquals(partitions.size(), partitionLocations.size(),
                "Number of locations is not same as number of partitions.");
        final List<HCatAddPartitionDesc> partitionDesc = new ArrayList<HCatAddPartitionDesc>();
        for (int i = 0; i < partitions.size(); ++i) {
            final String partition = partitions.get(i);
            final Map<String, String> onePartition = new HashMap<String, String>();
            onePartition.put(partitionCol, partition);
            final String partitionLoc = partitionLocations.get(i);
            partitionDesc
                    .add(HCatAddPartitionDesc.create(dbName, tableName, partitionLoc, onePartition)
                            .build());
        }
        hc.addPartitions(partitionDesc);
    }

    private ArrayList<String> createPeriodicDataset(List<String> dataDates, String localData,
                                                    FileSystem fileSystem,
                                                    String baseHDFSLocation) throws IOException {
        ArrayList<String> dataFolder = new ArrayList<String>();

        for (String dataDate : dataDates)
            dataFolder.add(baseHDFSLocation + "/" + dataDate);

        HadoopUtil.flattenAndPutDataInFolder(fileSystem, localData, dataFolder);
        return dataFolder;
    }

    public static List<String> getDatesList(String startDate, String endDate, String datePattern,
                                            int skipMinutes) throws ParseException {
        DateTime startDateJoda = new DateTime(InstanceUtil.oozieDateToDate(startDate));
        DateTime endDateJoda = new DateTime(InstanceUtil.oozieDateToDate(endDate));
        DateTimeFormatter formatter = DateTimeFormat.forPattern(datePattern);
        logger.info("generating data between " + formatter.print(startDateJoda) + " and " +
                formatter.print(endDateJoda));
        List<String> dates = new ArrayList<String>();
        dates.add(formatter.print(startDateJoda));
        while (!startDateJoda.isAfter(endDateJoda)) {
            startDateJoda = startDateJoda.plusMinutes(skipMinutes);
            dates.add(formatter.print(startDateJoda));
        }
        return dates;
    }

    private static void createTable(HCatClient hcatClient, String dbName, String tblName,
                                    List<HCatFieldSchema> cols, List<HCatFieldSchema> partitionCols,
                                    String hdfsDir) throws HCatException {
        hcatClient.dropTable(dbName, tblName, true);
        hcatClient.createTable(HCatCreateTableDesc
                .create(dbName, tblName, cols)
                .partCols(partitionCols)
                .ifNotExists(true)
                .isTableExternal(true)
                .location(hdfsDir)
                .build());
    }

    /*
    @8AfterMethod(alwaysRun = true)
    public void tearDown() throws Exception {
        removeBundles();
    }
    */
}
