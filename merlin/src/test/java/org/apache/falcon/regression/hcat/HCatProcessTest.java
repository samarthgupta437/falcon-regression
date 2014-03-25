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
import org.apache.falcon.regression.core.generated.cluster.Interfacetype;
import org.apache.falcon.regression.core.generated.dependencies.Frequency;
import org.apache.falcon.regression.core.generated.process.EngineType;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.enumsAndConstants.ENTITY_TYPE;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.api.HCatAddPartitionDesc;
import org.apache.hive.hcatalog.api.HCatClient;
import org.apache.hive.hcatalog.api.HCatCreateTableDesc;
import org.apache.hive.hcatalog.common.HCatException;
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

@Test(groups = "embedded")
public class HCatProcessTest extends BaseTestClass {
    private static Logger logger = Logger.getLogger(HCatProcessTest.class);
    ColoHelper cluster = servers.get(0);
    FileSystem clusterFS = serverFS.get(0);
    OozieClient clusterOC = serverOC.get(0);
    HCatClient clusterHC = cluster.getClusterHelper().getHCatClient();

    String hiveScriptDir = baseWorkflowDir + "/hive";
    String hiveScriptFile = hiveScriptDir + "/script.hql";
    String aggregateWorkflowDir = baseWorkflowDir + "/aggregator";
    String hiveScriptFileNonHCatInput = hiveScriptDir + "/script_non_hcat_input.hql";
    String hiveScriptFileNonHCatOutput = hiveScriptDir + "/script_non_hcat_output.hql";
    String hiveScriptTwoHCatInputOneHCatOutput = hiveScriptDir + "/script_two_hcat_input_one_hcat_output.hql";
    final String testDir = "/HCatProcessTest";
    final String baseTestHDFSDir = baseHDFSDir + testDir;
    final String inputHDFSDir = baseTestHDFSDir + "/input";
    final String inputHDFSDir2 = baseTestHDFSDir + "/input2";
    final String outputHDFSDir = baseTestHDFSDir + "/output";
    final String outputHDFSDir2 = baseTestHDFSDir + "/output2";

    final String dbName = "default";
    final String inputTableName = "hcatprocesstest_input_table";
    final String inputTableName2 = "hcatprocesstest_input_table2";
    final String outputTableName = "hcatprocesstest_output_table";
    final String outputTableName2 = "hcatprocesstest_output_table2";
    public static final String col1Name = "id";
    public static final String col2Name = "value";
    public static final String partitionColumn = "dt";

    private static final String hcatDir = OSUtil.getPath("src", "test", "resources", "hcat");
    private static final String localHCatData = OSUtil.getPath(hcatDir, "data");
    private static final String hiveScript = OSUtil.getPath(hcatDir, "hivescript");

    @BeforeClass
    public void uploadWorkflow() throws Exception {
        HadoopUtil.uploadDir(clusterFS, hiveScriptDir, hiveScript);
        HadoopUtil.uploadDir(clusterFS, aggregateWorkflowDir, OSUtil.RESOURCES_OOZIE);
    }

    @BeforeMethod
    public void setUp() throws Exception {
        bundles[0] = Util.readHCatBundle();
        bundles[0] = new Bundle(bundles[0], cluster.getEnvFileName(), cluster.getPrefix());
        bundles[0].generateUniqueBundle();
        bundles[0].setProcessWorkflow(hiveScriptFile, EngineType.HIVE);
        bundles[0].setClusterInterface(Interfacetype.REGISTRY, cluster.getClusterHelper().getHCatEndpoint());

        HadoopUtil.deleteDirIfExists(baseTestHDFSDir, clusterFS);
        HadoopUtil.createDir(outputHDFSDir, clusterFS);
        HadoopUtil.createDir(outputHDFSDir2, clusterFS);
        clusterHC.dropTable(dbName, inputTableName, true);
        clusterHC.dropTable(dbName, inputTableName2, true);
        clusterHC.dropTable(dbName, outputTableName, true);
        clusterHC.dropTable(dbName, outputTableName2, true);
    }

    @DataProvider
    public String[][] generateSeparators() {
        return new String[][] {{"-"}, {"/"}};
    }

    @Test(dataProvider = "generateSeparators")
    public void OneHCatInputOneHCatOutput(String separator) throws Exception {
        /* upload data and create partition */
        final String startDate = "2010-01-01T20:00Z";
        final String endDate = "2010-01-02T04:00Z";
        final String datePattern = StringUtils.join(new String[]{"yyyy", "MM", "dd", "HH"}, separator);
        List<String> dataDates = getDatesList(startDate, endDate, datePattern, 60);

        final ArrayList<String> dataset = createPeriodicDataset(dataDates, localHCatData, clusterFS, inputHDFSDir);

        ArrayList<HCatFieldSchema> cols = new ArrayList<HCatFieldSchema>();
        cols.add(new HCatFieldSchema(col1Name, HCatFieldSchema.Type.STRING, col1Name + " comment"));
        cols.add(new HCatFieldSchema(col2Name, HCatFieldSchema.Type.STRING, col2Name + " comment"));
        ArrayList<HCatFieldSchema> partitionCols = new ArrayList<HCatFieldSchema>();

        partitionCols.add(new HCatFieldSchema(partitionColumn, HCatFieldSchema.Type.STRING, partitionColumn + " partition"));
        clusterHC.createTable(HCatCreateTableDesc
                .create(dbName, inputTableName, cols)
                .partCols(partitionCols)
                .ifNotExists(true)
                .isTableExternal(true)
                .location(inputHDFSDir)
                .fieldsTerminatedBy('\t')
                .linesTerminatedBy('\n')
                .build());

        clusterHC.createTable(HCatCreateTableDesc
                .create(dbName, outputTableName, cols)
                .partCols(partitionCols)
                .ifNotExists(true)
                .isTableExternal(true)
                .location(outputHDFSDir)
                .fieldsTerminatedBy('\t')
                .linesTerminatedBy('\n')
                .build());

        addPartitionsToTable(dataDates, dataset, "dt", dbName, inputTableName);

        final String tableUriPartitionFragment = StringUtils.join(
                new String[] {"#dt=${YEAR}", "${MONTH}", "${DAY}", "${HOUR}"}, separator);
        String inputTableUri = "catalog:" + dbName + ":" + inputTableName + tableUriPartitionFragment;
        bundles[0].setInputFeedTableUri(inputTableUri);
        bundles[0].setInputFeedPeriodicity(1, Frequency.TimeUnit.hours);
        bundles[0].setInputFeedValidity(startDate, endDate);
        String outputTableUri = "catalog:" + dbName + ":" + outputTableName + tableUriPartitionFragment;
        bundles[0].setOutputFeedTableUri(outputTableUri);
        bundles[0].setOutputFeedPeriodicity(1, Frequency.TimeUnit.hours);
        bundles[0].setOutputFeedValidity(startDate, endDate);

        bundles[0].setProcessValidity(startDate, endDate);
        bundles[0].setProcessPeriodicity(1, Frequency.TimeUnit.hours);
        bundles[0].setProcessInputStartEnd("now(0,0)", "now(0,0)");
        bundles[0].submitFeedsScheduleProcess();

        InstanceUtil.waitTillInstanceReachState(
                clusterOC, bundles[0].getProcessName(), 1, CoordinatorAction.Status.SUCCEEDED, 5, ENTITY_TYPE.PROCESS);

        List<Path> inputData = HadoopUtil
                .getAllFilesRecursivelyHDFS(cluster, new Path(inputHDFSDir + "/" + dataDates.get(0)));
        List<Path> outputData = HadoopUtil
                .getAllFilesRecursivelyHDFS(cluster, new Path(outputHDFSDir + "/dt=" + dataDates.get(0)));
        AssertUtil.checkForPathsSizes(inputData, outputData);
    }

    @Test(dataProvider = "generateSeparators")
    public void TwoHCatInputOneHCatOutput(String separator) throws Exception {
        /* upload data and create partition */
        final String startDate = "2010-01-01T20:00Z";
        final String endDate = "2010-01-02T04:00Z";
        final String datePattern = StringUtils.join(new String[]{"yyyy", "MM", "dd", "HH"}, separator);
        List<String> dataDates = getDatesList(startDate, endDate, datePattern, 60);

        final ArrayList<String> dataset = createPeriodicDataset(dataDates, localHCatData, clusterFS, inputHDFSDir);
        final ArrayList<String> dataset2 = createPeriodicDataset(dataDates, localHCatData, clusterFS, inputHDFSDir2);

        ArrayList<HCatFieldSchema> cols = new ArrayList<HCatFieldSchema>();
        cols.add(new HCatFieldSchema(col1Name, HCatFieldSchema.Type.STRING, col1Name + " comment"));
        cols.add(new HCatFieldSchema(col2Name, HCatFieldSchema.Type.STRING, col2Name + " comment"));
        ArrayList<HCatFieldSchema> partitionCols = new ArrayList<HCatFieldSchema>();

        partitionCols.add(new HCatFieldSchema(partitionColumn, HCatFieldSchema.Type.STRING, partitionColumn + " partition"));
        clusterHC.createTable(HCatCreateTableDesc
                .create(dbName, inputTableName, cols)
                .partCols(partitionCols)
                .ifNotExists(true)
                .isTableExternal(true)
                .location(inputHDFSDir)
                .fieldsTerminatedBy('\t')
                .linesTerminatedBy('\n')
                .build());

        clusterHC.createTable(HCatCreateTableDesc
                .create(dbName, inputTableName2, cols)
                .partCols(partitionCols)
                .ifNotExists(true)
                .isTableExternal(true)
                .location(inputHDFSDir2)
                .fieldsTerminatedBy('\t')
                .linesTerminatedBy('\n')
                .build());

        clusterHC.createTable(HCatCreateTableDesc
                .create(dbName, outputTableName, cols)
                .partCols(partitionCols)
                .ifNotExists(true)
                .isTableExternal(true)
                .location(outputHDFSDir)
                .fieldsTerminatedBy('\t')
                .linesTerminatedBy('\n')
                .build());

        addPartitionsToTable(dataDates, dataset, "dt", dbName, inputTableName);
        addPartitionsToTable(dataDates, dataset, "dt", dbName, inputTableName2);

        final String tableUriPartitionFragment = StringUtils.join(
                new String[] {"#dt=${YEAR}", "${MONTH}", "${DAY}", "${HOUR}"}, separator);
        String inputTableUri = "catalog:" + dbName + ":" + inputTableName + tableUriPartitionFragment;
        String inputTableUri2 = "catalog:" + dbName + ":" + inputTableName2 + tableUriPartitionFragment;
        bundles[0].setInputFeedTableUri(inputTableUri);
        bundles[0].setInputFeedPeriodicity(1, Frequency.TimeUnit.hours);
        bundles[0].setInputFeedValidity(startDate, endDate);
        final String inputFeed1 = Util.getInputFeedFromBundle(bundles[0]);
        final String inputFeed2Name = "second-" + Util.getFeedName(inputFeed1);
        String inputFeed2 = Util.setFeedName(inputFeed1, inputFeed2Name);
        inputFeed2 = Util.setFeedTableUri(inputFeed2, inputTableUri2);
        bundles[0].addInputFeedToBundle("inputData2", inputFeed2, 0);

        String outputTableUri = "catalog:" + dbName + ":" + outputTableName + tableUriPartitionFragment;
        bundles[0].setOutputFeedTableUri(outputTableUri);
        bundles[0].setOutputFeedPeriodicity(1, Frequency.TimeUnit.hours);
        bundles[0].setOutputFeedValidity(startDate, endDate);

        bundles[0].setProcessValidity(startDate, endDate);
        bundles[0].setProcessPeriodicity(1, Frequency.TimeUnit.hours);
        bundles[0].setProcessInputStartEnd("now(0,0)", "now(0,0)");
        bundles[0].setProcessWorkflow(hiveScriptTwoHCatInputOneHCatOutput, EngineType.HIVE);
        bundles[0].submitFeedsScheduleProcess();

        InstanceUtil.waitTillInstanceReachState(
                clusterOC, bundles[0].getProcessName(), 1, CoordinatorAction.Status.SUCCEEDED, 5, ENTITY_TYPE.PROCESS);

        List<Path> inputData = HadoopUtil
                .getAllFilesRecursivelyHDFS(cluster, new Path(inputHDFSDir + "/" + dataDates.get(0)));
        List<Path> outputData = HadoopUtil
                .getAllFilesRecursivelyHDFS(cluster, new Path(outputHDFSDir + "/dt=" + dataDates.get(0)));
        final ContentSummary inputContentSummary =
                clusterFS.getContentSummary(new Path(inputHDFSDir + "/" + dataDates.get(0)));
        final ContentSummary inputContentSummary2 =
                clusterFS.getContentSummary(new Path(inputHDFSDir2 + "/" + dataDates.get(0)));
        final ContentSummary outputContentSummary =
                clusterFS.getContentSummary(new Path(outputHDFSDir + "/dt=" + dataDates.get(0)));
        logger.info("inputContentSummary = " + inputContentSummary.toString(false));
        logger.info("inputContentSummary2 = " + inputContentSummary2.toString(false));
        logger.info("outputContentSummary = " + outputContentSummary.toString(false));
        Assert.assertEquals(inputContentSummary.getLength() + inputContentSummary2.getLength(),
                outputContentSummary.getLength(),
                "Unexpected size of the output.");
    }

    @Test(dataProvider = "generateSeparators")
    public void OneHCatInputOneNonHCatOutput(String separator) throws Exception {
        final String startDate = "2010-01-01T20:00Z";
        final String endDate = "2010-01-02T04:00Z";
        /* upload data and create partition */
        final String datePattern = StringUtils.join(new String[]{"yyyy", "MM", "dd", "HH"}, separator);
        List<String> dataDates = getDatesList(startDate, endDate, datePattern, 60);

        final ArrayList<String> dataset = createPeriodicDataset(dataDates, localHCatData, clusterFS, inputHDFSDir);

        ArrayList<HCatFieldSchema> cols = new ArrayList<HCatFieldSchema>();
        cols.add(new HCatFieldSchema(col1Name, HCatFieldSchema.Type.STRING, col1Name + " comment"));
        cols.add(new HCatFieldSchema(col2Name, HCatFieldSchema.Type.STRING, col2Name + " comment"));
        ArrayList<HCatFieldSchema> partitionCols = new ArrayList<HCatFieldSchema>();

        partitionCols.add(new HCatFieldSchema(partitionColumn, HCatFieldSchema.Type.STRING, partitionColumn + " partition"));
        clusterHC.createTable(HCatCreateTableDesc
                .create(dbName, inputTableName, cols)
                .partCols(partitionCols)
                .ifNotExists(true)
                .isTableExternal(true)
                .location(inputHDFSDir)
                .fieldsTerminatedBy('\t')
                .linesTerminatedBy('\n')
                .build());

        addPartitionsToTable(dataDates, dataset, "dt", dbName, inputTableName);

        final String tableUriPartitionFragment = StringUtils.join(
                new String[] {"#dt=${YEAR}", "${MONTH}", "${DAY}", "${HOUR}"}, separator);
        String inputTableUri = "catalog:" + dbName + ":" + inputTableName + tableUriPartitionFragment;
        bundles[0].setInputFeedTableUri(inputTableUri);
        bundles[0].setInputFeedPeriodicity(1, Frequency.TimeUnit.hours);
        bundles[0].setInputFeedValidity(startDate, endDate);

        //
        String nonHCatFeed = Util.getOutputFeedFromBundle(Util.readELBundles()[0][0]);
        final String outputFeedName = Util.getOutputFeedNameFromBundle(bundles[0]);
        nonHCatFeed = Util.setFeedName(nonHCatFeed, outputFeedName);
        final List<String> clusterNames = bundles[0].getClusterNames();
        Assert.assertEquals(clusterNames.size(), 1, "Expected only one cluster in the bundle.");
        nonHCatFeed = Util.setClusterNameInFeed(nonHCatFeed, clusterNames.get(0), 0);
        InstanceUtil.writeFeedElement(bundles[0], nonHCatFeed, outputFeedName);
        bundles[0].setOutputFeedLocationData(outputHDFSDir + "/" +
                StringUtils.join(new String[]{"${YEAR}", "${MONTH}", "${DAY}", "${HOUR}"}, separator));
        bundles[0].setOutputFeedPeriodicity(1, Frequency.TimeUnit.hours);
        bundles[0].setOutputFeedValidity(startDate, endDate);

        bundles[0].setProcessValidity(startDate, endDate);
        bundles[0].setProcessPeriodicity(1, Frequency.TimeUnit.hours);
        bundles[0].setProcessInputStartEnd("now(0,0)", "now(0,0)");

        bundles[0].setProcessWorkflow(hiveScriptFileNonHCatOutput, EngineType.HIVE);
        bundles[0].submitFeedsScheduleProcess();

        InstanceUtil.waitTillInstanceReachState(
                clusterOC, bundles[0].getProcessName(), 1, CoordinatorAction.Status.SUCCEEDED, 5, ENTITY_TYPE.PROCESS);

        List<Path> inputData = HadoopUtil
                .getAllFilesRecursivelyHDFS(cluster, new Path(inputHDFSDir + "/" + dataDates.get(0)));
        List<Path> outputData = HadoopUtil
                .getAllFilesRecursivelyHDFS(cluster, new Path(outputHDFSDir + "/" + dataDates.get(0)));
        AssertUtil.checkForPathsSizes(inputData, outputData);
    }

    @Test(dataProvider = "generateSeparators")
    public void OneNonCatInputOneHCatOutput(String separator) throws Exception {
        /* upload data and create partition */
        final String startDate = "2010-01-01T20:00Z";
        final String endDate = "2010-01-02T04:00Z";
        final String datePattern = StringUtils.join(new String[] {"yyyy", "MM", "dd", "HH"}, separator);
        List<String> dataDates = getDatesList(startDate, endDate, datePattern, 60);

        final ArrayList<String> dataset = createPeriodicDataset(dataDates, localHCatData, clusterFS, inputHDFSDir);

        ArrayList<HCatFieldSchema> cols = new ArrayList<HCatFieldSchema>();
        cols.add(new HCatFieldSchema(col1Name, HCatFieldSchema.Type.STRING, col1Name + " comment"));
        cols.add(new HCatFieldSchema(col2Name, HCatFieldSchema.Type.STRING, col2Name + " comment"));
        ArrayList<HCatFieldSchema> partitionCols = new ArrayList<HCatFieldSchema>();

        partitionCols.add(new HCatFieldSchema(partitionColumn, HCatFieldSchema.Type.STRING, partitionColumn + " partition"));
        clusterHC.createTable(HCatCreateTableDesc
                .create(dbName, outputTableName, cols)
                .partCols(partitionCols)
                .ifNotExists(true)
                .isTableExternal(true)
                .location(outputHDFSDir)
                .fieldsTerminatedBy('\t')
                .linesTerminatedBy('\n')
                .build());

        String nonHCatFeed = Util.getInputFeedFromBundle(Util.readELBundles()[0][0]);
        final String inputFeedName = Util.getInputFeedNameFromBundle(bundles[0]);
        nonHCatFeed = Util.setFeedName(nonHCatFeed, inputFeedName);
        final List<String> clusterNames = bundles[0].getClusterNames();
        Assert.assertEquals(clusterNames.size(), 1, "Expected only one cluster in the bundle.");
        nonHCatFeed = Util.setClusterNameInFeed(nonHCatFeed, clusterNames.get(0), 0);
        InstanceUtil.writeFeedElement(bundles[0], nonHCatFeed, inputFeedName);
        bundles[0].setInputFeedDataPath(inputHDFSDir + "/" +
                StringUtils.join(new String[] {"${YEAR}", "${MONTH}", "${DAY}", "${HOUR}"}, separator));
        bundles[0].setInputFeedPeriodicity(1, Frequency.TimeUnit.hours);
        bundles[0].setInputFeedValidity(startDate, endDate);

        final String tableUriPartitionFragment = StringUtils.join(
                new String[] {"#dt=${YEAR}", "${MONTH}", "${DAY}", "${HOUR}"}, separator);
        String outputTableUri = "catalog:" + dbName + ":" + outputTableName + tableUriPartitionFragment;
        bundles[0].setOutputFeedTableUri(outputTableUri);
        bundles[0].setOutputFeedPeriodicity(1, Frequency.TimeUnit.hours);
        bundles[0].setOutputFeedValidity(startDate, endDate);

        bundles[0].setProcessWorkflow(hiveScriptFileNonHCatInput, EngineType.HIVE);
        bundles[0].setProcessValidity(startDate, endDate);
        bundles[0].setProcessPeriodicity(1, Frequency.TimeUnit.hours);
        bundles[0].setProcessInputStartEnd("now(0,0)", "now(0,0)");
        bundles[0].submitFeedsScheduleProcess();

        InstanceUtil.waitTillInstanceReachState(
                clusterOC, bundles[0].getProcessName(), 1, CoordinatorAction.Status.SUCCEEDED, 5, ENTITY_TYPE.PROCESS);

        List<Path> inputData = HadoopUtil
                .getAllFilesRecursivelyHDFS(cluster, new Path(inputHDFSDir + "/" + dataDates.get(0)));
        List<Path> outputData = HadoopUtil
                .getAllFilesRecursivelyHDFS(cluster, new Path(outputHDFSDir + "/dt=" + dataDates.get(0)));
        AssertUtil.checkForPathsSizes(inputData, outputData);
    }

    private void addPartitionsToTable(List<String> partitions, List<String> partitionLocations, String partitionCol,
                                      String dbName, String tableName) throws HCatException {
        Assert.assertEquals(partitions.size(), partitionLocations.size(),
                "Number of locations is not same as number of partitions.");
        final List<HCatAddPartitionDesc> partitionDesc = new ArrayList<HCatAddPartitionDesc>();
        for (int i = 0; i < partitions.size(); ++i) {
            final String partition = partitions.get(i);
            final Map<String, String> onePartition = new HashMap<String, String>();
            onePartition.put(partitionCol, partition);
            final String partitionLoc = partitionLocations.get(i);
            partitionDesc.add(HCatAddPartitionDesc.create(dbName, tableName, partitionLoc, onePartition).build());
        }
        clusterHC.addPartitions(partitionDesc);
    }

    private ArrayList<String> createPeriodicDataset(List<String> dataDates, String localData, FileSystem fileSystem,
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
        logger.info("generating data between " + formatter.print(startDateJoda) + " and " + formatter.print(endDateJoda));
        List<String> dates = new ArrayList<String>();
        dates.add(formatter.print(startDateJoda));
        while (!startDateJoda.isAfter(endDateJoda)) {
            startDateJoda = startDateJoda.plusMinutes(skipMinutes);
            dates.add(formatter.print(startDateJoda));
        }
        return dates;
    }

    @AfterMethod
    public void tearDown() throws Exception {
        removeBundles();
    }
}
