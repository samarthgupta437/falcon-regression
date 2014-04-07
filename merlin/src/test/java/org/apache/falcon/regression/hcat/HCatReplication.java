package org.apache.falcon.regression.hcat;

import org.apache.commons.lang.StringUtils;
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.enumsAndConstants.ENTITY_TYPE;
import org.apache.falcon.regression.core.generated.cluster.Interfacetype;
import org.apache.falcon.regression.core.generated.dependencies.Frequency;
import org.apache.falcon.regression.core.generated.feed.ActionType;
import org.apache.falcon.regression.core.generated.feed.ClusterType;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.response.ServiceResponse;
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
    OozieClient clusterOC = serverOC.get(0);
    HCatClient clusterHC = cluster.getClusterHelper().getHCatClient();

    ColoHelper cluster2 = servers.get(1);
    FileSystem cluster2FS = serverFS.get(1);
    OozieClient cluster2OC = serverOC.get(1);
    HCatClient cluster2HC = cluster2.getClusterHelper().getHCatClient();

    ColoHelper cluster3 = servers.get(2);
    FileSystem cluster3FS = serverFS.get(2);
    OozieClient cluster3OC = serverOC.get(2);
    HCatClient cluster3HC = cluster3.getClusterHelper().getHCatClient();

    final String baseTestHDFSDir = baseHDFSDir + "/HCatReplication";

    final String dbName = "default";
    private static final String localHCatData = OSUtil.getPath(OSUtil.RESOURCES, "hcat", "data");

    @BeforeClass
    public void beforeClass() throws IOException {
        // create the base dir on all clusters.
        // method will delete the dir if it exists.
        HadoopUtil.createDir(baseTestHDFSDir, clusterFS, cluster2FS, cluster3FS);
    }

    @BeforeMethod
    public void setUp() throws Exception {
        Bundle bundle = Util.readHCatBundle();
        bundles[0] = new Bundle(bundle, cluster.getEnvFileName(), cluster.getPrefix());
        bundles[0].generateUniqueBundle();
        bundles[0].setClusterInterface(Interfacetype.REGISTRY, cluster.getClusterHelper().getHCatEndpoint());

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
    /*public String[][] generateSeparators() {
        return new String[][] {{"-"}, {"/"}};
    } */
    public String[][] generateSeparators() {
        return new String[][] {{"-"}};
    }

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
        final String tableUriPartitionFragment = StringUtils.join(new String[] {"#dt=${YEAR}", "${MONTH}", "${DAY}", "${HOUR}"}, separator);
        String tableUri = "catalog:" + dbName + ":" + tblName + tableUriPartitionFragment;

        Bundle.submitCluster(bundles[0], bundles[1]);
        String startTime = InstanceUtil.getTimeWrtSystemTime(0);
        String endTime = InstanceUtil.addMinsToTime(startTime, 6*60);
        logger.info("Time range between : " + startTime + " and " + endTime);

        bundles[0].setInputFeedPeriodicity(1, Frequency.TimeUnit.hours);
        bundles[0].setInputFeedValidity(startTime, endTime);
        bundles[0].setInputFeedTableUri(tableUri);

        String feed = bundles[0].getDataSets().get(0);
        // set the cluster 2 as the target.
        feed = InstanceUtil.setFeedClusterWithTable(feed,
                XmlUtil.createValidity(startTime, endTime),
                XmlUtil.createRtention("months(9000)", ActionType.DELETE),
                Util.readClusterName(bundles[1].getClusters().get(0)), ClusterType.TARGET, null,
                tableUri, null);


        final String datePattern = StringUtils
                .join(new String[]{"yyyy", "MM", "dd", "HH"}, separator);
        List<String> dataDates = getDatesList(startTime, endTime, datePattern, 60);

        final ArrayList<String> dataset = createPeriodicDataset(dataDates, localHCatData, clusterFS, testHdfsDir);
        final String col1Name = "id";
        final String col2Name = "value";
        final String partitionColumn = "dt";

        ArrayList<HCatFieldSchema> cols = new ArrayList<HCatFieldSchema>();
        cols.add(new HCatFieldSchema(col1Name, HCatFieldSchema.Type.STRING, col1Name + " comment"));
        cols.add(new HCatFieldSchema(col2Name, HCatFieldSchema.Type.STRING, col2Name + " comment"));
        ArrayList<HCatFieldSchema> partitionCols = new ArrayList<HCatFieldSchema>();

        clusterHC.dropTable(dbName, tblName, true);
        partitionCols.add(new HCatFieldSchema(partitionColumn, HCatFieldSchema.Type.STRING, partitionColumn + " partition"));
        // create the table on cluster 1
        clusterHC.createTable(HCatCreateTableDesc
                .create(dbName, tblName, cols)
                .partCols(partitionCols)
                .ifNotExists(true)
                .isTableExternal(true)
                .location(testHdfsDir)
                .build());
        // create table on cluster 2 if we are copying data from cluster 1 to cluster2 then the
        // table also needs to exist on cluster 2
        cluster2HC.dropTable(dbName, tblName, true);
        cluster2HC.createTable(HCatCreateTableDesc
                .create(dbName, tblName, cols)
                .partCols(partitionCols)
                .ifNotExists(true)
                .isTableExternal(true)
                .location(testHdfsDir)
                .build());

        addPartitionsToTable(dataDates, dataset, "dt", dbName, tblName, clusterHC);

        AssertUtil.assertSucceeded(
                prism.getFeedHelper().submitAndSchedule(Util.URLS.SUBMIT_AND_SCHEDULE_URL,
                        feed));
        Thread.sleep(15000);
        //check if all coordinators exist
        Assert.assertEquals(InstanceUtil
                .checkIfFeedCoordExist(cluster2.getFeedHelper(), Util.readDatasetName(feed),
                        "REPLICATION"), 1);

        //replication should start, wait while it ends
        InstanceUtil.waitTillInstanceReachState(cluster2OC, Util.readEntityName(feed), 1,
                CoordinatorAction.Status.SUCCEEDED, 5, ENTITY_TYPE.FEED);

        /*
        bundles[0].submitFeedsScheduleProcess();

        InstanceUtil.waitTillInstanceReachState(
                clusterOC, bundles[0].getProcessName(), 1, CoordinatorAction.Status.SUCCEEDED, 5,
                ENTITY_TYPE.PROCESS);

        List<Path> inputData = HadoopUtil
                .getAllFilesRecursivelyHDFS(cluster, new Path(testHdfsDir + "/" + dataDates.get(0)));
        List<Path> outputData = HadoopUtil
                .getAllFilesRecursivelyHDFS(cluster2, new Path(testHdfsDir + "/dt=" + dataDates
                        .get(0)));
        AssertUtil.checkForPathsSizes(inputData, outputData);
        */
    }

    private void addPartitionsToTable(List<String> partitions, List<String> partitionLocations, String partitionCol,
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
            partitionDesc.add(HCatAddPartitionDesc.create(dbName, tableName, partitionLoc, onePartition).build());
        }
        hc.addPartitions(partitionDesc);
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

    /*
    @AfterMethod
    public void tearDown() throws Exception {
        removeBundles();
    }
    */
}