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


import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.jcraft.jsch.JSchException;
import org.apache.commons.lang.StringUtils;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.regression.core.enumsAndConstants.MerlinConstants;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.cluster.Interface;
import org.apache.falcon.entity.v0.cluster.Interfacetype;
import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.entity.v0.feed.Location;
import org.apache.falcon.entity.v0.feed.LocationType;
import org.apache.falcon.entity.v0.feed.Property;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.interfaces.IEntityManagerHelper;
import org.apache.falcon.regression.core.response.APIResult;
import org.apache.falcon.regression.core.response.ServiceResponse;
import org.apache.falcon.regression.core.supportClasses.Consumer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.http.HttpResponse;
import org.apache.falcon.request.BaseRequest;
import org.apache.falcon.request.RequestKeys;
import org.apache.hadoop.security.authentication.client.AuthenticationException;


import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.testng.Assert;
import org.apache.log4j.Logger;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;

import javax.xml.transform.OutputKeys;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

/**
 * util methods used across test.
 */
public final class Util {

    private Util() {
        throw new AssertionError("Instantiating utility class...");
    }
    private static final Logger LOGGER = Logger.getLogger(Util.class);

    public static ServiceResponse sendRequest(String url, String method)
        throws IOException, URISyntaxException, AuthenticationException {
        return sendRequest(url, method, null, null);
    }

    public static ServiceResponse sendRequest(String url, String method, String user)
        throws IOException, URISyntaxException, AuthenticationException {
        return sendRequest(url, method, null, user);
    }

    public static ServiceResponse sendRequest(String url, String method, String data,
                                              String user)
        throws IOException, URISyntaxException, AuthenticationException {
        BaseRequest request = new BaseRequest(url, method, user, data);
        request.addHeader(RequestKeys.CONTENT_TYPE_HEADER, RequestKeys.XML_CONTENT_TYPE);
        HttpResponse response = request.run();
        return new ServiceResponse(response);
    }

    public static String getProcessName(String data) {
        Process processElement = (Process) Entity.fromString(EntityType.PROCESS, data);
        return processElement.getName();
    }

    private static boolean isXML(String data) {

        if (data != null && data.trim().length() > 0) {
            if (data.trim().startsWith("<")) {
                return true; //find a better way of validation
            }
        }

        return false;
    }

    public static APIResult parseResponse(ServiceResponse response) throws JAXBException {

        if (!isXML(response.getMessage())) {
            return new APIResult(APIResult.Status.FAILED, response.getMessage(), "somerandomstring",
                response.getCode());
        }

        JAXBContext jc = JAXBContext.newInstance(APIResult.class);
        Unmarshaller u = jc.createUnmarshaller();
        APIResult temp;
        if (response.getMessage().contains("requestId")) {
            temp = (APIResult) u
                .unmarshal(new InputSource(new StringReader(response.getMessage())));
            temp.setStatusCode(response.getCode());
        } else {
            temp = new APIResult();
            temp.setStatusCode(response.getCode());
            temp.setMessage(response.getMessage());
            temp.setRequestId("");
            if (response.getCode() == 200) {
                temp.setStatus(APIResult.Status.SUCCEEDED);
            } else {
                temp.setStatus(APIResult.Status.FAILED);
            }
        }

        return temp;
    }

    public static List<String> getStoreInfo(IEntityManagerHelper helper, String subPath)
        throws IOException, JSchException {
        if (helper.getStoreLocation().startsWith("hdfs:")) {
            return HadoopUtil.getAllFilesHDFS(helper.getStoreLocation(),
                helper.getStoreLocation() + subPath);
        } else {
            return ExecUtil.runRemoteScriptAsSudo(helper.getQaHost(), helper.getUsername(),
                helper.getPassword(), "ls " + helper.getStoreLocation() + "/store" + subPath,
                helper.getUsername(), helper.getIdentityFile());
        }
    }

    public static String readEntityName(String data) {
        if (data.contains("uri:falcon:feed")) {
            return Entity.fromString(EntityType.FEED, data).getName();
        } else if (data.contains("uri:falcon:process")) {
            return Entity.fromString(EntityType.PROCESS, data).getName();
        } else {
            return Entity.fromString(EntityType.CLUSTER, data).getName();
        }
    }

    public static String getUniqueString() {

        return "-" + UUID.randomUUID().toString().split("-")[0];
    }

    public static List<String> getHadoopDataFromDir(ColoHelper helper, String feed, String dir)
        throws IOException {
        List<String> finalResult = new ArrayList<String>();

        String feedPath = getFeedPath(feed);
        int depth = feedPath.split(dir)[1].split("/").length - 1;
        List<Path> results = HadoopUtil.getAllDirsRecursivelyHDFS(helper,
            new Path(dir), depth);

        for (Path result : results) {
            int pathDepth = result.toString().split(dir)[1].split("/").length - 1;
            if (pathDepth == depth) {
                finalResult.add(result.toString().split(dir)[1]);
            }
        }

        return finalResult;
    }


    public static String setFeedProperty(String feed, String propertyName, String propertyValue) {

        Feed feedObject = (Feed) Entity.fromString(EntityType.FEED, feed);

        boolean found = false;
        for (Property prop : feedObject.getProperties().getProperties()) {
            //check if it is present
            if (prop.getName().equalsIgnoreCase(propertyName)) {
                prop.setValue(propertyValue);
                found = true;
                break;
            }
        }

        if (!found) {
            Property property = new Property();
            property.setName(propertyName);
            property.setValue(propertyValue);
            feedObject.getProperties().getProperties().add(property);
        }


        return feedObject.toString();

    }


    public static String getFeedPath(String feed) {
        Feed feedObject = (Feed) Entity.fromString(EntityType.FEED, feed);
        for (Location location : feedObject.getLocations().getLocations()) {
            if (location.getType() == LocationType.DATA) {
                return location.getPath();
            }
        }

        return null;
    }

    public static String insertLateFeedValue(String feed, Frequency frequency) {
        Feed feedObject = (Feed) Entity.fromString(EntityType.FEED, feed);
        feedObject.getLateArrival().setCutOff(frequency);
        return feedObject.toString();
    }

    public static void createLateDataFoldersWithRandom(ColoHelper prismHelper, String folderPrefix,
                                                       List<String> folderList)
        throws IOException {
        LOGGER.info("creating late data folders.....");
        Configuration conf = new Configuration();
        conf.set("fs.default.name", "hdfs://" + prismHelper.getProcessHelper().getHadoopURL() + "");

        final FileSystem fs = FileSystem.get(conf);

        folderList.add("somethingRandom");

        for (final String folder : folderList) {
            fs.mkdirs(new Path(folderPrefix + folder));
        }

        LOGGER.info("created all late data folders.....");
    }

    public static void copyDataToFolders(ColoHelper prismHelper, List<String> folderList,
                                         String directory, String folderPrefix)
        throws IOException {
        LOGGER.info("copying data into folders....");
        List<String> fileLocations = new ArrayList<String>();
        File[] files = new File(directory).listFiles();
        if (files != null) {
            for (final File file : files) {
                fileLocations.add(file.toString());
            }
        }
        copyDataToFolders(prismHelper, folderPrefix, folderList,
            fileLocations.toArray(new String[fileLocations.size()]));
    }

    public static void copyDataToFolders(ColoHelper prismHelper, final String folderPrefix,
                                         List<String> folderList,
                                         String... fileLocations)
        throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.default.name", "hdfs://" + prismHelper.getProcessHelper().getHadoopURL());

        final FileSystem fs = FileSystem.get(conf);

        for (final String folder : folderList) {
            boolean r;
            String folderSpace = folder.replaceAll("/", "_");
            File f = new File(
                OSUtil.NORMAL_INPUT + folderSpace
                        +
                    ".txt");
            if (!f.exists()) {
                r = f.createNewFile();
                if (!r) {
                    LOGGER.info("file could not be created");
                }
            }


            FileWriter fr = new FileWriter(f);
            fr.append("folder");
            fr.close();
            fs.copyFromLocalFile(new Path(f.getAbsolutePath()), new Path(folderPrefix + folder));
            r = f.delete();
            if (!r) {
                LOGGER.info("delete was not successful");
            }

            Path[] srcPaths = new Path[fileLocations.length];
            for (int i = 0; i < srcPaths.length; ++i) {
                srcPaths[i] = new Path(fileLocations[i]);
            }
            LOGGER.info("copying  " + Arrays.toString(srcPaths) + " to " + folderPrefix + folder);
            fs.copyFromLocalFile(false, true, srcPaths, new Path(folderPrefix + folder));
        }
    }


    public static String setFeedPathValue(String feed, String pathValue) {
        Feed feedObject = (Feed) Entity.fromString(EntityType.FEED, feed);
        for (Location location : feedObject.getLocations().getLocations()) {
            if (location.getType() == LocationType.DATA) {
                location.setPath(pathValue);
            }
        }
        return feedObject.toString();
    }


    public static String findFolderBetweenGivenTimeStamps(DateTime startTime, DateTime endTime,
                                                          List<String> folderList) {
        DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy/MM/dd/HH/mm");

        for (String folder : folderList) {
            if (folder.compareTo(formatter.print(startTime)) >= 0
                    &&
                folder.compareTo(formatter.print(endTime)) <= 0) {
                return folder;
            }
        }
        return null;
    }

    public static void lateDataReplenish(ColoHelper prismHelper, int interval,
                                         int minuteSkip, String folderPrefix) throws IOException {
        List<String> folderData = TimeUtil.getMinuteDatesOnEitherSide(interval, minuteSkip);

        Util.createLateDataFoldersWithRandom(prismHelper, folderPrefix, folderData);
        Util.copyDataToFolders(prismHelper, folderData,
            OSUtil.NORMAL_INPUT, folderPrefix);
    }

    public static void createLateDataFolders(ColoHelper prismHelper, List<String> folderList,
                                             final String folderPrefix)
        throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.default.name", "hdfs://" + prismHelper.getProcessHelper().getHadoopURL() + "");

        final FileSystem fs = FileSystem.get(conf);

        for (final String folder : folderList) {
            fs.mkdirs(new Path(folderPrefix + folder));
        }
    }

    public static void injectMoreData(ColoHelper prismHelper, final String remoteLocation,
                                      String localLocation)
        throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.default.name", "hdfs://" + prismHelper.getClusterHelper().getHadoopURL() + "");

        final FileSystem fs = FileSystem.get(conf);

        File[] files = new File(localLocation).listFiles();
        assert files != null;
        for (final File file : files) {
            if (!file.isDirectory()) {
                String path = remoteLocation + "/"
                        +
                    System.currentTimeMillis() / 1000 + "/";
                LOGGER.info("inserting data@ " + path);
                fs.copyFromLocalFile(new Path(file.getAbsolutePath()), new Path(path));
            }
        }

    }

    public static String setFeedName(String feedString, String newName) {
        Feed feedObject = (Feed) Entity.fromString(EntityType.FEED, feedString);
        feedObject.setName(newName);
        return feedObject.toString().trim();
    }

    public static String setClusterNameInFeed(String feedString, String clusterName,
                                              int clusterIndex) {
        Feed feedObject = (Feed) Entity.fromString(EntityType.FEED, feedString);
        feedObject.getClusters().getClusters().get(clusterIndex).setName(clusterName);
        return feedObject.toString().trim();
    }

    public static Cluster getClusterObject(String clusterXML) {
        return (Cluster) Entity.fromString(EntityType.CLUSTER, clusterXML);
    }

    public static List<String> getInstanceFinishTimes(ColoHelper coloHelper, String workflowId)
        throws IOException, JSchException {
        List<String> raw = ExecUtil.runRemoteScriptAsSudo(coloHelper.getProcessHelper()
                .getQaHost(), coloHelper.getProcessHelper().getUsername(),
            coloHelper.getProcessHelper().getPassword(),
            "cat /var/log/ivory/application.* | grep \"" + workflowId + "\" | grep "
                    +
                "\"Received\" | awk '{print $2}'",
            coloHelper.getProcessHelper().getUsername(),
            coloHelper.getProcessHelper().getIdentityFile()
        );
        List<String> finalList = new ArrayList<String>();
        for (String line : raw) {
            finalList.add(line.split(",")[0]);

        }
        return finalList;
    }

    public static List<String> getInstanceRetryTimes(ColoHelper coloHelper, String workflowId)
        throws IOException, JSchException {
        List<String> raw = ExecUtil.runRemoteScriptAsSudo(coloHelper.getProcessHelper()
                .getQaHost(), coloHelper.getProcessHelper().getUsername(),
            coloHelper.getProcessHelper().getPassword(),
            "cat /var/log/ivory/application.* | grep \"" + workflowId + "\" | grep "
                    +
                "\"Retrying attempt\" | awk '{print $2}'",
            coloHelper.getProcessHelper().getUsername(),
            coloHelper.getProcessHelper().getIdentityFile()
        );
        List<String> finalList = new ArrayList<String>();
        for (String line : raw) {
            finalList.add(line.split(",")[0]);
        }

        return finalList;
    }

    public static void shutDownService(IEntityManagerHelper helper)
        throws IOException, JSchException {
        ExecUtil.runRemoteScriptAsSudo(helper.getQaHost(), helper.getUsername(),
            helper.getPassword(), helper.getServiceStopCmd(),
            helper.getServiceUser(), helper.getIdentityFile());
        TimeUtil.sleepSeconds(10);
    }

    public static void startService(IEntityManagerHelper helper)
        throws IOException, JSchException, AuthenticationException, URISyntaxException {

        ExecUtil.runRemoteScriptAsSudo(helper.getQaHost(), helper.getUsername(),
            helper.getPassword(), helper.getServiceStartCmd(), helper.getServiceUser(),
            helper.getIdentityFile());
        int statusCode = 0;
        for (int tries = 20; tries > 0; tries--) {
            try {
                statusCode = Util.sendRequest(helper.getHostname(), "get").getCode();
            } catch (IOException e) {
                LOGGER.info(e.getMessage());
            }
            if (statusCode == 200) {
                return;
            }
            TimeUtil.sleepSeconds(5);
        }
        throw new RuntimeException("Service on" + helper.getHostname() + " did not start!");
    }

    public static void restartService(IEntityManagerHelper helper)
        throws IOException, JSchException, AuthenticationException, URISyntaxException {
        LOGGER.info("restarting service for: " + helper.getQaHost());

        shutDownService(helper);
        startService(helper);
    }

    public static Process getProcessObject(String processData) {
        return (Process) Entity.fromString(EntityType.PROCESS, processData);
    }

    public static void dumpConsumerData(Consumer consumer) {
        LOGGER.info("dumping all queue data:");

        for (HashMap<String, String> data : consumer.getMessageData()) {
            LOGGER.info("*************************************");
            for (String key : data.keySet()) {
                LOGGER.info(key + "=" + data.get(key));
            }
            LOGGER.info("*************************************");
        }
    }

    public static void lateDataReplenish(ColoHelper prismHelper, int interval,
                                         int minuteSkip,
                                         String folderPrefix, String postFix)
        throws IOException {
        List<String> folderPaths = TimeUtil.getMinuteDatesOnEitherSide(interval, minuteSkip);
        LOGGER.info("folderData: " + folderPaths.toString());

        if (postFix != null) {
            for (int i = 0; i < folderPaths.size(); i++) {
                folderPaths.set(i, folderPaths.get(i) + postFix);
            }
        }

        Util.createLateDataFolders(prismHelper, folderPaths, folderPrefix);
        Util.copyDataToFolders(prismHelper, folderPrefix, folderPaths,
            OSUtil.NORMAL_INPUT + "_SUCCESS",
            OSUtil.NORMAL_INPUT + "log_01.txt");
    }

    public static void lateDataReplenishWithoutSuccess(ColoHelper prismHelper, int interval,
                                                        int minuteSkip, String folderPrefix,
                                                        String postFix)
        throws IOException {
        List<String> folderPaths = TimeUtil.getMinuteDatesOnEitherSide(interval, minuteSkip);
        LOGGER.info("folderData: " + folderPaths.toString());

        if (postFix != null) {
            for (int i = 0; i < folderPaths.size(); i++) {
                folderPaths.set(i, folderPaths.get(i) + postFix);
            }
        }

        Util.createLateDataFolders(prismHelper, folderPaths, folderPrefix);
        Util.copyDataToFolders(prismHelper, folderPrefix, folderPaths,
            OSUtil.NORMAL_INPUT + "log_01.txt");
    }


    public static void putFileInFolderHDFS(ColoHelper prismHelper, int interval, int minuteSkip,
                                           String folderPrefix, String fileToBePut)
        throws IOException {
        List<String> folderPaths = TimeUtil.getMinuteDatesOnEitherSide(interval, minuteSkip);
        LOGGER.info("folderData: " + folderPaths.toString());

        Util.createLateDataFolders(prismHelper, folderPaths, folderPrefix);

        if (fileToBePut.equals("_SUCCESS")) {
            Util.copyDataToFolders(prismHelper, folderPrefix, folderPaths,
                    OSUtil.NORMAL_INPUT + "_SUCCESS");
        } else {
            Util.copyDataToFolders(prismHelper, folderPrefix, folderPaths,
                    OSUtil.NORMAL_INPUT + "log_01.txt");
        }

    }

    public static String getEnvClusterXML(String cluster, String prefix) {

        Cluster clusterObject =
            getClusterObject(cluster);
        if ((null == prefix) || prefix.isEmpty()) {
            prefix = "";
        } else {
            prefix = prefix + ".";
        }

        String hcatEndpoint = Config.getProperty(prefix + "hcat_endpoint");
        String hbaseEndpoint = Config.getProperty(prefix + "hbase_endpoint");
        //now read and set relevant values
        for (Interface iface : clusterObject.getInterfaces().getInterfaces()) {
            if (iface.getType() == Interfacetype.READONLY) {
                iface.setEndpoint(Config.getProperty(prefix + "cluster_readonly"));
            } else if (iface.getType() == Interfacetype.WRITE) {
                iface.setEndpoint(Config.getProperty(prefix + "cluster_write"));
            } else if (iface.getType() == Interfacetype.EXECUTE) {
                iface.setEndpoint(Config.getProperty(prefix + "cluster_execute"));
            } else if (iface.getType() == Interfacetype.WORKFLOW) {
                iface.setEndpoint(Config.getProperty(prefix + "oozie_url"));
            } else if (iface.getType() == Interfacetype.MESSAGING) {
                iface.setEndpoint(Config.getProperty(prefix + "activemq_url"));
            } else if (iface.getType() == Interfacetype.REGISTRY) {
                iface.setEndpoint(hcatEndpoint);
            }
        }

        if (StringUtils.isNotBlank(hbaseEndpoint)) {
            List<org.apache.falcon.entity.v0.cluster.Property> properties = clusterObject.getProperties()
                    .getProperties();
            for (org.apache.falcon.entity.v0.cluster.Property property : properties) {
                if (property.getName().equals("hbase.zookeeper.quorum")) {
                    property.setValue(hbaseEndpoint);
                }
            }
        }


        //set colo name:
        clusterObject.setColo(Config.getProperty(prefix + "colo"));
        // properties in the cluster needed when secure mode is on
        if (MerlinConstants.IS_SECURE) {
            // get the properties object for the cluster
            org.apache.falcon.entity.v0.cluster.Properties clusterProperties =
                clusterObject.getProperties();
            // add the namenode principal to the properties object
            clusterProperties.getProperties().add(getFalconClusterPropertyObject(
                "dfs.namenode.kerberos.principal",
                Config.getProperty(prefix + "namenode.kerberos.principal", "none")));

            // add the hive meta store principal to the properties object
            clusterProperties.getProperties().add(getFalconClusterPropertyObject(
                "hive.metastore.kerberos"
                        +
                    ".principal",
                Config.getProperty(prefix + "hive.metastore.kerberos"
                        +
                    ".principal", "none")
            ));

            // Until oozie has better integration with secure hive we need to send the properites to
            // falcon.
            // hive.metastore.sasl.enabled = true
            clusterProperties.getProperties()
                .add(getFalconClusterPropertyObject("hive.metastore.sasl"
                        +
                    ".enabled", "true"));
            // Only set the metastore uri if its not empty or null.
            if (null != hcatEndpoint && !hcatEndpoint.isEmpty()) {
                //hive.metastore.uris
                clusterProperties.getProperties()
                    .add(getFalconClusterPropertyObject("hive.metastore.uris", hcatEndpoint));
            }
        }
        return clusterObject.toString();
    }

    public static org.apache.falcon.entity.v0.cluster.Property
    getFalconClusterPropertyObject(String name, String value) {
        org.apache.falcon.entity.v0.cluster.Property property = new org
            .apache.falcon.entity.v0.cluster.Property();
        property.setName(name);
        property.setValue(value);
        return property;
    }

    public static EntityType getEntityType(String entity) {
        if (entity.contains("uri:falcon:process:0.1")) {
            return EntityType.PROCESS;
        } else if (entity.contains("uri:falcon:cluster:0.1")) {
            return EntityType.CLUSTER;
        } else if (entity.contains("uri:falcon:feed:0.1")) {
            return EntityType.FEED;
        }
        return null;
    }

    public static boolean isDefinitionSame(ColoHelper server1, ColoHelper server2,
                                           String entity)
        throws URISyntaxException, IOException, AuthenticationException, JAXBException,
        SAXException {
        return XmlUtil.isIdentical(getEntityDefinition(server1, entity, true),
            getEntityDefinition(server2, entity, true));
    }

    /**
     * emuns used for instance api.
     */
    public enum URLS {

        LIST_URL("/api/entities/list"),
        SUBMIT_URL("/api/entities/submit"),
        GET_ENTITY_DEFINITION("/api/entities/definition"),
        DELETE_URL("/api/entities/delete"),
        SCHEDULE_URL("/api/entities/schedule"),
        VALIDATE_URL("/api/entities/validate"),
        SUSPEND_URL("/api/entities/suspend"),
        RESUME_URL("/api/entities/resume"),
        UPDATE("/api/entities/update"),
        STATUS_URL("/api/entities/status"),
        SUBMIT_AND_SCHEDULE_URL("/api/entities/submitAndSchedule"),
        INSTANCE_RUNNING("/api/instance/running"),
        INSTANCE_STATUS("/api/instance/status"),
        INSTANCE_KILL("/api/instance/kill"),
        INSTANCE_RESUME("/api/instance/resume"),
        INSTANCE_SUSPEND("/api/instance/suspend"),
        INSTANCE_RERUN("/api/instance/rerun"),
        INSTANCE_SUMMARY("/api/instance/summary"),
        INSTANCE_PARAMS("/api/instance/params");
        private final String url;

        URLS(String url) {
            this.url = url;
        }

        public String getValue() {
            return this.url;
        }
    }


    public static String getPathPrefix(String pathString) {
        return pathString.substring(0, pathString.indexOf("$"));
    }

    public static String getFileNameFromPath(String path) {

        return path.substring(path.lastIndexOf("/") + 1, path.length());
    }


    public static String getMethodType(String url) {
        List<String> postList = new ArrayList<String>();
        postList.add("/entities/validate");
        postList.add("/entities/submit");
        postList.add("/entities/submitAndSchedule");
        postList.add("/entities/suspend");
        postList.add("/entities/resume");
        postList.add("/instance/kill");
        postList.add("/instance/suspend");
        postList.add("/instance/resume");
        postList.add("/instance/rerun");
        for (String item : postList) {
            if (url.toLowerCase().contains(item)) {
                return "post";
            }
        }
        List<String> deleteList = new ArrayList<String>();
        deleteList.add("/entities/delete");
        for (String item : deleteList) {
            if (url.toLowerCase().contains(item)) {
                return "delete";
            }
        }

        return "get";
    }

    public static String prettyPrintXml(final String xmlString) {
        if (xmlString == null) {
            return null;
        }
        try {
            Source xmlInput = new StreamSource(new StringReader(xmlString));
            StringWriter stringWriter = new StringWriter();
            StreamResult xmlOutput = new StreamResult(stringWriter);
            TransformerFactory transformerFactory = TransformerFactory.newInstance();
            transformerFactory.setAttribute("indent-number", "2");
            Transformer transformer = transformerFactory.newTransformer();
            transformer.setOutputProperty(OutputKeys.INDENT, "yes");
            transformer.transform(xmlInput, xmlOutput);
            return xmlOutput.getWriter().toString();
        } catch (TransformerConfigurationException e) {
            return xmlString;
        } catch (TransformerException e) {
            return xmlString;
        }

    }

    public static String prettyPrintJson(final String jsonString) {
        if (jsonString == null) {
            return null;
        }
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        JsonElement json = new JsonParser().parse(jsonString);

        return gson.toJson(json);
    }

    public static String prettyPrintXmlOrJson(final String str) {
        if (str == null) {
            return null;
        }
        String cleanStr = str.trim();
        //taken from http://stackoverflow.com/questions/7256142/way-to-quickly-check-if-string-is-xml-or-json-in-c-sharp
        if (cleanStr.startsWith("{") || cleanStr.startsWith("[")) {
            return prettyPrintJson(cleanStr);
        }
        if (cleanStr.startsWith("<")) {
            return prettyPrintXml(cleanStr);
        }
        LOGGER.warn("The string does not seem to be either json or xml: " + cleanStr);
        return str;
    }

    public static String getEntityDefinition(ColoHelper cluster,
                                             String entity,
                                             boolean shouldReturn) throws
        JAXBException,
        IOException, URISyntaxException, AuthenticationException {
        EntityType type = getEntityType(entity);
        IEntityManagerHelper helper;
        if (EntityType.PROCESS == type) {
            helper = cluster.getProcessHelper();
        } else if (EntityType.FEED == type) {
            helper = cluster.getFeedHelper();
        } else {
            helper = cluster.getClusterHelper();
        }

        ServiceResponse response = helper.getEntityDefinition(URLS
            .GET_ENTITY_DEFINITION, entity);

        if (shouldReturn) {
            AssertUtil.assertSucceeded(response);
        } else {
            AssertUtil.assertFailed(response);
        }
        String result = response.getMessage();
        Assert.assertNotNull(result);

        return result;
    }
}
