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

import org.apache.commons.lang.StringUtils;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Util methods related to hadoop.
 */
public final class HadoopUtil {

    private HadoopUtil() {
        throw new AssertionError("Instantiating utility class...");
    }

    private static final Logger LOGGER = Logger.getLogger(HadoopUtil.class);

    public static Configuration getHadoopConfiguration(ColoHelper prismHelper) {
        Configuration conf = new Configuration();
        String hadoopURL = prismHelper.getProcessHelper().getHadoopURL();
        if (!hadoopURL.startsWith("hdfs://"))
        conf.set("fs.default.name", "hdfs://" + hadoopURL + "");
        else
            conf.set("fs.default.name", hadoopURL + "");
        return conf;
    }

    @SuppressWarnings("deprecation")
    public static List<Path> getAllFilesRecursivelyHDFS(
            ColoHelper colcoHelper, Path location) throws IOException {

        List<Path> returnList = new ArrayList<Path>();

        Configuration conf = HadoopUtil.getHadoopConfiguration(colcoHelper);

        final FileSystem fs = FileSystem.get(conf);

        if (location.toString().contains("*")) {
            location = new Path(
                    location.toString().substring(0, location.toString().indexOf("*") - 1));
        }

        FileStatus[] stats = fs.listStatus(location);

        for (FileStatus stat : stats) {
            //Util.print("crrentPath: " +stat.getPath().toUri().getPath()); // gives directory name
            if (!stat.isDir()) {
                if (!stat.getPath().getName().contains("_SUCCESS")) {
                    returnList.add(stat.getPath());
                }
            } else {
                returnList.addAll(getAllFilesRecursivelyHDFS(colcoHelper, stat.getPath()));
            }

        }

        return returnList;
    }

    public static List<String> getAllFilesHDFS(String hadoopURL, String location)
            throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.default.name", hadoopURL);
        final FileSystem fs = FileSystem.get(conf);

        return getAllFilesHDFS(fs, new Path(location));

    }

    @SuppressWarnings("deprecation")
    public static List<String> getAllFilesHDFS(FileSystem fs, Path location) throws IOException {

        List<String> files = new ArrayList<String>();
        if (!fs.exists(location)) {
            return files;
        }
        FileStatus[] stats = fs.listStatus(location);

        for (FileStatus stat : stats) {
            if (!stat.isDir()) {
                files.add(stat.getPath().toString());
            }
        }
        return files;
    }

    public static List<Path> getAllDirsRecursivelyHDFS(
            ColoHelper colcoHelper, Path location, int depth) throws IOException {

        Configuration conf = HadoopUtil.getHadoopConfiguration(colcoHelper);
        final FileSystem fs = FileSystem.get(conf);

        return getAllDirsRecursivelyHDFS(fs, location, depth);
    }

    @SuppressWarnings("deprecation")
    private static List<Path> getAllDirsRecursivelyHDFS(
            FileSystem fs, Path location, int depth) throws IOException {

        List<Path> returnList = new ArrayList<Path>();

        FileStatus[] stats = fs.listStatus(location);

        for (FileStatus stat : stats) {
            if (stat.isDir()) {
                returnList.add(stat.getPath());
                if (depth > 0) {
                    returnList.addAll(getAllDirsRecursivelyHDFS(fs, stat.getPath(), depth - 1));
                }
            }

        }

        return returnList;
    }

    @SuppressWarnings("deprecation")
    public static List<Path> getAllFilesRecursivelyHDFS(
            ColoHelper coloHelper, Path location, String... ignoreFolders) throws IOException {

        List<Path> returnList = new ArrayList<Path>();

        Configuration conf = HadoopUtil.getHadoopConfiguration(coloHelper);

        final FileSystem fs = FileSystem.get(conf);

        FileStatus[] stats;
        try {

            stats = fs.listStatus(location);

        } catch (FileNotFoundException e) {
            e.printStackTrace();
            return new ArrayList<Path>();
        }

        //Util.print("getAllFilesRecursivelyHDFS: "+location);

        if (stats == null) {
            return returnList;
        }
        for (FileStatus stat : stats) {

            //Util.print("checking in DIR: "+stat.getPath());

            if (!stat.isDir()) {
                if (!checkIfIsIgnored(stat.getPath().toUri().toString(), ignoreFolders)) {
                    //	Util.print("adding File: " +stat.getPath().toUri().getPath()); // gives
                    // file name

                    returnList.add(stat.getPath());
                }
            } else {
                //	Util.print("recursing for DIR: " +stat.getPath().toUri().getPath()); // gives
                // directory name

                returnList.addAll(getAllFilesRecursivelyHDFS(coloHelper, stat.getPath(),
                        ignoreFolders));
            }
        }

        return returnList;

    }

    private static boolean checkIfIsIgnored(String folder,
            String[] ignoreFolders) {

        for (String ignoreFolder : ignoreFolders) {

            if (folder.contains(ignoreFolder)) {
                //	Util.print("ignored Folder found: "+ignoreFolders[i]);
                return true;
            }
        }
        return false;
    }

    public static void deleteFile(ColoHelper coloHelper, Path fileHDFSLocaltion)
            throws IOException {
        Configuration conf = HadoopUtil.getHadoopConfiguration(coloHelper);

        final FileSystem fs = FileSystem.get(conf);

        fs.delete(fileHDFSLocaltion, false);

    }

    public static void copyDataToFolder(ColoHelper coloHelper, final Path folder,
            final String fileLocation)
            throws IOException {
        Configuration conf = new Configuration();
        String hadoopURL = coloHelper.getProcessHelper().getHadoopURL();
        if (!hadoopURL.startsWith("hdfs://")) {
            conf.set("fs.default.name", "hdfs://"
                    + hadoopURL);
        } else {
            conf.set("fs.default.name",
                    hadoopURL);
        }

        final FileSystem fs = FileSystem.get(conf);
        copyDataToFolder(fs, folder.toString(), fileLocation);
    }

    public static void copyDataToFolder(final FileSystem fs, final String dstHdfsDir,
            final String srcFileLocation)
            throws IOException {
        LOGGER.info(String.format("Copying local dir %s to hdfs location %s on %s",
                srcFileLocation,
                dstHdfsDir, fs.getConf().get("fs.default.name")));
        fs.copyFromLocalFile(new Path(srcFileLocation), new Path(dstHdfsDir));
    }

    public static void uploadDir(final FileSystem fs, final String dstHdfsDir,
            final String localLocation)
            throws IOException {
        LOGGER.info(String.format("Uploading local dir %s to hdfs location %s", localLocation,
                dstHdfsDir));
        HadoopUtil.deleteDirIfExists(dstHdfsDir, fs);
        HadoopUtil.copyDataToFolder(fs, dstHdfsDir, localLocation);
    }

    @SuppressWarnings("deprecation")
    public static List<String> getHDFSSubFoldersName(FileSystem fs,
            String baseDir) throws IOException {

        List<String> returnList = new ArrayList<String>();

        FileStatus[] stats = fs.listStatus(new Path(baseDir));

        for (FileStatus stat : stats) {
            if (stat.isDir()) {
                returnList.add(stat.getPath().getName());
            }

        }

        return returnList;
    }

    public static boolean isFilePresentHDFS(ColoHelper prismHelper,
            String hdfsPath, String fileToCheckFor)
            throws IOException {

        LOGGER.info("getting file from folder: " + hdfsPath);
        Configuration conf = new Configuration();
        String hadoopURL = prismHelper.getProcessHelper().getHadoopURL() ;
        if (!hadoopURL.startsWith("hdfs://"))
        conf.set("fs.default.name", "hdfs://" + hadoopURL + "");
        else
            conf.set("fs.default.name", hadoopURL + "");

        final FileSystem fs = FileSystem.get(conf);

        List<String> fileNames = getAllFileNamesFromHDFS(fs, hdfsPath);

        for (String filePath : fileNames) {

            if (filePath.contains(fileToCheckFor)) {
                return true;
            }
        }

        return false;
    }

    @SuppressWarnings("deprecation")
    private static List<String> getAllFileNamesFromHDFS(
            FileSystem fs, String hdfsPath) throws IOException {

        List<String> returnList = new ArrayList<String>();

        LOGGER.info("getting file from folder: " + hdfsPath);
        FileStatus[] stats = fs.listStatus(new Path(hdfsPath));

        for (FileStatus stat : stats) {
            String currentPath = stat.getPath().toUri().getPath(); // gives directory name
            if (!stat.isDir()) {
                returnList.add(currentPath);
            }

        }
        return returnList;
    }

    public static void createDir(String path, FileSystem... fileSystems) throws IOException {

        for (FileSystem fs : fileSystems) {
            deleteDirIfExists(path, fs);
            LOGGER.info("creating hdfs dir: " + path + " on " + fs
                    .getConf().get("fs.default.name"));
            fs.mkdirs(new Path(path));
        }
    }

    public static void deleteDirIfExists(String hdfsPath, FileSystem fs) throws IOException {
        Path path = new Path(hdfsPath);
        if (fs.exists(path)) {
            LOGGER.info(String.format("Deleting HDFS path: %s on %s", path,
                    fs.getConf().get("fs.default.name")));
            fs.delete(path, true);
        } else {
            LOGGER.info(String.format(
                    "Not deleting non-existing HDFS path: %s on %s", path,
                    fs.getConf().get("fs.default.name")));
        }
    }

    public static FileSystem getFileSystem(String fs) throws IOException {
        Configuration conf = new Configuration();
        if (!fs.startsWith("hdfs://")) {
            conf.set("fs.default.name", "hdfs://" + fs);
        } else {
            conf.set("fs.default.name", fs);
        }
        return FileSystem.get(conf);
    }

    public static List<String> getWriteLocations(ColoHelper coloHelper,
            List<String> readOnlyLocations) {
        List<String> writeFolders = new ArrayList<String>();
        final String clusterReadonly = coloHelper.getClusterHelper().getClusterReadonly();
        final String clusterWrite = coloHelper.getClusterHelper().getClusterWrite();
        for (String location : readOnlyLocations) {
            writeFolders.add(location.trim().replaceFirst(clusterReadonly, clusterWrite));
        }
        return writeFolders;
    }

    public static void flattenAndPutDataInFolder(FileSystem fs, String inputPath,
            List<String> remoteLocations) throws IOException {
        flattenAndPutDataInFolder(fs, inputPath, "", remoteLocations);
    }

    public static List<String> flattenAndPutDataInFolder(FileSystem fs, String inputPath,
            String remotePathPrefix,
            List<String> remoteLocations) throws IOException {
        if (StringUtils.isEmpty(remotePathPrefix)) {
            deleteDirIfExists(remotePathPrefix, fs);
        }
        LOGGER.info("Creating data in folders: \n" + remoteLocations);
        File input = new File(inputPath);
        File[] files = input.isDirectory() ? input.listFiles() : new File[] { input };
        List<Path> filePaths = new ArrayList<Path>();
        assert files != null;
        for (final File file : files) {
            if (!file.isDirectory()) {
                final Path filePath = new Path(file.getAbsolutePath());
                filePaths.add(filePath);
            }
        }

        if (!remotePathPrefix.endsWith("/") && !remoteLocations.get(0).startsWith("/")) {
            remotePathPrefix += "/";
        }

        List<String> locations = new ArrayList<String>();
        for (String remoteDir : remoteLocations) {
            String remoteLocation = remotePathPrefix + remoteDir;
            locations.add(remoteLocation);
            LOGGER.info(String.format("copying to: %s files: %s",
                    fs.getUri() + remoteLocation, Arrays.toString(files)));
            if (!fs.exists(new Path(remoteLocation))) {
                fs.mkdirs(new Path(remoteLocation));
            }

            fs.copyFromLocalFile(false, true, filePaths.toArray(new Path[filePaths.size()]),
                    new Path(remoteLocation));
        }
        return locations;
    }

    public static List<String> createEmptyDirWithinDatesAndPrefix(ColoHelper colo,
            DateTime startDateJoda,
            DateTime endDateJoda,
            String prefix,
            int interval) throws IOException {
        List<String> dataDates = TimeUtil.getMinuteDatesOnEitherSide(startDateJoda, endDateJoda, interval);
        for (int i = 0; i < dataDates.size(); i++) {
            dataDates.set(i, prefix + dataDates.get(i));
        }

        List<String> dataFolder = new ArrayList<String>();

        for (String dataDate : dataDates) {
            dataFolder.add(dataDate);
        }

        InstanceUtil.createHDFSFolders(colo, dataFolder);
        return dataFolder;
    }

}
