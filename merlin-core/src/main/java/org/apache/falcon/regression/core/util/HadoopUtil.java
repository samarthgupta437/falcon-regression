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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.testng.log4testng.Logger;

import java.io.File;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;


public class HadoopUtil {

    static Logger logger = Logger.getLogger(HadoopUtil.class);

    public static void setSystemPropertyHDFS() {
        System.setProperty("java.security.krb5.realm", "");
        System.setProperty("java.security.krb5.kdc", "");

    }

    public static Configuration getHadoopConfiguration(ColoHelper prismHelper) {
        Configuration conf = new Configuration();
        conf.set("fs.default.name", "hdfs://" + prismHelper.getProcessHelper().getHadoopURL() + "");
        return conf;
    }

    public static List<Path> getAllFilesRecursivelyHDFS(
            ColoHelper colcoHelper, Path location) throws IOException {

        setSystemPropertyHDFS();
        List<Path> returnList = new ArrayList<Path>();

        Configuration conf = HadoopUtil.getHadoopConfiguration(colcoHelper);

        final FileSystem fs = FileSystem.get(conf);

        if (location.toString().contains("*"))
            location = new Path(
                    location.toString().substring(0, location.toString().indexOf("*") - 1));

        FileStatus[] stats = fs.listStatus(location);

        for (FileStatus stat : stats) {
            //Util.print("crrentPath: " +stat.getPath().toUri().getPath()); // gives directory name
            if (!stat.isDir()) {
                if (!stat.getPath().getName().contains("_SUCCESS"))
                    returnList.add(stat.getPath());
            } else
                returnList.addAll(getAllFilesRecursivelyHDFS(colcoHelper, stat.getPath()));


        }

        return returnList;
    }

    public static List<String> getAllFilesHDFS(String hadoopURL, String location)
    throws IOException {
        setSystemPropertyHDFS();
        Configuration conf = new Configuration();
        conf.set("fs.default.name", hadoopURL);
        final FileSystem fs = FileSystem.get(conf);

        return getAllFilesHDFS(fs, new Path(location));

    }

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

        setSystemPropertyHDFS();
        Configuration conf = HadoopUtil.getHadoopConfiguration(colcoHelper);
        final FileSystem fs = FileSystem.get(conf);

        return getAllDirsRecursivelyHDFS(fs, location, depth);
    }


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


    public static List<Path> getAllFilesRecursivelyHDFS(
            ColoHelper coloHelper, Path location, String... ignoreFolders) throws IOException {

        setSystemPropertyHDFS();
        List<Path> returnList = new ArrayList<Path>();

        Configuration conf = HadoopUtil.getHadoopConfiguration(coloHelper);

        final FileSystem fs = FileSystem.get(conf);

        FileStatus[] stats = fs.listStatus(location);

        //Util.print("getAllFilesRecursivelyHDFS: "+location);

        if (stats == null)
            return returnList;
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


    public static List<Path> getAllFilesRecursivelyHDFS(
            Configuration conf, Path location, String... ignoreFolders) throws IOException {

        setSystemPropertyHDFS();
        List<Path> returnList = new ArrayList<Path>();

        final FileSystem fs = FileSystem.get(conf);

        FileStatus[] stats = fs.listStatus(location);

        //	Util.print("getAllFilesRecursivelyHDFS: "+location);

        if (stats == null)
            return returnList;
        for (FileStatus stat : stats) {

            //	Util.print("checking in DIR: "+stat.getPath());

            if (!stat.isDir()) {
                if (!checkIfIsIgnored(stat.getPath().toUri().toString(), ignoreFolders)) {
                    //	Util.print("adding File: " +stat.getPath().toUri().getPath()); // gives
                    // file name

                    returnList.add(stat.getPath());
                }
            } else {
                //Util.print("recursing for DIR: " +stat.getPath().toUri().getPath()); // gives
                // directory name

                returnList.addAll(getAllFilesRecursivelyHDFS(new Configuration(), stat.getPath(),
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
        setSystemPropertyHDFS();
        Configuration conf = HadoopUtil.getHadoopConfiguration(coloHelper);

        final FileSystem fs = FileSystem.get(conf);

        fs.delete(fileHDFSLocaltion, false);

    }

    public static void copyDataToFolder(ColoHelper coloHelper, final Path folder,
                                        final String fileLocation)
    throws IOException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("fs.default.name", "hdfs://"
                + coloHelper.getProcessHelper().getHadoopURL());

        final FileSystem fs = FileSystem.get(conf);
        copyDataToFolder(fs, folder.toString(), fileLocation);
    }

    public static void copyDataToFolder(final FileSystem fs, final String dstHdfsDir,
                                         final String srcFileLocation)
    throws IOException, InterruptedException {
        UserGroupInformation user = UserGroupInformation
                .createRemoteUser("hdfs");


        user.doAs(new PrivilegedExceptionAction<Boolean>() {

            @Override
            public Boolean run() throws IOException {
                fs.copyFromLocalFile(new Path(srcFileLocation), new Path(dstHdfsDir));
                return true;

            }
        });
    }

    public static void uploadDir(final FileSystem fs, final String dstHdfsDir,
                                 final String localLocation)
    throws IOException, InterruptedException {
        HadoopUtil.deleteDirIfExists(dstHdfsDir, fs);
        HadoopUtil.copyDataToFolder(fs, dstHdfsDir, localLocation);
    }

    @Deprecated
    public static List<String> getHDFSSubFoldersName(ColoHelper prismHelper,
                                                     String baseDir) throws IOException {
        setSystemPropertyHDFS();

        List<String> returnList = new ArrayList<String>();

        logger.info("getHDFSSubFoldersName: " + baseDir);
        Configuration conf = new Configuration();
        conf.set("fs.default.name", "hdfs://" + prismHelper.getProcessHelper().getHadoopURL() + "");

        final FileSystem fs = FileSystem.get(conf);
        return getHDFSSubFoldersName(fs, baseDir);
    }

    public static List<String> getHDFSSubFoldersName(FileSystem fs,
                                                     String baseDir) throws IOException {
        setSystemPropertyHDFS();

        List<String> returnList = new ArrayList<String>();

        FileStatus[] stats = fs.listStatus(new Path(baseDir));


        for (FileStatus stat : stats) {
            if (stat.isDir())

                returnList.add(stat.getPath().getName());

        }


        return returnList;
    }

    public static boolean isFilePresentHDFS(ColoHelper prismHelper,
                                            String hdfsPath, String fileToCheckFor)
    throws IOException {

        logger.info("getting file from folder: " + hdfsPath);
        Configuration conf = new Configuration();
        conf.set("fs.default.name", "hdfs://" + prismHelper.getProcessHelper().getHadoopURL() + "");

        final FileSystem fs = FileSystem.get(conf);

        List<String> fileNames = getAllFileNamesFromHDFS(fs, hdfsPath);

        for (String filePath : fileNames) {

            if (filePath.contains(fileToCheckFor))
                return true;
        }

        return false;
    }

    private static List<String> getAllFileNamesFromHDFS(
            FileSystem fs, String hdfsPath) throws IOException {
        setSystemPropertyHDFS();

        List<String> returnList = new ArrayList<String>();

        logger.info("getting file from folder: " + hdfsPath);
        FileStatus[] stats = fs.listStatus(new Path(hdfsPath));

        for (FileStatus stat : stats) {
            String currentPath = stat.getPath().toUri().getPath(); // gives directory name
            if (!stat.isDir()) {
                returnList.add(currentPath);
            }


        }
        return returnList;
    }

    @Deprecated
    public static boolean isDirPresent(ColoHelper prismHelper, String path) throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.default.name", "hdfs://" + prismHelper.getProcessHelper().getHadoopURL() + "");
        final FileSystem fs = FileSystem.get(conf);
        return isDirPresent(fs, path);
    }

    public static boolean isDirPresent(FileSystem fs, String path) throws IOException {
        setSystemPropertyHDFS();

        boolean isPresent = fs.exists(new Path(path));
        if (isPresent)
            System.out.println("dir exists");
        else
            System.out.println("dir does not exists");
        return isPresent;

    }

    @Deprecated
    public static void createDir(ColoHelper prismHelper, String path) throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.default.name", "hdfs://" + prismHelper.getProcessHelper().getHadoopURL() + "");
        final FileSystem fs = FileSystem.get(conf);
        createDir(path, fs);
    }

    public static void createDir(String path, FileSystem... fileSystems) throws IOException {

        for (FileSystem fs : fileSystems) {
            deleteDirIfExists(path, fs);
            System.out.println("creating hdfs dir: " + path + " on " + fs
              .getConf().get("fs.default.name"));
            fs.mkdirs(new Path(path));
        }
    }

    public static void deleteDirIfExists(String hdfsPath, FileSystem fs) throws IOException {
        Path path = new Path(hdfsPath);
        if (fs.exists(path)) {
          System.out.println("Deleting HDFS path: "+ path + " on "+fs.getConf().get("fs.default.name"));
            fs.delete(path, true);
        }
    }

    public static FileSystem getFileSystem(String fs) throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.default.name", "hdfs://" + fs);
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
        File[] files = new File(inputPath).listFiles();
        assert files != null;
        for (final File file : files) {
            if (!file.isDirectory()) {
                for (String remoteLocation : remoteLocations) {
                  logger.info("copying to: " + remoteLocation + " " +
                    "on:" +
                    " " + fs
                    .getConf().get("fs.default.name")+" file: "+file.getName());

                    if (!fs.exists(new Path(remoteLocation)))
                        fs.mkdirs(new Path(remoteLocation));

                    fs.copyFromLocalFile(new Path(file.getAbsolutePath()),
                            new Path(remoteLocation));
                }
            }
        }
    }
}
