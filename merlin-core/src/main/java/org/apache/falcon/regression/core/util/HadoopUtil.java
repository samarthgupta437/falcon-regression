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


import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;


public class HadoopUtil {

    static Logger logger = Logger.getLogger(Util.class);

    /*public static File getFileFromHDFSFolder(PrismHelper prismHelper,
                                             String hdfsLocation, String localLocation)
    throws Exception {

        System.setProperty("java.security.krb5.realm", "");
        System.setProperty("java.security.krb5.kdc", "");

        //	logger.info("getting file: "+ hdfsLocation);
        Configuration conf = new Configuration();
        conf.set("fs.default.name", "hdfs://" + prismHelper.getProcessHelper().getHadoopURL() + "");

        final FileSystem fs = FileSystem.get(conf);

        UserGroupInformation user = UserGroupInformation.createRemoteUser("hdfs");

        Path srcPath = Util.stringToPath(hdfsLocation);
        Path destPath = Util.stringToPath(localLocation + srcPath.getName());

        File f = new File(destPath.toString());
        if (f.exists())
            f.delete();

        fs.copyToLocalFile(srcPath, destPath);

        return new File(destPath.toString());

    }*/

    /*public static File getFileFromHDFSFolder(ColoHelper coloHelper,
                                             String hdfsLocation, String localLocation)
    throws Exception {

        System.setProperty("java.security.krb5.realm", "");
        System.setProperty("java.security.krb5.kdc", "");

        //	logger.info("getting file: "+ hdfsLocation);
        Configuration conf = new Configuration();
        conf.set("fs.default.name", "hdfs://" + coloHelper.getProcessHelper().getHadoopURL() + "");

        final FileSystem fs = FileSystem.get(conf);

        UserGroupInformation user = UserGroupInformation.createRemoteUser("hdfs");

        Path srcPath = Util.stringToPath(hdfsLocation);
        Path destPath = Util.stringToPath(localLocation + srcPath.getName());

        File f = new File(destPath.toString());
        if (f.exists())
            f.delete();

        fs.copyToLocalFile(srcPath, destPath);

        return new File(destPath.toString());

    }*/


    /*public static File getFileFromHDFSFolder(PrismHelper prismHelper,
                                             String hdfsLocation) throws Exception {
        System.setProperty("java.security.krb5.realm", "");
        System.setProperty("java.security.krb5.kdc", "");
        return getFileFromHDFSFolder(prismHelper, hdfsLocation, "dataFromHDFS/");
    }*/


    /*public static ArrayList<File> getAllFilesFromHDFSFolder(
            PrismHelper prismHelper, String hdfsLocation, String localBaseFolder) throws Exception {

        setSystemPropertyHDFS();

        ArrayList<File> returnList = new ArrayList<File>();

        logger.info("getting file from folder: " + hdfsLocation);
        Configuration conf = new Configuration();
        conf.set("fs.default.name", "hdfs://" + prismHelper.getProcessHelper().getHadoopURL() + "");

        final FileSystem fs = FileSystem.get(conf);

        FileStatus[] stats = fs.listStatus(Util.stringToPath(hdfsLocation));


        for (FileStatus stat : stats) {
            String currentPath = stat.getPath().toUri().getPath(); // gives directory name
            if (!stat.isDir()) {
                returnList.add(getFileFromHDFSFolder(prismHelper, currentPath, localBaseFolder));
            }


        }


        return returnList;

    }*/


    public static void setSystemPropertyHDFS() {
        System.setProperty("java.security.krb5.realm", "");
        System.setProperty("java.security.krb5.kdc", "");

    }


    /*public static ArrayList<String> getHDFSSubFolders(ColoHelper prismHelper,
                                                      String baseDir) throws IOException {
        setSystemPropertyHDFS();

        ArrayList<String> returnList = new ArrayList<String>();

        logger.info("getting folder list from: " + baseDir);
        Configuration conf = new Configuration();
        conf.set("fs.default.name", "hdfs://" + prismHelper.getProcessHelper().getHadoopURL() + "");

        final FileSystem fs = FileSystem.get(conf);

        FileStatus[] stats = fs.listStatus(Util.stringToPath(baseDir));


        for (FileStatus stat : stats) {
            if (stat.isDir())
                returnList.add(stat.getPath().toUri().getPath());

        }


        return returnList;


    }*/


    public static Configuration getHadoopConfiguration(ColoHelper prismHelper) throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.default.name", "hdfs://" + prismHelper.getProcessHelper().getHadoopURL() + "");
        return conf;
    }


    /*public static ArrayList<Path> getAllFilesHDFSLocation(
            ColoHelper prismHelper, String hdfsLocation) throws Exception {
        setSystemPropertyHDFS();

        ArrayList<Path> returnList = new ArrayList<Path>();

        //logger.info("getting file from folder: "+ hdfsLocation);
        Configuration conf = new Configuration();
        conf.set("fs.default.name", "hdfs://" + prismHelper.getProcessHelper().getHadoopURL() + "");

        final FileSystem fs = FileSystem.get(conf);

        FileStatus[] stats = fs.listStatus(Util.stringToPath(hdfsLocation));


        for (FileStatus stat : stats) {
            if (!stat.isDir()) {
                returnList.add(stat.getPath());
            }

        }

        return returnList;

    }*/

    public static ArrayList<Path> getAllFilesRecursivelyHDFS(
            ColoHelper colcoHelper, Path location) throws Exception {

        setSystemPropertyHDFS();
        ArrayList<Path> returnList = new ArrayList<Path>();

        Configuration conf = HadoopUtil.getHadoopConfiguration(colcoHelper);

        final FileSystem fs = FileSystem.get(conf);

        if (location.toString().contains("*"))
            location = Util.stringToPath(
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

    public static ArrayList<Path> getAllFilesRecursivelyHDFS(
            ColoHelper coloHelper, Path location, String... ignoreFolders) throws Exception {

        setSystemPropertyHDFS();
        ArrayList<Path> returnList = new ArrayList<Path>();

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


    public static ArrayList<Path> getAllFilesRecursivelyHDFS(
            Configuration conf, Path location, String... ignoreFolders) throws Exception {

        setSystemPropertyHDFS();
        ArrayList<Path> returnList = new ArrayList<Path>();

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

    public static void deleteFile(ColoHelper coloHelper, Path fileHDFSLocaltion) throws Exception {
        setSystemPropertyHDFS();
        Configuration conf = HadoopUtil.getHadoopConfiguration(coloHelper);

        final FileSystem fs = FileSystem.get(conf);

        fs.delete(fileHDFSLocaltion, false);

    }

    /*public static long getFileLength(ColoHelper coloHelper, Path fileHDFSLocaltion)
    throws Exception {
        setSystemPropertyHDFS();
        Configuration conf = HadoopUtil.getHadoopConfiguration(coloHelper);

        final FileSystem fs = FileSystem.get(conf);

        if (fs.exists(fileHDFSLocaltion)) {
            return fs.getFileStatus(fileHDFSLocaltion).getLen();
        }
        return 0;

    }*/

    /*public static void copyDataToFolders(PrismHelper prismHelper,
                                         final String folderPrefix, final Path folder,
                                         final String... fileLocations) throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.default.name", "hdfs://"
                + prismHelper.getProcessHelper().getHadoopURL());

        final FileSystem fs = FileSystem.get(conf);

        UserGroupInformation user = UserGroupInformation
                .createRemoteUser("hdfs");


        for (final String file : fileLocations) {
            user.doAs(new PrivilegedExceptionAction<Boolean>() {

                @Override
                public Boolean run() throws Exception {
                    //	logger.info("copying  "+file+" to "+folderPrefix+folder);
                    fs.copyFromLocalFile(new Path(file), new Path(
                            folderPrefix + folder));
                    return true;

                }
            });
        }


    }*/

    public static void copyDataToFolder(ColoHelper coloHelper, final Path folder,
                                        final String fileLocation)
    throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.default.name", "hdfs://"
                + coloHelper.getProcessHelper().getHadoopURL());

        final FileSystem fs = FileSystem.get(conf);

        UserGroupInformation user = UserGroupInformation
                .createRemoteUser("hdfs");


        user.doAs(new PrivilegedExceptionAction<Boolean>() {

            @Override
            public Boolean run() throws Exception {
                //	logger.info("copying  "+file+" to "+folderPrefix+folder);
                fs.copyFromLocalFile(new Path(fileLocation), folder);
                return true;

            }
        });


    }


    public static ArrayList<String> getHDFSSubFoldersName(ColoHelper prismHelper,
                                                          String baseDir) throws IOException {
        setSystemPropertyHDFS();

        ArrayList<String> returnList = new ArrayList<String>();

        logger.info("getHDFSSubFoldersName: " + baseDir);
        Configuration conf = new Configuration();
        conf.set("fs.default.name", "hdfs://" + prismHelper.getProcessHelper().getHadoopURL() + "");

        final FileSystem fs = FileSystem.get(conf);

        FileStatus[] stats = fs.listStatus(Util.stringToPath(baseDir));


        for (FileStatus stat : stats) {
            if (stat.isDir())

                returnList.add(stat.getPath().getName());

        }


        return returnList;


    }

    public static boolean isFilePresentHDFS(ColoHelper prismHelper,
                                            String hdfsPath, String fileToCheckFor)
    throws Exception {

        ArrayList<String> fileNames = getAllFileNamesFromHDFS(prismHelper, hdfsPath);

        for (String filePath : fileNames) {

            if (filePath.contains(fileToCheckFor))
                return true;
        }

        return false;
    }

    private static ArrayList<String> getAllFileNamesFromHDFS(
            ColoHelper prismHelper, String hdfsPath) throws Exception {
        setSystemPropertyHDFS();

        ArrayList<String> returnList = new ArrayList<String>();

        logger.info("getting file from folder: " + hdfsPath);
        Configuration conf = new Configuration();
        conf.set("fs.default.name", "hdfs://" + prismHelper.getProcessHelper().getHadoopURL() + "");

        final FileSystem fs = FileSystem.get(conf);

        FileStatus[] stats = fs.listStatus(Util.stringToPath(hdfsPath));


        for (FileStatus stat : stats) {
            String currentPath = stat.getPath().toUri().getPath(); // gives directory name
            if (!stat.isDir()) {
                returnList.add(currentPath);
            }


        }
        return returnList;

    }

    public static boolean isDirPresent(ColoHelper prismHelper, String path) throws Exception {
        System.setProperty("java.security.krb5.realm", "");
        System.setProperty("java.security.krb5.kdc", "");

        Configuration conf = new Configuration();
        conf.set("fs.default.name", "hdfs://" + prismHelper.getProcessHelper().getHadoopURL() + "");
        final FileSystem fs = FileSystem.get(conf);
        boolean isPresent = fs.exists(new Path(path));
        if (isPresent)
            System.out.println("dir exists");
        else
            System.out.println("dir does not exists");
        return isPresent;

    }

    public static void createDir(ColoHelper prismHelper, String path) throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.default.name", "hdfs://" + prismHelper.getProcessHelper().getHadoopURL() + "");
        final FileSystem fs = FileSystem.get(conf);
        System.out.println("creating hdfs dir: " + path);
        fs.mkdirs(new Path(path));

    }

    /*public static ArrayList<String> getAllRecords(Path path) throws Exception {
        ArrayList<Path> rrPaths =
                HadoopUtil.getAllFilesRecursivelyHDFS(new Configuration(), path, "shouldNOtMatch");
        FileSystem fs = FileSystem.get(new Configuration());
        ArrayList<String> records = new ArrayList<String>();
        BufferedReader br;
        for (Path rrPath : rrPaths) {

            if (!fs.exists(rrPath)) {
                System.out.println("File does not exists");
            }

            FSDataInputStream in = fs.open(rrPath);
            System.out.println("getting all records for path: " + rrPath);
            if (rrPath.toString().endsWith(".gz"))
                br = new BufferedReader(new InputStreamReader(new GZIPInputStream(in)));
            else
                br = new BufferedReader(new InputStreamReader(in));

            String line;

            while ((line = br.readLine()) != null) {
                records.add(line);
            }

            br.close();
        }

        return records;
    }*/

    /*public static ArrayList<String> getAllRecords(ColoHelper prismHelper, Path path,
                                                  String... notToMatch)
    throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.default.name", "hdfs://" + prismHelper.getProcessHelper().getHadoopURL() + "");
        ArrayList<Path> rrPaths =
                HadoopUtil.getAllFilesRecursivelyHDFS(prismHelper, path, notToMatch);
        FileSystem fs = FileSystem.get(conf);
        ArrayList<String> records = new ArrayList<String>();
        BufferedReader br;
        for (int allPaths = 0; allPaths < rrPaths.size(); allPaths++) {

            if (!fs.exists(rrPaths.get(allPaths))) {
                System.out.println("File does not exists");
            }

            FSDataInputStream in = fs.open(rrPaths.get(allPaths));
            System.out.println("getting all records for path: " + rrPaths.get(allPaths));
            if (rrPaths.get(allPaths).toString().endsWith(".gz"))
                br = new BufferedReader(new InputStreamReader(new GZIPInputStream(in)));
            else
                br = new BufferedReader(new InputStreamReader(in));

            String line;

            while ((line = br.readLine()) != null) {
                records.add(line);
            }

            br.close();
        }

        return records;
    }*/


}
