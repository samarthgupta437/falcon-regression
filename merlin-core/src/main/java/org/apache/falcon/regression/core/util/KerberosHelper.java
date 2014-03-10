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

import org.testng.Assert;
import org.testng.log4testng.Logger;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Properties;

public class KerberosHelper {

    public static final String CURRENT_USER_KEYTAB = "~/.ssh/";
    private static Logger logger = Logger.getLogger(KerberosHelper.class);
    public static final String KERBEROS_PROPERTIES = "Kerberos.properties";

    static String user2_name = null;
    static String user2_keytab = null;
    final static boolean kerberosEnabled;
    private static String currentUser = null;

    static {
        Properties prop = Util.getPropertiesObj(KERBEROS_PROPERTIES);
        kerberosEnabled = Boolean.parseBoolean(prop.getProperty("kerberos_enabled", "false"));
        if(kerberosEnabled) {
            user2_name = prop.getProperty("user2_name");
            user2_keytab = prop.getProperty("user2_keytab");
            logger.info("user2_name: " + user2_name);
            logger.info("user2_keytab: " + user2_keytab);
        }
    }

    public static void switchUser(String user) {
        if(!kerberosEnabled) {
            return;
        }

        String keytab = CURRENT_USER_KEYTAB;
        if(user == null) {
            user = System.getProperty("user.name");
        } else {
            Assert.assertEquals(user, user2_name, "Unexpected user.");
            keytab = user2_keytab;
        }
        if(user.equals(currentUser)) {
            logger.info("kerberos switching is not required.");
            return;
        }
        logger.info(String.format("Switching kerberos keytab from %s to %s", currentUser, user));
        final String command = String.format("ls -al %s", keytab);
        final int exitVal = executeCommand(command);
        Assert.assertEquals(exitVal, 0, "Switching Kerberos credential did not succeed.");
        currentUser = user;
    }
    /* Example usage:
    String command = "ping -c 3 google.com";
    String output = executeCommand(command);
    System.out.println(output);
    */
    private static int executeCommand(String command) {
        StringBuilder output = new StringBuilder();
        StringBuilder error = new StringBuilder();

        Process p;
        try {
            p = Runtime.getRuntime().exec(command);
            p.waitFor();
            BufferedReader outputReader =
                    new BufferedReader(new InputStreamReader(p.getInputStream()));
            BufferedReader errorReader =
                    new BufferedReader(new InputStreamReader(p.getErrorStream()));
            String line = "";
            while ((line = outputReader.readLine()) != null) {
                output.append(line + "\n");
            }
            while ((line = errorReader.readLine()) != null) {
                error.append(line + "\n");
            }
            logger.info("output:" + output.toString());
            logger.info("error:" + error.toString());
            logger.info("exit status:" + p.exitValue());
            return p.exitValue();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return -1;
    }
}
