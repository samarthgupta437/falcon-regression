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

package org.apache.falcon.regression.core.enumsAndConstants;

import org.apache.falcon.regression.core.util.Util;
import org.apache.hadoop.conf.Configuration;
import org.testng.Assert;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Properties;

public class MerlinConstants {
    private static Logger logger = Logger.getLogger(MerlinConstants.class);

    public static final String MERLIN_PROPERTIES = "Merlin.properties";
    public final static boolean IS_SECURE =
            "kerberos".equals(new Configuration().get("hadoop.security.authentication", "simple"));
    public static final String CURRENT_USER_NAME = System.getProperty("user.name");
    private static final String CURRENT_USER_KEYTAB_STR =
      "current_user_keytab";
    private static final String USER_2_NAME_STR = "user2_name";
    private static final String USER_2_KEYTAB_STR = "user2_keytab";
    public static final String USER2_NAME;
    private static HashMap<String, String> keyTabMap;

    /* initialize keyTabMap */
    static {
        Properties prop = Util.getPropertiesObj(MERLIN_PROPERTIES);
        final String current_user_keytab = prop.getProperty(CURRENT_USER_KEYTAB_STR);
        final String user2_name = prop.getProperty(USER_2_NAME_STR);
        final String user2_keytab = prop.getProperty(USER_2_KEYTAB_STR);
        logger.info("current_user_name: " + CURRENT_USER_NAME);
        logger.info("current_user_keytab: " + current_user_keytab);
        logger.info("user2_name: " + user2_name);
        logger.info("user2_keytab: " + user2_keytab);
        USER2_NAME = user2_name;
        keyTabMap = new HashMap<String, String>();
        keyTabMap.put(CURRENT_USER_NAME, current_user_keytab);
        keyTabMap.put(user2_name, user2_keytab);
    }

    public static String getKeytabForUser(String user) {
        Assert.assertTrue(keyTabMap.containsKey(user), "Unknown user: " + user);
        return keyTabMap.get(user);
    }
}
