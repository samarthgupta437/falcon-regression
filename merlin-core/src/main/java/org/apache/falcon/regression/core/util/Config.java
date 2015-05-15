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

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.Logger;
import org.testng.Assert;

import java.io.File;
import java.io.FileInputStream;

public class Config {
    private static final Logger logger = Logger.getLogger(Config.class);

    private static final String MERLIN_PROPERTIES = "Merlin.properties";
    public static Config INSTANCE ;
    static {
        try {
            INSTANCE = new Config(MERLIN_PROPERTIES);
        }
        catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }
    private PropertiesConfiguration confObj;
    public Config(String propFileName) throws Exception {
        try {
            logger.info("Going to read properties from: " + propFileName);
            logger.info("Looking from "+ propFileName + "on local disk");
            File propFile = new File(propFileName);
            if (propFile.exists()) {
                FileInputStream fis = new FileInputStream(propFile);
                confObj = new PropertiesConfiguration();
                confObj.load(fis);
                fis.close();
            }
            else
            {
                logger.info("Coudnt find properties on disk. trying to load properties from from classpath");
                confObj = new PropertiesConfiguration(Config.class.getResource("/" + propFileName));
            }
        } catch (ConfigurationException e) {
            Assert.fail("Could not read properties because of exception: " + e);
        }
    }

    public static void setConfig(String filePath) throws Exception{
        INSTANCE = new Config(filePath);
    }
    public static String getProperty(String key) {
        return INSTANCE.confObj.getString(key);
    }

    public static String[] getStringArray(String key) {
        return INSTANCE.confObj.getStringArray(key);
    }

    public static String getProperty(String key, String defaultValue) {
        return INSTANCE.confObj.getString(key, defaultValue);
    }

}
