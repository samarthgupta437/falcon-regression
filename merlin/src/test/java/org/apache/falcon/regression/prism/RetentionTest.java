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

package org.apache.falcon.regression.prism;


import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.generated.feed.Feed;
import org.apache.falcon.regression.core.generated.feed.Location;
import org.apache.falcon.regression.core.generated.feed.LocationType;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.core.util.Util.URLS;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.xml.bind.JAXBContext;
import java.io.StringReader;
import java.io.StringWriter;
import java.lang.reflect.Method;
import java.util.Random;

//@Test(groups = "embedded")
public class RetentionTest extends BaseTestClass {

    ColoHelper cluster1;
    private Bundle bundle;

    public RetentionTest(){
        super();
        cluster1 = servers.get(0);
    }

    @BeforeMethod(alwaysRun = true)
    public void testName(Method method) {
        Util.print("test name: " + method.getName());
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws Exception {
        finalCheck(bundle);
    }

    @Test(groups = {"0.1", "0.2", "prism"}, dataProvider = "betterDP", priority = -1)
    public void testRetention(Bundle b, String period, String unit, boolean gaps, String dataType) throws Exception {
        bundle = new Bundle(b, cluster1);
        displayDetails(period, unit, gaps, dataType);

        System.setProperty("java.security.krb5.realm", "");
        System.setProperty("java.security.krb5.kdc", "");
        String feed = setFeedPathValue(Util.getInputFeedFromBundle(bundle), getFeedPathValue(dataType));
        feed = Util.insertRetentionValueInFeed(feed, unit + "(" + period + ")");
        bundle.getDataSets().remove(Util.getInputFeedFromBundle(bundle));
        bundle.getDataSets().add(feed);
        bundle.generateUniqueBundle();

        bundle.submitClusters(prism);

        if (Integer.parseInt(period) > 0) {
            Util.assertSucceeded(prism.getFeedHelper()
                    .submitEntity(URLS.SUBMIT_URL, Util.getInputFeedFromBundle(bundle)));

            replenishData(cluster1, dataType, gaps);

            Util.CommonDataRetentionWorkflow(cluster1, bundle, Integer.parseInt(period),unit);
        } else {
            Util.assertFailed(prism.getFeedHelper()
                    .submitEntity(URLS.SUBMIT_URL, Util.getInputFeedFromBundle(bundle)));
        }
    }

    private String setFeedPathValue(String feed, String pathValue) throws Exception {
        JAXBContext feedContext = JAXBContext.newInstance(Feed.class);
        Feed feedObject = (Feed) feedContext.createUnmarshaller().unmarshal(new StringReader(feed));

        //set the value
        for (Location location : feedObject.getLocations().getLocation()) {
            if (location.getType().equals(LocationType.DATA)) {
                location.setPath(pathValue);
            }
        }

        StringWriter feedWriter = new StringWriter();
        feedContext.createMarshaller().marshal(feedObject, feedWriter);
        return feedWriter.toString();
    }

    private void finalCheck(Bundle bundle) throws Exception {
        prism.getFeedHelper().delete(URLS.DELETE_URL, Util.getInputFeedFromBundle(bundle));
        Util.verifyFeedDeletion(Util.getInputFeedFromBundle(bundle), cluster1);
    }

    private void displayDetails(String period, String unit, boolean gaps, String dataType)
    throws Exception {
        System.out.println("***********************************************");
        System.out.println("executing for:");
        System.out.println(unit + "(" + period + ")");
        System.out.println("gaps=" + gaps);
        System.out.println("dataType=" + dataType);
        System.out.println("***********************************************");
    }

    private void replenishData(ColoHelper helper, String dataType, boolean gap) throws Exception {
        int skip = 0;

        if (gap) {
            Random r = new Random();
            skip = gaps[r.nextInt(gaps.length)];
        }

        if (dataType.equalsIgnoreCase("daily")) {
            Util.replenishData(helper, Util.convertDatesToFolders(Util.getDailyDatesOnEitherSide(36, skip), skip));
        } else if (dataType.equalsIgnoreCase("yearly")) {
            Util.replenishData(helper, Util.getYearlyDatesOnEitherSide(10, skip));
        } else if (dataType.equalsIgnoreCase("monthly")) {
            Util.replenishData(helper, Util.getMonthlyDatesOnEitherSide(30, skip));
        }
    }

    private String getFeedPathValue(String dataType) throws Exception {
        if (dataType.equalsIgnoreCase("monthly")) {
            return "/retention/testFolders/${YEAR}/${MONTH}";
        }
        if (dataType.equalsIgnoreCase("daily")) {
            return "/retention/testFolders/${YEAR}/${MONTH}/${DAY}/${HOUR}";
        }
        if (dataType.equalsIgnoreCase("yearly")) {
            return "/retention/testFolders/${YEAR}";
        }
        return null;
    }

    final static int[] gaps = new int[]{2, 4, 5, 1};

    @DataProvider(name = "betterDP")
    public Object[][] getTestData(Method m) throws Exception {
        Bundle[] bundles = Util.getBundleData("RetentionBundles/valid/bundle1");
        String[] units = new String[]{"hours", "days"};// "minutes","hours","days",
        String[] periods = new String[]{"0", "10080", "60", "8", "24"}; // a negative value like -4 should be covered in validation scenarios.
        boolean[] gaps = new boolean[]{false, true};
        String[] dataTypes = new String[]{"daily", "yearly", "monthly"};
        Object[][] testData = new Object[bundles.length * units.length * periods.length * gaps.length * dataTypes.length][5];

        int i = 0;

        for (Bundle bundle : bundles) {
            for (String unit : units) {
                for (String period : periods) {
                    for (boolean gap : gaps) {
                        for (String dataType : dataTypes) {
                            testData[i][0] = bundle;
                            testData[i][1] = period;
                            testData[i][2] = unit;
                            testData[i][3] = gap;
                            testData[i][4] = dataType;
                            i++;
                        }
                    }
                }
            }
        }

        return testData;
    }

}
