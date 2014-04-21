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

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.falcon.regression.core.supportClasses;

import org.apache.log4j.Logger;
import org.testng.IResultMap;
import org.testng.ITestContext;
import org.testng.ITestNGMethod;
import org.testng.ITestResult;
import org.testng.Reporter;
import org.testng.TestListenerAdapter;

import java.util.ArrayList;
import java.util.List;

public class RetryTestListener extends TestListenerAdapter {

    private Logger logger = Logger.getLogger(RetryTestListener.class);
    private int count = 0;
    private int maxCount = 3;

    @Override
    public void onTestFailure(ITestResult result) {
        //Logger log = Logger.getLogger("transcript.test");
        Reporter.setCurrentTestResult(result);

        if (result.getMethod().getRetryAnalyzer().retry(result)) {
            count++;
            result.setStatus(ITestResult.SKIP);

            logger.info("Error in " + result.getName() + " with status "
                    + result.getStatus() + " Retrying " + count + " of 3 times");
            //log.warn("Error in " + result.getName() + " with status "
            // + result.getStatus()+ " Retrying " + count + " of 3 times");
            //log.info("Setting test run attempt status to Skipped");
            logger.info("Setting test run attempt status to Skipped");
        } else {
            count = 0;
            //log.error("Retry limit exceeded for " + result.getName());
            logger.info("Retry limit exceeded for " + result.getName());
        }

        Reporter.setCurrentTestResult(null);
    }

    @Override
    public void onTestSuccess(ITestResult result) {
        count = 0;
    }

    private IResultMap removeIncorrectlyFailedTests(ITestContext test) {
        List<ITestNGMethod> failsToRemove = new ArrayList<ITestNGMethod>();
        IResultMap returnValue = test.getFailedTests();

        for (ITestResult result : test.getFailedTests().getAllResults()) {
            long failedResultTime = result.getEndMillis();

            for (ITestResult resultToCheck : test.getSkippedTests().getAllResults()) {
                if (failedResultTime == resultToCheck.getEndMillis()) {
                    failsToRemove.add(resultToCheck.getMethod());
                    break;
                }
            }

            for (ITestResult resultToCheck : test.getPassedTests().getAllResults()) {
                if (failedResultTime == resultToCheck.getEndMillis()) {
                    failsToRemove.add(resultToCheck.getMethod());
                    break;
                }
            }
        }

        for (ITestNGMethod method : failsToRemove) {
            returnValue.removeResult(method);
        }

        return returnValue;
    }


}
