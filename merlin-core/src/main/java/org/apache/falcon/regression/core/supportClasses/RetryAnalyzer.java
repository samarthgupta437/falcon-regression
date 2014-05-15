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

import org.testng.IRetryAnalyzer;
import org.testng.ITestResult;
import org.apache.log4j.Logger;

public class RetryAnalyzer implements IRetryAnalyzer {

    private int count = 0;
    /*this number is actually twice the number
    of retry attempts will allow due to the retry
    method being called twice for each retry*/
    private int maxCount = 6;
    protected static Logger logger = Logger.getLogger(RetryAnalyzer.class);

    public RetryAnalyzer() {
    }

    @Override
    public boolean retry(ITestResult result) {
        if (count < maxCount) {
            logger.info(
                ">>>>> Error in " + result.getName() + "with status " + (result.getStatus()) +
                    " Retrying " +
                    count + 1 + ". time\n");
            count += 1;
            return true;
        }
        return false;
    }


}
