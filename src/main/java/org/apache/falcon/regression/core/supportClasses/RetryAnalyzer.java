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
    // this number is actually twice the number
// of retry attempts will allow due to the retry
// method being called twice for each retry
    private int maxCount = 6;
    protected Logger log;
    private static Logger testbaseLog;

    static {
        //PropertyConfigurator.configure("test-config/log4j.properties");
        //testbaseLog = Logger.getLogger("testbase.testng");
        //testbaseLog=Logger.getLogger();

    }

    public RetryAnalyzer() {
        //testbaseLog.trace( " ModeledRetryAnalyzer constructor " + this.getClass().getName() );
        //log = Logger.getLogger("transcript.test");
        //log=Logger.getLogger(this.getClass().ge);
    }

    @Override
    public boolean retry(ITestResult result) {
//    testbaseLog.trace("running retry logic for  '" 
//            + result.getName() 
//            + "' on class " + this.getClass().getName() );
        if (count < maxCount) {
            System.out.println(
                    ">>>>> Error in " + result.getName() + "with status " + (result.getStatus()) +
                            " Retrying " +
                            count + 1 + ". time\n");
            count += 1;
            return true;
//                count++;                                    
//                return true; 
        }
        return false;
    }


}
