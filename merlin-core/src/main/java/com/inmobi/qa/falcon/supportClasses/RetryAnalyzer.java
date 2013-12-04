/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.inmobi.qa.falcon.supportClasses;

import org.testng.IRetryAnalyzer;
import org.testng.ITestResult;
import org.testng.log4testng.Logger;

/**
 *
 * @author rishu.mehrotra
 */
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

public RetryAnalyzer()
{
    //testbaseLog.trace( " ModeledRetryAnalyzer constructor " + this.getClass().getName() );
    //log = Logger.getLogger("transcript.test");
    //log=Logger.getLogger(this.getClass().ge);
}

@Override 
public boolean retry(ITestResult result) { 
//    testbaseLog.trace("running retry logic for  '" 
//            + result.getName() 
//            + "' on class " + this.getClass().getName() );
        if(count < maxCount) {  
            System.out.println(">>>>> Error in " + result.getName() + "with status "+ (result.getStatus()) + " Retrying " + count+1 + ". time\n");
            count += 1;
            return true; 
//                count++;                                    
//                return true; 
        } 
        return false; 
}

    
}
