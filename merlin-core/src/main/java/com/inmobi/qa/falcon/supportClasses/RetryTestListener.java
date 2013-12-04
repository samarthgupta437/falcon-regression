/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.inmobi.qa.falcon.supportClasses;

import java.util.ArrayList;
import java.util.List;

import org.testng.IResultMap;
import org.testng.ITestContext;
import org.testng.ITestNGMethod;
import org.testng.ITestResult;
import org.testng.Reporter;
import org.testng.TestListenerAdapter;

/**
 *
 * @author rishu.mehrotra
 */
public class RetryTestListener extends TestListenerAdapter {
    
private int count = 0; 
private int maxCount = 3; 

@Override
public void onTestFailure(ITestResult result) {     
    //Logger log = Logger.getLogger("transcript.test");
    Reporter.setCurrentTestResult(result);

    if(result.getMethod().getRetryAnalyzer().retry(result)) {    
        count++;
        result.setStatus(ITestResult.SKIP);
        
        System.out.println("Error in " + result.getName() + " with status " 
                + result.getStatus()+ " Retrying " + count + " of 3 times");
        //log.warn("Error in " + result.getName() + " with status " 
               // + result.getStatus()+ " Retrying " + count + " of 3 times");
        //log.info("Setting test run attempt status to Skipped");
                System.out.println("Setting test run attempt status to Skipped");
    } 
    else
    {
        count = 0;
        //log.error("Retry limit exceeded for " + result.getName());
        System.out.println("Retry limit exceeded for "+result.getName());
    }       

    Reporter.setCurrentTestResult(null);
}

@Override
public void onTestSuccess(ITestResult result)
{
    count = 0;
}

private IResultMap removeIncorrectlyFailedTests(ITestContext test)
{     
  List<ITestNGMethod> failsToRemove = new ArrayList<ITestNGMethod>();
  IResultMap returnValue = test.getFailedTests();

  for(ITestResult result : test.getFailedTests().getAllResults())
  {
    long failedResultTime = result.getEndMillis();          

    for(ITestResult resultToCheck : test.getSkippedTests().getAllResults())
    {
        if(failedResultTime == resultToCheck.getEndMillis())
        {
            failsToRemove.add(resultToCheck.getMethod());
            break;
        }
    }

    for(ITestResult resultToCheck : test.getPassedTests().getAllResults())
    {
        if(failedResultTime == resultToCheck.getEndMillis())
        {
            failsToRemove.add(resultToCheck.getMethod());
            break;
        }
    }           
  }

  for(ITestNGMethod method : failsToRemove)
  {
      returnValue.removeResult(method);
  }  

  return returnValue;
}


    
}
