package com.inmobi.qa.falcon;


import java.lang.reflect.Method;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.inmobi.qa.falcon.util.ELUtil;
import com.inmobi.qa.falcon.util.Util;


/**
 * 
 * @author samarth.gupta
 *
 */
public class ELTest {

	
	@BeforeMethod(alwaysRun=true)
	public void testName(Method method)
	{
		Util.print("test name: "+method.getName());
	}
	
	
	@Test(groups = {"singleCluster"},dataProvider="EL-DP")
	public void ExpressionLanguageTest(String startInstance,String endInstance) throws Exception{ELUtil.testWith(startInstance,endInstance,true);}

	@DataProvider(name="EL-DP")
	public Object[][] getELData(Method m) throws Exception
	{
		return new Object[][] {

			//	{"now(-3,0)","now(4,20)"},
			//	{"today(-2*4-1,0)","now(4,20)"},
			//	{"yesterday(22,0)","now(4,20)"},
			//	{"currentMonth(0,22,0)","now(4,20)"},    
			//	{"lastMonth(30,22,0)","now(4,20)"},
			//	{"currentYear(0,0,22,0)","currentYear(1,1,22,0)"},
			//	{"currentMonth(0,22,0)","currentMonth(1,22,20)"},
			//	{"lastMonth(30,22,0)","lastMonth(60,2,40)"},
				{"lastYear(12,0,22,0)","lastYear(13,1,22,0)"}
		};
	}

}
