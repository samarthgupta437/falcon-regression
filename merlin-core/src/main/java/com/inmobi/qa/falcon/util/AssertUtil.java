package com.inmobi.qa.falcon.util;

import java.util.ArrayList;

import org.apache.hadoop.fs.Path;
import org.testng.Assert;

import com.inmobi.qa.falcon.response.APIResult;
import com.inmobi.qa.falcon.response.ServiceResponse;
/**
 * 
 * @author samarth.gupta
 *
 */
public class AssertUtil {

	public static void failIfStringFoundInPath(
			ArrayList<Path> paths,String ...shouldNotBePresent) {

		for(int i = 0 ; i < paths.size() ; i++)
		{
			for(int j = 0; j < shouldNotBePresent.length; j++ )
				if(paths.get(i).toUri().toString().contains(shouldNotBePresent[j]))
					Assert.assertTrue(false,"String "+shouldNotBePresent[j]+" was not expected in path "+paths.get(i).toUri().toString());
		}
	}

	public static void checkForPathsSizes(ArrayList<Path> expected,
			ArrayList<Path> actual) {

		Assert.assertEquals(actual.size(),expected.size(),"array size of the 2 paths array list is not the same");
	}

	public static void assertSucceeded(ServiceResponse response) throws Exception{
		Assert.assertEquals(Util.parseResponse(response).getStatus(),
				APIResult.Status.SUCCEEDED);
		Assert.assertEquals(Util.parseResponse(response).getStatusCode(), 200);
		Assert.assertNotNull(Util.parseResponse(response).getMessage());
		
	}

	public static void assertFailed(ServiceResponse response, String message)throws Exception {
		if (response.message.equals("null"))
			Assert.assertTrue(false, "response message should not be null");

		Assert.assertEquals(Util.parseResponse(response).getStatus(),
				APIResult.Status.FAILED, message);
		Assert.assertEquals(Util.parseResponse(response).getStatusCode(), 400,
				message);
		Assert.assertNotNull(Util.parseResponse(response).getRequestId());		
	}

}
