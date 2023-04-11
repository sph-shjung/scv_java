package sph.supermap.isc.service;
//
//import com.google.gson.JsonElement;
//import com.google.gson.JsonParser;

import sph.supermap.isc.rest.Sender;

public class SphService {
	private String workspacePath = null;
	private Sender sender = Sender.getInstance();
	
	
	public SphService() {
		
	}
	
	//generate token
	public String getToken(String userName, String password) {
		String token = null;
		try {
			token = sender.send("/services/security/tokens.json","POST","{\"userName\":\""+userName+"\", \"password\":\""+password+"\", \"clientType\":\"NONE\", \"expiration\":60}");
			//Log 남길것.. Token 발행시간
		}catch (Exception e) {
			// TODO: handle exception
		}
		return token;
	}
	
	//create service
	public String createSerivce(String token, String workspacePath) {
		String result = null;
		String message = "{\"workspaceConnectionInfo\":\""+workspacePath+"\", \"servicesTypes\":[\"RESTMAP\"], \"isDataEditable\":false, \"mapEditable\":false, \"cacheDisabled\":true, \"mapDpi\":\"0\", \"isMultiInstance\":false, \"instanceCount\":0, \"dataProviderDelayCommitSetting\":null}";
		System.out.println("MESSAGE:" + message);
		
		try {
			result = sender.send("/manager/workspaces.json?token="+token, "POST", message);
			System.out.println("Create Service URL : " + result);
		}
		catch (Exception e) {
			// TODO: handle exception
			System.out.println("ERROE EXCEPTION : " + e.getMessage());
		}
		return result;
	}
	
	//create Templayer Service
	public String createTemplayer(String mapserviceURL) {
		String tempLayerInfo = null;
		try {
			System.out.println("REQUEST TEMPLAYER : " + mapserviceURL +"/tempLayersSet.json");
			
			tempLayerInfo = sender.send(mapserviceURL +"/tempLayersSet.json?", "POST", "");
			System.out.println("TEMPLAYER:" + tempLayerInfo);
		}catch (Exception e) {
			// TODO: handle exception
		}
		return tempLayerInfo;
	}
	
	//
	public String deleteService(String token, String workspacePath) {
		String result = null;
		String message = "{\"workspaceConnectionInfo\":\""+workspacePath+"\"}";
		
		try {
			result = sender.send("/manager/workspaces.json?token="+token, "PUT", message);
			System.out.println("DELETE :" + result);
		}
		catch (Exception e) {
			// TODO: handle exception
		}
		return result;
	}
	
	public String getProviderJson(String token, String providerName) {
		String result = "";
		try {
			result = sender.send("/manager/providers/"+providerName+".json?token="+token, "GET", "");
		}catch(Exception e) {
		}
		return result;
	}
	
	public String setProviderJson(String token, String providerName, String json) {
		String result = "";
		try {
			result = sender.send("/manager/providers/"+providerName+".json?token="+token, "PUT", json);
		}catch(Exception e) {
		}
		return result;
	}
	
	public String getServiceURL(String json) {
		
//		JsonParser jParser = new JsonParser();
//		JsonElement element = jParser.parse(json);
//		String serviceURL = element.getAsJsonArray().get(0).getAsJsonObject().get("serviceAddress").toString().replaceAll("\"", "");
//		System.out.println(serviceURL);
		
//		return serviceURL;
		return null;
	}
}
