package sph.supermap.isc.util;

import java.util.Date;

public class ResultItem {
	private String statusCode;
	private String mapServiceURL;
	private String tempLayerURL;
	private String workspacePath;
	private String type;
	private String disasterType;
	private Date publish_date;
	
	public ResultItem() {
		this.statusCode = ServiceStatus.Fail.getStatus();
		this.disasterType = "";
		this.type = "default";
	}
	
	public Date getPublish_date() {
		return publish_date;
	}

	public void setPublish_date(Date publish_date) {
		this.publish_date = publish_date;
	}

	public void setResultItem(ServiceStatus service, String mapServiceURL, String tempLayerURL, String workspacePath, String type, String disasterType) {
		this.statusCode = service.getStatus();
		
		this.mapServiceURL = mapServiceURL;
		this.tempLayerURL = tempLayerURL;
		this.workspacePath = workspacePath;
		this.disasterType = disasterType;
		this.type = type;
	}

	public String getStatusCode() {
		return statusCode;
	}

	public void setStatusCode(ServiceStatus statusCode) {
		this.statusCode = statusCode.getStatus();
	}

	public String getMapServiceURL() {
		return mapServiceURL;
	}

	public void setMapServiceURL(String mapServiceURL) {
		this.mapServiceURL = mapServiceURL;
	}

	public String getTempLayerURL() {
		return tempLayerURL;
	}

	public void setTempLayerURL(String tempLayerURL) {
		this.tempLayerURL = tempLayerURL;
	}

	public String getWorkspacePath() {
		return workspacePath;
	}

	public void setWorkspacePath(String workspacePath) {
		this.workspacePath = workspacePath;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getDisasterType() {
		return disasterType;
	}

	public void setDisasterType(String disasterType) {
		this.disasterType = disasterType;
	}
	
	
}
