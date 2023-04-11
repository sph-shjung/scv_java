package sph.supermap.isc.util;

import java.util.Date;

public class PublishItem {
	private String mapServiceURL;
	private String tempLayerURL;
	private String workspacePath;
	private String type;
	private String disasterType;
	private Date publish_date;
	private String file_path;
	private String data_name;
	private String datasource_path;
	private String rgb_band_idx;
	private String rgb_band_count;
	private StringBuilder message; 
	private double left;
	private double right;
	private double top;
	private double bottom;
	private boolean status;
	//
	//
	private String data_type;
	
	public PublishItem() {}
	
	public PublishItem(String datasource_path, String rgb_band_count, String rgb_band_idx, double left, double right, double top, double bottom, Date publish_date, boolean status) {
		this.datasource_path = datasource_path;
		this.rgb_band_idx = rgb_band_idx;
		this.left = left;
		this.right = right;
		this.bottom = bottom;
		this.top = top;
		//퍼블리시날짜
		this.publish_date = publish_date;
		this.message = null;
		
		this.status = status;
	}
	
	public PublishItem(String file_path, String data_name, String datasource_path, String rgb_band_idx, String data_type) {
		this.file_path = file_path;
		this.data_name = data_name;
		this.datasource_path = datasource_path;
		this.rgb_band_idx = rgb_band_idx;
		this.data_type =  data_type;
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

	public String getFile_path() {
		return file_path;
	}

	public void setFile_path(String file_path) {
		this.file_path = file_path;
	}

	public String getData_name() {
		return data_name;
	}

	public void setData_name(String data_name) {
		this.data_name = data_name;
	}
	
	public String getDatasource_path() {
		return datasource_path;
	}

	public void setDatasource_path(String datasource_path) {
		this.datasource_path = datasource_path;
	}

	public String getRgb_band_idx() {
		return rgb_band_idx;
	}

	public void setRgb_band_idx(String rgb_band_idx) {
		this.rgb_band_idx = rgb_band_idx;
	}

	public String getData_type() {
		return data_type;
	}

	public void setData_type(String data_type) {
		this.data_type = data_type;
	}

	public String getRgb_band_count() {
		return rgb_band_count;
	}

	public void setRgb_band_count(String rgb_band_count) {
		this.rgb_band_count = rgb_band_count;
	}

	public StringBuilder getMessage() {
		return message;
	}

	public void setMessage(StringBuilder message) {
		this.message = message;
	}

	public double getLeft() {
		return left;
	}

	public void setLeft(double left) {
		this.left = left;
	}

	public double getRight() {
		return right;
	}

	public void setRight(double right) {
		this.right = right;
	}

	public double getTop() {
		return top;
	}

	public void setTop(double top) {
		this.top = top;
	}

	public double getBottom() {
		return bottom;
	}

	public void setBottom(double bottom) {
		this.bottom = bottom;
	}
	
	public Date getPublish_date() {
		return publish_date;
	}

	public void setPublish_date(Date publish_date) {
		this.publish_date = publish_date;
	}

	public boolean isStatus() {
		return status;
	}

	public void setStatus(boolean status) {
		this.status = status;
	}
}






//private boolean kmzPublishMapService(String tFilePath, String mapName) {
//	try {
//		Datasource source = agent.OpenTifDatasource(tFilePath, null);
//		
//		DatasetImage dataset = (DatasetImage)source.getDatasets().get(0);
//		agent.addLayerFromDatasetImageKMZ(mapName, dataset, null);
//		
//		/*
//		String mapserviceURL = null;
//		String token = service.getToken(config.getIServer().getUser(), config.getIServer().getPassword());
//		System.out.println("TOKEN:" + token);
//		
//		//2번째 파라미터 테스트코드... 
//		String mapServiceJSON = service.createSerivce(token, workspacePath + "/TEST.smwu");
//		
//		if(!mapServiceJSON.equals("")) {
//			mapServiceJSON = mapServiceJSON.substring(1, mapServiceJSON.length()-1);
//			JSONObject item = new JSONObject(mapServiceJSON);
//			mapserviceURL = item.getString("serviceAddress") + "/maps/" + mapName;
//		}*/
//		//String tempLayerStr = service.createTemplayer(mapserviceURL);
//		return true;
//	}catch (Exception e) {
//		return false;
//		// TODO: handle exception
//	}finally {
//		agent.ReleaseInstance();
//	}
//}