package sph.supermap.isc.execute;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import sph.supermap.isc.VectorProcess;
import sph.supermap.isc.config.Config;
import sph.supermap.isc.config.Config.Settings;
import sph.supermap.isc.util.SphUtil;

public class KMLtoSHPThread extends Thread{

	private VectorProcess process;
	
	private ArrayList<Thread> threadList;
	private String filePath;
	private Settings settings;
	
	public KMLtoSHPThread(VectorProcess process, String filePath, Settings settings) {
		this.process = process;
		this.filePath = filePath;
		this.settings = settings;
	}
	
	public KMLtoSHPThread(VectorProcess process, String filePath, Settings settings, ArrayList<Thread> threadList) {
		this.process = process;
		this.filePath = filePath;
		this.settings = settings;
		this.threadList = threadList;
	}
	
	public void run() {
		try {
			String transFilePath = processOgrBuilder(getExecuteGdalOgrCommand());
			//this.process.callbackKMLtoShp(transFilePath);
			this.stop();
		}catch (Exception e) {
			// TODO: handle exception
		}finally {
			threadList.remove(this);
		}
	}
	
	public String processOgrBuilder(List<String> cmd) {
		try {
			Process process = new ProcessBuilder(cmd).start();
	    	BufferedReader reader = new BufferedReader(
	    	        new InputStreamReader(process.getInputStream()));
	    	
	    	String line = null;
	    	StringBuffer sb = new StringBuffer();
	    	while ((line = reader.readLine()) != null) {
	    	    sb.append(line);
	    	}
	    	
	    	int rslt = process.waitFor();
	    	System.out.println("RSLT :" + (rslt == 0 ? "kTOs SUCCESS" : "kTOs FAILED"));
	    	this.start();
	    	return rslt == 0 ? cmd.get(cmd.size() - 2) : "";
		}catch (Exception e) {
			// TODO: handle exception
		}
		return "";
	}
	
	public List<String> getExecuteGdalOgrCommand(){
		List<String> arr = new ArrayList<String>();
		try {
			//gdal ogr2ogr.exe Path
			arr.add(this.settings.getGdalogrexe());
			arr.add("-f");
			arr.add("\"ESRI Shapefile\"");
			
			arr.add("-t_srs");
			arr.add("EPSG:3857");
			
			arr.add("-lco");
			arr.add("ENCODING=UTF-8");
			//arr.add("D:/download/" + process.callIndexNumber() + "kTos_" + SphUtil.getName() + ".shp");
			arr.add(this.filePath);
		}catch (Exception e) {
			// TODO: handle exception
		}
		return arr;
	}
}
