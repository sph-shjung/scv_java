package sph.supermap.isc.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.json.JSONArray;
import org.json.JSONObject;

public class SphUtil {
	
	public static String getDate() {
		Calendar cal = Calendar.getInstance();
		cal.setTime(new Date());
		//2022.07.19 날짜 +1일 추가
		cal.add(Calendar.DATE, 1);
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
		
		return df.format(cal.getTime());
	}
	
	public static String getDate(String date) {
		Calendar cal = Calendar.getInstance();
		cal.setTime(new Date());
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
		
		cal.add(Calendar.DATE, -(Integer.parseInt(date)));
		
		return df.format(cal.getTime());
	}
	
	public static String getName() {
		//return new SimpleDateFormat("yyyyMMddhhmmsss").format(new Date()).toString();
		return new SimpleDateFormat("yyyyMMddHHmmssSSS").format(new Date()).toString();
	}
	
	public static HashMap<String, String> GetWorkSpaceInfo(String info){
		HashMap<String, String> rtnHash = new HashMap<String, String>();
		String[] items = info.replaceAll(" ", "").split("--");
		
		for(String item : items) {
			if(!item.contentEquals("")) {
				String[] keyValuePair = item.split("=");
				addKeyValuePair(rtnHash, keyValuePair[0], keyValuePair[1]);
			}
		}
		return rtnHash;
	}
	
	public static void addKeyValuePair(HashMap<String, String> map, String divisionCD, String pValue) {
		
		String value = pValue;
		
		if(value.equals("")) {
			if(divisionCD.equals("dbtype")) {
				value = "udbx";
			}
		}
		
		map.put(divisionCD, value);
	}
	
	public static String getReplacePath(String item) {
		return item.replaceAll("/", Matcher.quoteReplacement(File.separator));
	}
	
	public static String getFilePath(String drive, String type, HashMap<String, String> hash) {
		if(hash == null)
			return "";
		//-> RESULT : C:\\\\demoUDBX\\
		String path = drive + File.separator+ hash.get("path") + File.separator + getName() + File.separator + type;
		File fileDirectory = new File(path);
		if(!fileDirectory.exists())
			fileDirectory.mkdirs();
		
		path += File.separator + hash.get("name") + "." + type;
		System.out.println(path);
 		return path;
	}
	
	public static JSONArray getJsonArrayList(String jsonStr) {
		JSONArray array = new JSONArray();
		try {
			JSONObject object = new JSONObject(jsonStr);
			array = object.getJSONArray("itemList");
			return array;
			
		}catch(Exception ie) {
			return null;
		}
	}	
	
	public static int[] parseBandIndexes(String bandIndexes) {
		String[] strIndexes = bandIndexes.split(",");
		int[] indexes = new int[strIndexes.length];
		for (int i = 0; i < indexes.length; i++) {
			try {
				indexes[i] = Integer.parseInt(strIndexes[i]);
			} catch (Exception e) {
			}
		}
		return indexes;
	}
	
	//###########################################################
	//		ZIP File Method
	//###########################################################
	public static String unzip(String path) {
		//	F:/test/yyyyMMdd/folderName/folderName.XXXX
		String destPath = path.substring(0, path.lastIndexOf("."));
		File deCompressPath = new File(destPath);
		
		if(!deCompressPath.exists()) {
			deCompressPath.mkdir();
		}
		
		FileInputStream fis;
		byte[] buffer = new byte[1024];
		try {
			fis = new FileInputStream(path);
			ZipInputStream zis = new ZipInputStream(fis);
			
			ZipEntry ze = zis.getNextEntry();
			while (ze != null) {
				String fileName = deCompressPath + File.separator + ze.getName();
				
				if(ze.isDirectory())
					new File(fileName).mkdir();
				else {
					File newFile = new File(fileName);
					FileOutputStream fos = new FileOutputStream(newFile);
					int len;
					while((len = zis.read(buffer)) > 0) {
						fos.write(buffer, 0, len);
					}
					fos.close();
				}
				zis.closeEntry();
				ze = zis.getNextEntry();
			}
			zis.closeEntry();
			zis.close();
			fis.close();
		}catch (IOException e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		
		return destPath;
	}
	
	public static String getFileName(String path, String suffix) {
		String fileName= "";
		File file = new File(path);
		for (File shpFile: file.listFiles()) {
			fileName = shpFile.getAbsolutePath();
			if(fileName.toUpperCase().contains("." + suffix.toUpperCase())) {
				break;
			}
			else {
				fileName = null;
			}
		}
		return fileName;
	}
}
