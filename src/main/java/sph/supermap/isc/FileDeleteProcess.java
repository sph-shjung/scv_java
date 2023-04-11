package sph.supermap.isc;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.supermap.data.CursorType;
import com.supermap.data.DatasetVector;
import com.supermap.data.Datasource;
import com.supermap.data.Recordset;

import sph.supermap.isc.config.Config;
import sph.supermap.isc.config.Config.Settings;
import sph.supermap.isc.manage.SphDataManage;
import sph.supermap.isc.manage.SphManangeAgent;
import sph.supermap.isc.service.SphService;

public class FileDeleteProcess {
	private static FileDeleteProcess process = null;
	private static SphService service = new SphService();
	private static SphManangeAgent agent = SphManangeAgent.getInstance();

	private static Settings config = null;

	private static String token = "";
	private static String fileDir = "/data/workspaces";
	private static SphDataManage manage = null;

	private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("MM/dd/yyyy HH:mm:ss");

	public static void main(String[] args) {
		process = new FileDeleteProcess();
		config = new Config().settings;

		String iServerUserName = "supermap";
		String iServerPassword = "supermap12!@";
		manage = new SphDataManage();
		token = service.getToken(iServerUserName, iServerPassword);

		Datasource postDS = agent.connectionPostgre(config.getPostgre().getId(), config.getPostgre().getPassword(),
				config.getPostgre().getServer() + ":" + config.getPostgre().getPort(), config.getPostgre().getName(),
				false);

		DatasetVector serviceDV = (DatasetVector) postDS.getDatasets().get(config.getPostgre().getServicetablename());

		List<Path> fileList;
		try {
			Path path = Paths.get(fileDir);
			Stream<Path> walk = Files.walk(path);
			fileList = walk.filter(Files::isRegularFile).collect(Collectors.toList());
			Collections.reverse(fileList);

			HashMap<String, String> hash = new HashMap<String, String>();
			
			//HashMap<Long, String> hashItemList = new HashMap<Long, String>();
			Map<Long, String> hashItemList = new HashMap<Long, String>();

			for (Path filePath : fileList) {
				String fileName = filePath.getFileName().toString();
				String type = null;

				if (fileName.startsWith("DEFAULT") || fileName.startsWith("DYNAMIC") || fileName.startsWith("WEATHER"))
					type = fileName.substring(0, 7);
				else
					type = fileName.substring(0, 5);
				
				if(!hash.containsKey(type)) {
					hash.put(type, "");
				}
				
				BasicFileAttributes attr = Files.readAttributes(filePath, BasicFileAttributes.class);
				FileTime creationTime = attr.creationTime();

				LocalDateTime ldt = creationTime.toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime();
				String date = ldt.format(DATE_FORMATTER);

				Date newDate = new Date(date);
				Calendar fileCal = Calendar.getInstance();
				fileCal.setTime(newDate);

				Calendar nowCal = Calendar.getInstance();
				nowCal.setTime(new Date());
				long sec = (nowCal.getTimeInMillis() - fileCal.getTimeInMillis()) / (1000);
				long min = (nowCal.getTimeInMillis() - fileCal.getTimeInMillis()) / (60 * 1000);
				long hour = (nowCal.getTimeInMillis() - fileCal.getTimeInMillis()) / (60 * 60 * 1000);
				
				hashItemList.put(sec, fileName);
			}
			
			for (String key : hash.keySet()) {
				//Stream<Entry<Long, String>> obj =  hashItemList.entrySet().stream().filter(map -> map.getValue().toString().startsWith(key));
				Map<Long, String> typeMap =  hashItemList.entrySet().stream().filter(map -> map.getValue().toString().startsWith(key)).collect(Collectors.toMap(map -> map.getKey(), map -> map.getValue()));
				System.out.println(typeMap.size());
				if(typeMap.size() > 1)
				{
					long min = Collections.min(typeMap.keySet());
					for (Long secKey : typeMap.keySet()) {
						if(secKey != min) {
							System.out.println("DELETE FILE LIST : " + typeMap.get(secKey) +"\t\t" + key);
							service.deleteService(token, fileDir + "/" + typeMap.get(secKey));
							manage.removeFile(fileDir + "/" + typeMap.get(secKey));
						}
					}
				}
				
			}
		} catch (Exception e) {
			// TODO: handle exception
			System.out.println(e.getMessage());
		} finally {
			serviceDV.close();
			postDS.close();
		}
	}
	

}





