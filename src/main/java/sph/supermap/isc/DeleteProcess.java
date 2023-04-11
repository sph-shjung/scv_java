package sph.supermap.isc;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;

import com.supermap.data.CursorType;
import com.supermap.data.DatasetVector;
import com.supermap.data.Datasource;
import com.supermap.data.Recordset;

import sph.supermap.isc.config.Config;
import sph.supermap.isc.config.Config.Settings;
import sph.supermap.isc.manage.SphDataManage;
import sph.supermap.isc.manage.SphManangeAgent;
import sph.supermap.isc.service.SphService;

public class DeleteProcess {
	private static DeleteProcess process = null;
	private static SphService service = new SphService();
	private static SphManangeAgent agent = SphManangeAgent.getInstance();

	private static Settings config = null;

	private static String token = "";
	private static SphDataManage manage = null;

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		process = new DeleteProcess();

		config = new Config().settings;

		String iServerUserName = "supermap";
		String iServerPassword = "supermap12!@";
		manage = new SphDataManage();
		token = service.getToken(iServerUserName, iServerPassword);

		HashMap<String, String> itemList = new HashMap<>();
		ArrayList<String> deleteList = new ArrayList<String>();

		Datasource postDS = agent.connectionPostgre(config.getPostgre().getId(), config.getPostgre().getPassword(),
				config.getPostgre().getServer() + ":" + config.getPostgre().getPort(), config.getPostgre().getName(),
				false);

		DatasetVector disasaterDV = (DatasetVector) postDS.getDatasets().get(config.getPostgre().getDisaster());
		Recordset disasterRS = disasaterDV.query("", CursorType.STATIC);

		Calendar cal = Calendar.getInstance();
		Date nowDate = new Date();
		
		ArrayList<String> list = new ArrayList<String>();
		list.add("DEFAULT");

		DatasetVector serviceDV = (DatasetVector) postDS.getDatasets().get(config.getPostgre().getServicetablename());
		try {
			while (!disasterRS.isEOF()) {
				String disasterType = disasterRS.getObject("disaster_type").toString();
				list.add(disasterType);
				disasterRS.moveNext();
			}
		} catch (Exception e) {
			// TODO: handle exception
		} finally {
			
			for (String disasterType : list) {
				Recordset rs = serviceDV.query("service_url like '%" + disasterType + "%' order by smid desc ", CursorType.STATIC);
				try {
					while (!rs.isEOF()) {
						if (!itemList.containsKey(disasterType))
						{
							itemList.put(disasterType, rs.getObject("WORKSPACE_PATH").toString());
						}
						else {
							String workspacePath = rs.getObject("WORKSPACE_PATH").toString();
							String date = workspacePath.replace("/data/workspaces/" + disasterType, "");
							int lastIdx = date.lastIndexOf(".");
							date = date.substring(0, lastIdx);
							
							String format = (lastIdx == 17) ? "yyyyMMddHHmmssSSS" : "yyyyMMddHHmmsss";
							SimpleDateFormat sdf = new SimpleDateFormat(format);
							
							cal.setTime(sdf.parse(new SimpleDateFormat("yyyyMMddHHmmssSSS").format(nowDate).toString()));
							
							Calendar cal2 = Calendar.getInstance();
							cal2.setTime(sdf.parse(date));
							
							long sec = (cal.getTimeInMillis() - cal2.getTimeInMillis()) / 1000;
							long min = (cal.getTimeInMillis() - cal2.getTimeInMillis()) / (60 * 1000);
							long hour = (cal.getTimeInMillis() - cal2.getTimeInMillis()) / (60 * 60 * 1000);
							
							if(min > 14) //1시간 이상
							{
								deleteList.add(rs.getObject("WORKSPACE_PATH").toString());
							}
								
						}
						rs.moveNext();
					}
				} catch (Exception e) {
					// TODO: handle exception
					System.out.println(e.getMessage());
				} finally {
					rs.close();
				}
			}
			
			disasterRS.close();
			postDS.close();

			for (String deleteUrl : deleteList) {
				File file = new File(deleteUrl);
				if(file.exists()) {
					System.out.println("DELETE URL PATH : " + deleteUrl);
					service.deleteService(token, deleteUrl);
					manage.removeFile(deleteUrl);
				}
			}

		}
	}
}
