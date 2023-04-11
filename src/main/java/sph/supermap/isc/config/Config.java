package sph.supermap.isc.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.annotations.XStreamAlias;

public class Config {
public Settings settings = null;
	
	public Config() {
		if(settings == null) {
			InputStream is = this.getClass().getResourceAsStream("/config.xml");
			System.out.println(this.getClass().getResourceAsStream("/config.xml"));
			try {
				XStream stream = new XStream();
				XStream.setupDefaultSecurity(stream);
				stream.allowTypesByRegExp(new String[] { ".*" });
				stream.processAnnotations(Config.class);
				settings = (Settings)stream.fromXML(is);
			} catch (Exception e) {
				e.printStackTrace();
			}finally {
				try {
					is.close();
				} catch (IOException e) {}
			}
		}
	}
	
	@XStreamAlias("settings")
	public class Settings{
		private String gdalexe;
		private String gdalwarpexe;
		private String gdalogrexe;
		private PostgreSQL postgre;
		private Image image;
		private Vector vector;
		private Satellite satellite;
		private IServer iServer;
		private String dbPath;
		private String directory;
		private int maxThreadCount;

		
		public PostgreSQL getPostgre() {
			return postgre;
		}
		public String getGdalexe() {
			return gdalexe;
		}
		public String getGdalwarpexe() {
			return gdalwarpexe;
		}
		public String getGdalogrexe() {
			return gdalogrexe;
		}
		public Image getImage() {
			return image;
		}
		public Vector getVector() {
			return vector;
		}
		public Satellite getSatellite() {
			return satellite;
		}
		public IServer getIServer() {
			return iServer;
		}
		public String getDbPath() {
			return dbPath;
		}
		public String getDirectory() {
			return directory;
		}
		public int getMaxThreadCount() {
			return maxThreadCount;
		}
	}
	
	public class PostgreSQL{
		private String server;
		private String port;
		private String id;
		private String password;
		private String name;
		private String tablename;
		private String weathertablename;
		private String servicetablename;
		private String disaster;
		private String disastercount;
		private String disasterquery;
		//2023-04-06
		private String publishurl;
		
		public String getServer() {
			return server;
		}
		public String getPort() {
			return port;
		}
		public String getId() {
			return id;
		}
		public String getPassword() {
			return password;
		}
		public String getName() {
			return name;
		}
		public String getTablename() {
			return tablename;
		}
		public String getDisaster() {
			return disaster;
		}
		public String getDisastercount() {
			return disastercount;
		}
		public String getWeathertablename() {
			return weathertablename;
		}
		public String getServicetablename() {
			return servicetablename;
		}
		public String getDisasterquery() {
			return disasterquery;
		}
		public String getPublishUrl() {
			return publishurl;
		}
	}
	
	public class Image{
		private String iServerUrl;
		private String workspace;
		private String datasource;
		private String datasetName;
		public String getiServerUrl() {
			return iServerUrl;
		}
		public String getWorkspace() {
			return workspace;
		}
		public String getDatasource() {
			return datasource;
		}
		public String getDatasetName() {
			return datasetName;
		}
	}
	
	public class Vector{
		private String iServerUrl;
		private String workspace;
		private String datasource;
		private String datasetName;
		public String getiServerUrl() {
			return iServerUrl;
		}
		public String getWorkspace() {
			return workspace;
		}
		public String getDatasource() {
			return datasource;
		}
		public String getDatasetName() {
			return datasetName;
		}
	}
	
	public class Satellite{
		private String iServerUrl;
		private String workspace;
		private String datasource;
		private String serviceName;
		private String datasetName;
		private List<Period> deletePeriods;
		public String getiServerUrl() {
			return iServerUrl;
		}
		public String getWorkspace() {
			return workspace;
		}
		public String getDatasource() {
			return datasource;
		}
		public String getServiceName() {
			return serviceName;
		}
		public String getDatasetName() {
			return datasetName;
		}
		public List<Period> getDeletePeriods(){
			return deletePeriods;
		}
	}
	@XStreamAlias("period")
	public class Period{
		private String name;
		private int hour;
		public String getName() {
			return name;
		}
		public int getHour() {
			return hour;
		}
	}
	
	public class IServer{
		private String user;
		private String password;
		public String getUser() {
			return user;
		}
		public String getPassword() {
			return password;
		}
	}
}