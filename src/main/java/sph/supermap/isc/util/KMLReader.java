package sph.supermap.isc.util;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.annotations.XStreamAlias;

public class KMLReader {
public KML kml;
	
	public KMLReader(String path) {		
		InputStream is = null;
		try {
			is = new FileInputStream(path);
			XStream stream = new XStream();
			XStream.setupDefaultSecurity(stream);
			stream.allowTypesByRegExp(new String[] { ".*" });
			stream.ignoreUnknownElements();
			stream.processAnnotations(KMLReader.class);
			kml = (KML)stream.fromXML(is);
		} catch (Exception e) {
			e.printStackTrace();
		}finally {
			try {
				is.close();
			} catch (IOException e) {}
		}
	}
	
	@XStreamAlias("kml")
	public class KML{
		public Folder Folder;
	}
	@XStreamAlias("Folder")
	public class Folder{
		public  String name;
		public GroundOverlay GroundOverlay;
		public ScreenOverlay ScreenOverlay;
	}
	
	public class GroundOverlay{
		public String name;
		public Icon Icon;
		public LatLonBox LatLonBox;
	}
	
	public class ScreenOverlay{
		public String name;
		public Icon Icon;
	}
	
	public class Icon{
		public String href;

		public String getHref() {
			return href;
		}
	}
	
	public class LatLonBox{
		public double north;
		public double east;
		public double south;
		public double west;
	}
}
