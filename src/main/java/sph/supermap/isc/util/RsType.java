package sph.supermap.isc.util;

public enum RsType {
	FilePath("FILE_PATH"), DataType("DATA_TYPE"), DataName("DATA_NAME");
	
	private String status;
	private RsType(String val) {
		status = val;
	}
	
	public String getStatus() {
		return status;
	}

}
