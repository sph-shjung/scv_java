package sph.supermap.isc.util;

public enum ServiceStatus {
	Wait("W"), Service("S"), Deleted("D"), Fail("F");
	
	private String status;
	private ServiceStatus(String val) {
		status = val;
	}
	public String getStatus() {
		return status;
	}
}
