package sph.supermap.isc.file;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class Compressor {
	/**
	 * @param path
	 * @return 
	 */
	public static String unzip(String path) {
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
            while(ze != null){
            	String fileName = deCompressPath + File.separator + ze.getName();
            	if(ze.isDirectory()) {
            		new File(fileName).mkdir();
            	}else {
            		
                    File newFile = new File(fileName);
                    FileOutputStream fos = new FileOutputStream(newFile);
                    int len;
                    while ((len = zis.read(buffer)) > 0) {
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
        } catch (IOException e) {
            e.printStackTrace();
        }
        
        File zipFile = new File(path);
        return destPath;
	}
	
	public static String getFileName(String path, String suffix) {
		String fileName = "";
		File file = new File(path);
		for(File shpFile : file.listFiles()) {
			fileName = shpFile.getAbsolutePath();
			if(fileName.toUpperCase().contains("."+suffix.toUpperCase())) {
				break;
			}else {
				fileName = null;
			}
		}
		return fileName;
	}
}
