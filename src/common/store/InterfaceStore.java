package common.store;

import common.java.file.UploadFile;
import org.json.simple.JSONObject;

import java.io.File;

interface InterfaceStore {
    /***
     * 存储文件数据
     */
    String addResource(byte[] data, String extName);
    String addResource(byte[] data, String extName, JSONObject metas);
    /***
     * 存储文件
     */
    String addResource(File file);
    String addResource(UploadFile file);
    String addResource(File file, JSONObject metas);
    /**
     * 设置文件元信息
     */
    boolean setMetas(String resourceID, JSONObject newMetas);
    /**
     * 删除文件
     */
    boolean delResource(String resourceID);
    /**
     * 获得文件数据
     */
    byte[] getResource(String resourceID);
    /**
     * 获得文件一次性连接
     */
    String getSafeResourceUrl(String resourceID);
}
