package common.store;

import common.java.Config.nConfig;
import common.java.JGrapeSystem.SystemDefined;
import common.java.file.FileHelper;
import common.java.file.UploadFile;
import common.java.nlogger.nlogger;
import common.java.string.StringHelper;
import org.csource.common.NameValuePair;
import org.csource.fastdfs.*;
import org.json.simple.JSONObject;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/***
 * fastDFS配置解析
 * {
 *      "storeName":"FastDFS",
 *      "connect_timeout":2,
 *      "network_timeout":30,
 *      "charset":"UTF-8",
 *      "http.tracker_http_port":8080,
 *      "http.anti_steal_token":no,
 *      "http.secret_key":"FastDFS1234567890",
 *      "tracker_server":"10.0.11.243:22122,10.0.11.243:22122,10.0.11.243:22122"
 * }
 *
 * fileServer配置解析
 * {
 *      "nodeServer":"http://123.57.214.226:8080"
 * }
 */
public class FastDFS implements InterfaceStore {
    private static String fileHostServer;

    static {
        JSONObject fileNodeServer = JSONObject.toJSON(nConfig.netConfig(SystemDefined.commonConfigUnit.FileHost));
        fileHostServer = fileNodeServer.getString(SystemDefined.commonConfigUnit.FileNode);
    }

    private StorageClient1 client;


    private void initFastDFS(String configJson){
        if( !ClientGlobal.isInit() ){
            try {
                ClientGlobal.initByJSON(configJson);
                // ClientGlobal.g_anti_steal_token = true;
            } catch (Exception e) {
                nlogger.logInfo(e,"存储系统初始化失败! 配置信息:[" + configJson + "]");
            }
        }
    }

    public FastDFS(String configJson) {
        init(configJson);
    }

    private void init(String configJson){
        initFastDFS(configJson);
        TrackerClient tracker = new TrackerClient();
        try {
            TrackerServer trackerServer = tracker.getConnection();
            StorageServer storageServer = null;
            client = new StorageClient1(trackerServer, storageServer);
        } catch (Exception e) {
            nlogger.errorInfo("存储系统连接失败! 配置信息:[" + configJson + "]");
        }
    }

    private NameValuePair[] json2nvp(JSONObject json) {
        if (json == null) {
            return null;
        }
        List<NameValuePair> list = new ArrayList<>();
        for (Object _key : json.keySet()) {
            NameValuePair nvp = new NameValuePair(_key.toString(), json.get(_key).toString());
            list.add(nvp);
        }
        return list.toArray(new NameValuePair[list.size()]);
    }

    /***
     * 存储文件数据
     */
    public String addResource(byte[] data, String extName){
        return addResource(data, extName, null);
    }
    public String addResource(byte[] data, String extName,JSONObject metas) {
        if(StringHelper.invaildString(extName)){
            extName = "tmp";
        }
        String file_id = null;
        try {
            file_id = client.upload_file1(data, extName, json2nvp(metas));
        } catch (Exception e) {
            nlogger.logInfo(e, "添加文件失败");
        }
        return file_id;
    }

    /***
     * 存储文件
     */
    public String addResource(File file) {
        return addResource(file, null);
    }

    /**
     * 发送上传文件到fastDFS
     * */
    public String addResource(UploadFile file){
        String rs = null;
        File tempFile = FileHelper.newFileEx(FileHelper.getTempPath(), file.getFileInfo().getClientName());
        if( file.writeTo(tempFile) ){// 上传的文件落盘地址,文件落盘成功
            rs = addResource(tempFile);
            tempFile.delete();
        }
        return rs;
    }

    public String addResource(File file, JSONObject metas) {
        String path = file.getAbsolutePath();
        String[] fileName = path.split("\\.");
        String extName = fileName.length > 1 ? fileName[fileName.length - 1] : "";
        String file_id = null;
        try {
            file_id = client.upload_file1(path, extName, json2nvp(metas));
        } catch (Exception e) {
            nlogger.logInfo(e, "添加文件失败");
        }
        return file_id;
    }

    /**
     * 设置文件元信息
     */
    public boolean setMetas(String resourceID, JSONObject newMetas) {
        boolean rb = true;
        try {
            client.set_metadata1(resourceID, json2nvp(newMetas), ProtoCommon.STORAGE_SET_METADATA_FLAG_MERGE);
        } catch (Exception e) {
            nlogger.logInfo(e, "添加文件失败");
            rb = false;
        }
        return rb;
    }

    /**
     * 删除文件
     */
    public boolean delResource(String resourceID) {
        boolean rb = true;
        try {
            client.delete_file1(resourceID);
        } catch (Exception e) {
            nlogger.logInfo(e, "删除文件" + resourceID + "失败");
            rb = false;
        }
        return rb;
    }

    /**
     * 获得文件数据
     */
    public byte[] getResource(String resourceID) {
        byte[] rb = null;
        try {
            rb = client.download_file1(resourceID);
        } catch (Exception e) {
            nlogger.logInfo(e, "下载文件失败");
        }
        return rb;
    }

    /**
     * 获得文件一次性安全连接
     */
    public String getSafeResourceUrl(String resourceID) {
        String file_url = null;
        file_url += "/" + resourceID;
        int ts = (int) (System.currentTimeMillis() / 1000);
        try {
            file_url = fileHostServer;
            String token = ProtoCommon.getToken(resourceID, ts, ClientGlobal.g_secret_key);
            file_url += "?token=" + token + "&ts=" + ts;
        } catch (Exception e) {
            nlogger.logInfo(e, "获得文件访问授权失败");
            file_url = null;
        }
        return file_url;
    }
}
