package common.store;

import common.java.Config.nConfig;
import common.java.Reflect._reflect;
import common.java.apps.AppContext;
import common.java.apps.MicroServiceContext;
import common.java.file.UploadFile;
import common.java.nlogger.nlogger;
import common.java.string.StringHelper;
import org.json.simple.JSONObject;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class Store implements InterfaceStore {
    public int _storeName = storeType.DFS;
    private _reflect _store;            //存储器抽象对象

    public static final Store getInstance(){
        return new Store();
    }
    public static final Store getInstance(String configName){
        return new Store(configName);
    }

    public Store() {
        init(null);
    }

    public Store(String configName) {
        init(configName);
    }

    private void init(String inputConfigName) {
        String configName = null;
        if( inputConfigName == null ){
            if( MicroServiceContext.current().hasData() ){
                configName = MicroServiceContext.current().config().store();
            }
            else if(AppContext.current().hasData()){
                configName = AppContext.current().config().store();
            }
        }
        else {
            configName = inputConfigName;
        }
        if (configName == null || configName.equals("")) {
            nlogger.logInfo("存储库配置丢失");
        }
        _store = getStoreObject(configName);
    }

    private _reflect getStoreObject(String cN) {
        String dbName;
        JSONObject obj;
        String _configString = null;
        try {
            if (_configString == null) {
                _configString = nConfig.netConfig(cN);
            }
            if (_configString != null) {
                obj = JSONObject.toJSON(_configString);
                if (obj != null) {
                    dbName = obj.getString("storeName");
                    switch (dbName) {
                        case "FastDFS":
                            _store = (new _reflect(FastDFS.class)).newInstance(_configString);
                            _storeName = storeType.fastDFS;
                            break;
                        default://disk file system
                            _store = (new _reflect(File.class)).newInstance(_configString);
                            _storeName = storeType.DFS;
                            break;
                    }
                } else {
                    nlogger.logInfo("存储库配置信息格式错误 ：" + _configString);
                }
            } else {
                nlogger.logInfo("存储库配置信息[" + cN + "]为空:=>" + _configString);
            }
        } catch (Exception e) {
            nlogger.logInfo(e, cN + "初始化配置:" + _configString);
            _store = null;
        }
        return _store;
    }
    //返回资源ID
    @Override
    public String addResource(byte[] data, String extName) {
        return (String) _store._call(Thread.currentThread().getStackTrace()[1].getMethodName(), data, extName);
    }
    @Override
    public String addResource(byte[] data, String extName, JSONObject metas) {
        return (String) _store._call(Thread.currentThread().getStackTrace()[1].getMethodName(), data, extName, metas);
    }
    //返回资源ID
    @Override
    public String addResource(File file) {
        return (String) _store._call(Thread.currentThread().getStackTrace()[1].getMethodName(), file);
    }
    @Override
    public String addResource(UploadFile file) {
        return (String) _store._call(Thread.currentThread().getStackTrace()[1].getMethodName(), file);
    }
    @Override
    public String addResource(File file, JSONObject metas) {
        return (String) _store._call(Thread.currentThread().getStackTrace()[1].getMethodName(), file, metas);
    }

    //设置资源元数据
    @Override
    public boolean setMetas(String resourceID, JSONObject newMetas) {
        return (boolean) _store._call(Thread.currentThread().getStackTrace()[1].getMethodName(), resourceID, newMetas);
    }

    //删除资源
    @Override
    public boolean delResource(String resourceID) {
        return (boolean) _store._call(Thread.currentThread().getStackTrace()[1].getMethodName(), resourceID);
    }

    //通过资源ID获得资源数据
    @Override
    public byte[] getResource(String resourceID) {
        return (byte[]) _store._call(Thread.currentThread().getStackTrace()[1].getMethodName(), resourceID);
    }

    //获得一次性资源URL
    @Override
    public String getSafeResourceUrl(String resourceID) {
        return (String) _store._call(Thread.currentThread().getStackTrace()[1].getMethodName(), resourceID);
    }

    //下载网络文件到指定文件
    public boolean downloadResource(String resourceID, File file){
        if( StringHelper.invaildString(resourceID) ){
            return false;
        }
        try (FileOutputStream fop = new FileOutputStream(file)) {
            if (!file.exists()) {
                file.createNewFile();
            }
            byte[] contentInBytes = Store.getInstance().getResource(resourceID);
            fop.write(contentInBytes);
            fop.flush();
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    //下载网络文件到指定流
    public boolean downloadResource(String resourceID, OutputStream stream){
        if( StringHelper.invaildString(resourceID) ){
            return false;
        }
        byte[] contentInBytes = Store.getInstance().getResource(resourceID);
        final int chunk = 1024 * 1024 * 10;   // 10M
        final int maxBuffer = contentInBytes.length;
        int offset = 0;
        int len = chunk;
        try{
            while(offset < maxBuffer){
                stream.write(contentInBytes, offset, len);
                offset += chunk;
                if( offset > maxBuffer ){
                    len = chunk - (offset - maxBuffer);
                }
            }
        }
        catch (Exception e){
            nlogger.logInfo(e);
            return false;
        }
        return true;
    }

    public static class storeType {
        public final static int fastDFS = 1;
        public final static int DFS = 2;
    }
}
