package common.store;

import common.java.file.FileHelper;
import common.java.file.UploadFile;
import common.java.string.StringHelper;
import org.json.simple.JSONObject;

import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
/**
 * 文件储存方法
 * <p>
 * 按文件扩展名,文件入库的日期(YYYYMMDD),储存文件
 * <p>
 * 一次性资源在 rootPath + once 文件夹
 *
 *  * File配置解析
 *  * {
 *  *      "storeName":"File",
 *  *      "root":"c:/data/"
 *  * }
 */
public class File implements InterfaceStore{
    private final static int MAXNO = 1024;
    private String rootPath;

    public File(String configString) {
        JSONObject config = JSONObject.toJSON(configString);
        rootPath = StringHelper.build(config.getString("root")).trimFrom('/').toString() + "/";
    }

    /**
     * 资源ID计算方法(资源ID就是文件名)
     * <p>
     * YYYYMMDDHHMMSSEE.xxx
     * 临时资源为   GSC_ONCExxxxxxxx.eee
     */
    private String getResourceID() {
        DateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
        Calendar calendar = Calendar.getInstance();
        return df.format(calendar.getTime()) + StringHelper.createRandomCode(6);
    }

    /**
     * 获得文件实际储存地址和文件名
     */
    private String getNewStorePath(String fileName) {
        DateFormat df = new SimpleDateFormat("yyyyMMdd");
        Calendar calendar = Calendar.getInstance();
        String ymd = df.format(calendar.getTime());
        String tempPath = "";
        for (int i = 0; i < MAXNO; i++) {
            tempPath = rootPath + ymd + "/" + FileHelper.fileExtension(fileName) + "/" + i + "_" + fileName;
            java.io.File tFile = new java.io.File(tempPath);
            if (!tFile.exists()) {
                FileHelper.newTempFile(tempPath);//创建一个文件占位
                break;
            }
        }
        //---------得到唯一文件了
        return tempPath;
    }

    /**
     * 根据resID获得文件实际储存地址
     */
    private String getRealStorePath(String resID) {
        String exFileName = FileHelper.fileExtension(resID);
        String tString = resID.split("_")[1];
        String dateFolderName = tString.substring(0, tString.indexOf('.') - (1 + 6));
        return rootPath + dateFolderName + "/" + exFileName + "/" + resID;
    }

    public String addResource(byte[] data, String extName){
        String resID = getNewStorePath(getResourceID() + "." + extName);//新的文件名
        return FileHelper.moveFile(data, resID, true) ? resID : null;
    }
    public String addResource(byte[] data, String extName,JSONObject metas) {
        System.out.println("文件存储模式不支持储存元信息");
        addResource(data, extName);
        return null;
    }

    //返回资源ID
    @Override
    public String addResource(java.io.File file) {
        String resID = getNewStorePath(getResourceID() + FileHelper.fileExtension(file.getName()));//新的文件名
        return FileHelper.moveFile(file.getAbsolutePath(), resID, true) ? resID : null;
    }
    @Override
    public String addResource(UploadFile file) {
        String rs = null;
        java.io.File tempFile = FileHelper.newFileEx(FileHelper.getTempPath(), file.getFileInfo().getClientName());
        if( file.writeTo(tempFile) ){// 上传的文件落盘地址,文件落盘成功
            rs = addResource(tempFile);
        }
        return rs;
    }
    @Override
    public String addResource(java.io.File file, JSONObject metas) {
        System.out.println("文件存储模式不支持储存元信息");
        return addResource(file);
    }

    //设置资源元数据
    @Override
    public boolean setMetas(String resourceID, JSONObject newMetas) {
        return true;
    }

    //删除资源
    @Override
    public boolean delResource(String resourceID) {
        java.io.File file = new java.io.File(getRealStorePath(resourceID));
        if (file.exists()) {
            return file.delete();
        }
        return true;
    }

    private boolean isOnceResource(String resourceID) {
        return resourceID.split("_")[0].equals("GSC");
    }

    private String getOnceResourceSymbolicLink(String OnceResourceID) {
        return rootPath + "once/" + OnceResourceID;
    }

    //通过资源ID获得资源数据
    @Override
    public byte[] getResource(String resourceID) {
        byte[] rb = null;
        String filePath = isOnceResource(resourceID) ? getOnceResourceSymbolicLink(resourceID) : getRealStorePath(resourceID);
        java.io.File file = new java.io.File(filePath);
        if (file.exists()) {
            rb = FileHelper.getFile(file);
            if (isOnceResource(resourceID)) {//是一次性文件使用完,删除
                try {
                    Files.deleteIfExists(Paths.get(filePath));
                } catch (Exception e) {
                }
            }
        }
        return rb;
    }

    public InputStream getResourceStream(String resourceID) {
        String filePath = isOnceResource(resourceID) ? getOnceResourceSymbolicLink(resourceID) : getRealStorePath(resourceID);
        java.io.File file = new java.io.File(filePath);
        InputStream out = null;
        if (file.exists()) {
            try {
                out = new FileInputStream(file);
                if (isOnceResource(resourceID)) {
                    Files.deleteIfExists(Paths.get(filePath));
                }
            } catch (Exception e) {
                out = null;
            }
        }
        return out;
    }

    private String createOnceFile(String resID) {
        String newFilePath = "";
        for (int i = 0; i < MAXNO; i++) {
            newFilePath = rootPath + "once/GSC_ONCE" + StringHelper.numCode(6) + i + "." + FileHelper.fileExtension(resID);
            java.io.File newFile = new java.io.File(newFilePath);
            if (!newFile.exists()) {
                //FileHelper.copyFile( getRealStorePath(resID) , newFilePath);
                try {
                    Files.createSymbolicLink(Paths.get(newFilePath), Paths.get(getRealStorePath(resID)));
                } catch (Exception e) {
                }
                break;
            }
        }
        String[] ts = newFilePath.split("/");
        return ts[ts.length - 1];
    }

    //获得一次性资源URL
    @Override
    public String getSafeResourceUrl(String resourceID) {
        return createOnceFile(resourceID);
    }
}
