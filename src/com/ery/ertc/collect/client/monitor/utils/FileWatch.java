package com.ery.ertc.collect.client.monitor.utils;

import com.ery.base.support.log4j.LogUtils;
import net.contentobjects.jnotify.JNotify;
import net.contentobjects.jnotify.JNotifyException;
import net.contentobjects.jnotify.JNotifyListener;

import java.util.HashMap;
import java.util.Map;


public class FileWatch {
    
    private static Map<String,Integer> watchMap = new HashMap<String, Integer>();

    public synchronized static boolean addFileMonitor(String path,JNotifyListener listener){
        if(watchMap.containsKey(path)){
            removeFileMonitor(path);
        }
        int mask = JNotify.FILE_CREATED | JNotify.FILE_DELETED | JNotify.FILE_MODIFIED | JNotify.FILE_RENAMED;
        try {
            int watchId = JNotify.addWatch(path, mask, false, listener);//第三个参数标示递归子目录
            watchMap.put(path,watchId);
            return true;
        } catch (JNotifyException e) {
            LogUtils.error("添加文件["+path+"]监控出错!"+e.getMessage(),e);
            return false;
        }
    }

    public synchronized static void removeFileMonitor(String path){
        int watchId = watchMap.remove(path);
        try {
            JNotify.removeWatch(watchId);
        } catch (JNotifyException e) {
            LogUtils.error("删除文件[" + path + "]监控出错!" + e.getMessage(), e);
        }
    }
    

    public static void main(String[] args){
        try{
            print(1|2);
            print(1|2|4);
            print(1|2|4|8);
            print(1|4|8);
            print(2|4|8);
            print(4|8);
            print(6&8);
            print(6&4);
            print(6&2);
            print(6&1);
            new FileWatch().sampleTest();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void sampleTest() throws Exception{
        // path to watch
        String path = "C:\\TDDOWNLOAD";

        // watch mask, specify events you care about,
        // or JNotify.FILE_ANY for all events.
        int mask = JNotify.FILE_CREATED
                | JNotify.FILE_DELETED
                | JNotify.FILE_MODIFIED
                | JNotify.FILE_RENAMED;


        // watch subtree?
        boolean watchSubtree = true;
        // add actual watch
        int watchID = JNotify.addWatch(path, mask, watchSubtree, new Listener());


        // sleep a little, the application will exit if you
        // don't (watching is asynchronous), depending on your
        // application, this may not be required
        Thread.sleep(1000000);

        // to remove watch the watch
        boolean res = JNotify.removeWatch(watchID);
        if (!res) {
            // invalid watch ID specified.
        }
    }

    static void print(Object msg) {
        System.err.println(msg);
    }


    class Listener implements JNotifyListener {
        public void fileRenamed(int wd, String rootPath, String oldName, String newName) {
            print("renamed " + rootPath + " : " + oldName + " -> " + newName);
        }


        public void fileModified(int wd, String rootPath, String name) {
            print("modified " + rootPath + " : " + name);
        }


        public void fileDeleted(int wd, String rootPath, String name) {
            print("deleted " + rootPath + " : " + name);
        }


        public void fileCreated(int wd, String rootPath, String name) {
            print("created " + rootPath + " : " + name);
        }

    }

}
