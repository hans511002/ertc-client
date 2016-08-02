package com.ery.ertc.collect.client.monitor.utils;

import com.ery.base.support.log4j.LogUtils;
import name.pachler.nio.file.*;
import name.pachler.nio.file.ext.ExtendedWatchEventKind;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;


public class JPathWatch {

    private static Map<String,WatchService> watchMap = new HashMap<String, WatchService>();

    public interface FileListener{
        void fileCreated(String rootPath, String name);

        void fileDeleted(String rootPath,String name);

        void fileModified(String rootPath, String name);

        void fileRenamed(String rootPath, String oldName,String newName);
        
        void fileInvalid(String rootPath);
    }

    public synchronized static boolean addFileMonitor(final String path,final FileListener listener){
        if(watchMap.containsKey(path)){
            removeFileMonitor(path);
        }
        final WatchService watchService = FileSystems.getDefault().newWatchService();
        Path p = Paths.get(path);
        WatchKey key = null;
        try {
            WatchEvent.Kind[] events = new WatchEvent.Kind[]{
                    StandardWatchEventKind.ENTRY_CREATE,
                    StandardWatchEventKind.ENTRY_DELETE,
                    StandardWatchEventKind.ENTRY_MODIFY,
                    ExtendedWatchEventKind.ENTRY_RENAME_FROM,
                    ExtendedWatchEventKind.ENTRY_RENAME_TO,
            };
            key = p.register(watchService,events);
        } catch (Exception e){
            LogUtils.error("添加文件["+path+"]监控出错!"+e.getMessage(),e);
            return false;
        }
        new Thread(){
            @Override
            public void run(){
                for(;;){
                    WatchKey signalledKey = null;
                    try {
                        signalledKey = watchService.take();
                    } catch (InterruptedException ix){
                        continue;
                    } catch (ClosedWatchServiceException cwse){
                        break;
                    }
                    List<WatchEvent<?>> list = signalledKey.pollEvents();
                    signalledKey.reset();

                    for(Iterator<WatchEvent<?>> it = list.iterator();it.hasNext();){
                        WatchEvent e = it.next();
                        if(e.context() instanceof Path){
                            Path context = (Path)e.context();
                            String filename = context.toString();//此可能标示文件，也可能标示子目录
                            if(e.kind() == StandardWatchEventKind.ENTRY_CREATE){
                                listener.fileCreated(path, filename);
                            } else if(e.kind() == StandardWatchEventKind.ENTRY_DELETE){
                                listener.fileDeleted(path, filename);
                            } else if (e.kind() == StandardWatchEventKind.ENTRY_MODIFY){
                                //修改子目录里面的内容会触发子目录的修改事件，修改文件内容会触发两个事件
                                //未递归实现，子目录的子目录下面文件变动不会触发爷爷目录的监控事件
                                listener.fileModified(path, filename);
                            }else if(e.kind() == ExtendedWatchEventKind.ENTRY_RENAME_FROM){
                                //如果注册时未注册重命名事件，重命名动作将触发一个创建，一个删除事件
                                //重命名会触发连续两个事件
                                String oldName = filename;
                                e = it.next();//ENTRY_RENAME_TO
                                context = (Path)e.context();
                                filename = context.toString();
                                listener.fileRenamed(path,oldName,filename);
                            }
                        }else{
                            if(e.kind() == StandardWatchEventKind.OVERFLOW){
                                listener.fileInvalid(path);
                            }else if(e.kind() == ExtendedWatchEventKind.KEY_INVALID){
                                listener.fileInvalid(path);
                            }
                        }
                    }

                }
            }
        }.start();

        watchMap.put(path,watchService);
        return true;
    }


    public synchronized static void removeFileMonitor(String path){
        WatchService watchService = watchMap.remove(path);
        if(watchService!=null){
            try {
                watchService.close();
            } catch (IOException e) {
                LogUtils.error("删除文件[" + path + "]监控出错!" + e.getMessage(), e);
            }
        }
    }

    public static void main(String[] args) {

        WatchService watchService = FileSystems.getDefault().newWatchService();

//        String tempDir = System.getProperty("java.io.tmpdir");
        String tempDir = "C:\\TDDOWNLOAD";
        Path path = Paths.get(tempDir);

        System.out.println("using tempdir: " + path.toString());

        WatchKey key = null;
        try {

            key = path.register(watchService, StandardWatchEventKind.ENTRY_CREATE, StandardWatchEventKind.ENTRY_DELETE,StandardWatchEventKind.ENTRY_MODIFY);
        } catch (UnsupportedOperationException uox){
            System.err.println("file watching not supported!");
            // handle this error here
        } catch (IOException iox){
            System.err.println("I/O errors");
            iox.printStackTrace();
            // handle this error here
        }

        // typically, real world applications will run this loop in a
        // separate thread and signal directory changes to another thread
        // (often, this will be Swing's event dispatcher thread; use
        // SwingUtilities.invokeLater())
        for(;;){
            // take() will block until a file has been created/deleted
            WatchKey signalledKey = null;
            try {
                signalledKey = watchService.take();
            } catch (InterruptedException ix){
                // we'll ignore being interrupted
                continue;
            } catch (ClosedWatchServiceException cwse){
                // other thread closed watch service
                System.out.println("watch service closed, terminating.");
                break;
            }

            // get list of events from key
            List<WatchEvent<?>> list = signalledKey.pollEvents();

            // VERY IMPORTANT! call reset() AFTER pollEvents() to allow the
            // key to be reported again by the watch service
            signalledKey.reset();

            // we'll simply print what has happened; real applications
            // will do something more sensible here
            for(WatchEvent e : list){
                String message = "";
                Path context = (Path)e.context();
                String filename = context.toString();
                if(e.kind() == StandardWatchEventKind.ENTRY_CREATE){
                    message = filename + " ----------created";
                } else if(e.kind() == StandardWatchEventKind.ENTRY_DELETE){
                    message = filename + " ----------deleted";
                } else if (e.kind() == StandardWatchEventKind.ENTRY_MODIFY){
                    message = filename + " ----------modify";
                }else if(e.kind() == StandardWatchEventKind.OVERFLOW){
                    message = "OVERFLOW: more changes happened than we could retreive";
                }
                if (filename.endsWith("swp")||filename.endsWith("swpx"))
                {
                    continue;
                }else{
                    System.out.println(message);
                }
            }
        }

    }

}
