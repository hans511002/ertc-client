package com.ery.ertc.collect.client.monitor;

import com.ery.ertc.collect.client.monitor.podo.MonitorPO;
import com.ery.base.support.log4j.LogUtils;
import com.ery.base.support.utils.Utils;

import java.io.File;
import java.io.FilenameFilter;
import java.nio.channels.FileChannel;
import java.util.*;


public class FilePollMonitor extends FileMonitor{

    private Map<Long,Thread> pollThreadMap = new HashMap<Long, Thread>();

    protected FilePollMonitor(List<MonitorPO> pos,FileChannel fileChannel) throws Exception{
        super(pos,fileChannel);
        for(final MonitorPO po : pos){
            Thread pollThread = new Thread(){
                @Override
                public void run(){
                    List<LogParser> logParsers = new ArrayList<LogParser>();
                    for(List<LogParser> logps : monitorPathMap.values()){
                        for(LogParser logParser : logps){
                            if(logParser.getPo().getMONITOR_ID()==po.getMONITOR_ID()){
                                logParser.init();
                                logParsers.add(logParser);
                            }
                        }
                    }

                    while (true){
                        for(final LogParser logParser : logParsers){
                            try{
                                File f = new File(logParser.getPath());
                                String[] fns = f.list(new FilenameFilter() {
                                    @Override
                                    public boolean accept(File dir, String name) {
                                        File file = new File(dir,name);
                                        if(file.isFile() && logParser.checkMonitor(logParser.getPath(),name)){
                                            logParser.change(name,name);
                                            logParser.savePointor();
                                            return true;
                                        }
                                        return false;//返回false，标示被过滤掉
                                    }
                                });

                                Set<String> fset = new HashSet<String>();
                                Collections.addAll(fset,fns);
                                Map<String,List<Object>> pointorMap = FileMonitor.STORE_POINTER.get(logParser.getPo().getMONITOR_ID()).get(logParser.getPath());
                                for(Iterator<Map.Entry<String,List<Object>>> it = pointorMap.entrySet().iterator() ; it.hasNext();){
                                    String k = it.next().getKey();
                                    if(!fset.contains(k)){
                                        it.remove();
                                        logParser.savePointor();
                                    }
                                }

                            }catch (Exception e){
                                LogUtils.error(null, e);
                            }
                        }
                        Utils.sleep(po.getMONITOR_FREQUENCY());
                    }
                }
            };
            pollThreadMap.put(po.getMONITOR_ID(),pollThread);
        }
    }

    @Override
    public void start() {
        if(started)return;
        if(pos.size()>0){
            for(Thread th : pollThreadMap.values()){
                th.start();
            }
            started = true;
            LogUtils.info("轮询文件监控已启动!");
        }
    }

    @Override
    public void stop() {
        if(started){
            for(Thread th : pollThreadMap.values()){
                th.stop();
            }
            for(String path : monitorPathMap.keySet()){
                List<LogParser> logParsers = monitorPathMap.get(path);
                for(LogParser logParser : logParsers){
                    logParser.savePointor();
                }
            }
            started = false;
        }
    }

}
