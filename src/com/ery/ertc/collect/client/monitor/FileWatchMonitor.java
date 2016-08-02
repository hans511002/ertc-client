package com.ery.ertc.collect.client.monitor;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.List;

import net.contentobjects.jnotify.JNotifyListener;

import com.ery.ertc.collect.client.monitor.podo.MonitorPO;
import com.ery.ertc.collect.client.monitor.utils.FileWatch;
import com.ery.base.support.log4j.LogUtils;

public class FileWatchMonitor extends FileMonitor {

	protected FileWatchMonitor(List<MonitorPO> pos, FileChannel fileChannel) throws IOException {
		super(pos, fileChannel);
	}

	@Override
	public void start() {
		if (started)
			return;
		if (pos.size() > 0) {
			for (String path : monitorPathMap.keySet()) {
				// 初始每个监控项上次监控点
				for (LogParser logParser : monitorPathMap.get(path)) {
					logParser.init();
				}

				// 注册基于操作系统通知的文件监听
				FileWatch.addFileMonitor(path, new JNotifyListener() {
					@Override
					public void fileCreated(int i, String path, String name) {
						if (!path.endsWith("/")) {
							path = path + "/";
						}
						for (LogParser logParser : monitorPathMap.get(path)) {
							if (logParser.checkMonitor(path, name)) {
								logParser.setOffset(name, 0);
								logParser.change(name, name);
								logParser.savePointor();
							}
						}
						LogUtils.info("目录[" + path + "]发生文件创建事件:" + name);
					}

					@Override
					public void fileDeleted(int i, String path, String name) {
						if (!path.endsWith("/")) {
							path = path + "/";
						}
						for (LogParser logParser : monitorPathMap.get(path)) {
							if (logParser.checkMonitor(path, name)) {
								logParser.delete(name);
								logParser.savePointor();
							}
						}
						LogUtils.info("目录[" + path + "]发生文件删除事件:" + name);
					}

					@Override
					public void fileModified(int i, String path, String name) {
						if (!path.endsWith("/")) {
							path = path + "/";
						}
						for (LogParser logParser : monitorPathMap.get(path)) {
							if (logParser.checkMonitor(path, name)) {
								logParser.change(name, name);
								logParser.savePointor();
							}
						}
						// LogUtils.debug("目录["+path+"]发生文件修改事件:" + name);
						// //此事件会很频繁，最好关闭此条日志
					}

					@Override
					public void fileRenamed(int i, String path, String oldName, String newName) {
						if (!path.endsWith("/")) {
							path = path + "/";
						}
						for (LogParser logParser : monitorPathMap.get(path)) {
							if (logParser.checkMonitor(path, oldName)) {
								// 文件重命名为其他文件(需要将未读完的继续读完)
								logParser.change(newName, oldName);
								logParser.setOffset(oldName, 0);
								logParser.savePointor();
							} else if (logParser.checkMonitor(path, newName)) {
								// 其他文件重命名为本文件
								logParser.setOffset(newName, -1);
								logParser.change(newName, newName);
								logParser.savePointor();
							}
						}
						LogUtils.info("目录[" + path + "]发生重命名事件:" + oldName + "->" + newName);
					}
				});
				LogUtils.info("已注册操作系统文件监控[" + path + "]!");
			}
			started = true;
		}
	}

	@Override
	public void stop() {
		if (started) {
			for (String path : monitorPathMap.keySet()) {
				FileWatch.removeFileMonitor(path);
				List<LogParser> logParsers = monitorPathMap.get(path);
				for (LogParser logParser : logParsers) {
					logParser.stop();
				}
			}
			started = false;
		}
	}

}
