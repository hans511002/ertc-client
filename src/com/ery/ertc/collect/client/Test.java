package com.ery.ertc.collect.client;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import net.contentobjects.jnotify.JNotifyListener;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;

import com.alibaba.fastjson.JSON;
import com.ery.ertc.collect.client.monitor.utils.FileWatch;
import com.ery.ertc.collect.client.sender.CollectSender;
import com.ery.ertc.collect.client.sender.HttpSender;
import com.ery.ertc.collect.client.sender.Sender;
import com.ery.base.support.log4j.LogUtils;
import com.ery.base.support.sys.SystemConstant;
import com.ery.base.support.utils.Convert;
import com.ery.base.support.utils.Utils;

public class Test {

	// 日志解析器
	static class LogParser {
		String path;
		String key;
		String topicName;
		Sender cc;
		long offset;
		long lastModifyTime;
		Boolean readed = false;
		int betchSize = 1000;

		LogParser(String path, String key, String topicName) {
			this.path = path;
			this.key = key;
			this.topicName = topicName;
			cc = new HttpSender();// http发送客户端
			offset = 0;
			lastModifyTime = 0;
		}

		void setOffset(long offset) {
			this.offset = offset;
		}

		void start() {
			try {
				File f = new File(path + "/" + key);
				lastModifyTime = f.lastModified();
				read(f);// 读取内容
			} catch (Exception e) {
				System.err.println("读取[" + key + "]出错:" + e.getMessage());
				e.printStackTrace();
			}
		}

		synchronized void read(File f) throws IOException {
			// FileInputStream fin = new FileInputStream(f);
			// if (offset > 0)
			// fin.skip(offset);
			if (!f.exists()) {
				LogUtils.error("文件不存在:" + f.getAbsolutePath());
				return;
			}
			RandomAccessFile raf = new RandomAccessFile(f, "r");
			raf.seek(offset);
			String str = null;
			int rowCount = 0;
			List<String> content = new ArrayList<String>(this.betchSize);
			while ((str = raf.readLine()) != null) {
				rowCount++;
				offset += str.getBytes().length + 1;
				String[] arr = new String[6];
				arr[0] = key;

				int idx = 15;
				arr[1] = str.substring(0, idx);
				Date d = Convert.toTime(arr[1], "MMM d HH:mm:ss");
				DateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
				arr[1] = df.format(d);

				str = str.substring(idx + 1);
				idx = str.indexOf(" ");
				arr[2] = str.substring(0, idx);

				str = str.substring(idx + 1);
				idx = str.indexOf(": ");
				String process = str.substring(0, idx);
				Matcher mcher = Pattern.compile("\\[(\\d+)\\]").matcher(process);
				if (mcher.find()) {
					arr[3] = process.replace(mcher.group(0), "");
					arr[4] = mcher.group(1);
				} else {
					arr[3] = process;
					arr[4] = "-1";
				}
				arr[5] = str.substring(idx + 2);
				String sss = JSON.toJSONString(arr);
				try {
					if (this.betchSize > 1) {
						content.add(sss);
						if (content.size() >= this.betchSize) {
							String ret = cc.sendData(topicName, content);
							if ("ok".equals(ret)) {
								content.clear();
								LogUtils.debug("ok->" + content);
							} else {
								LogUtils.info("fail->" + ret + ", content=" + content);
							}
						}
					} else {
						String ret = cc.sendData(topicName, sss);
						if ("ok".equals(ret)) {
							LogUtils.debug("ok->" + sss);
						} else {
							LogUtils.info("fail->" + ret + "," + sss);
						}
					}
				} catch (Exception e) {
					LogUtils.error("失败[" + e.getMessage() + "]:" + sss, e);
				}
			}
			if (!content.isEmpty()) {
				String ret = cc.sendData(topicName, content);
				if ("ok".equals(ret)) {
					content.clear();
					LogUtils.debug("ok->" + content);
				} else {
					LogUtils.info("fail->" + ret + ", content=" + content);
				}
			}
			raf.close();
			System.out.println("read file " + f.getAbsolutePath() + " rows:" + rowCount);
		}

		void modify(String name) {
			synchronized (readed) {
				if (readed) {
					return;
				} else {
					readed = true;
				}
			}
			String fn = null;
			try {
				if (name != null) {
					fn = name;
				} else {
					fn = key;
				}
				File f = new File(path + "/" + fn);
				if (lastModifyTime < f.lastModified()) {
					lastModifyTime = f.lastModified();
				} else {
					return;
				}
				read(f);// 读取内容
			} catch (Exception e) {
				LogUtils.error("修改读取[" + fn + "]出错:" + e.getMessage(), e);
			} finally {
				readed = false;
			}
		}

		void delete() {

		}
	}

	public static CommandLine buildCommandline(String[] args) {
		final Options options = new Options();
		Option opt = new Option("h", "help", false, "打印帮助");
		opt.setRequired(false);
		options.addOption(opt);

		opt = new Option("t", "time", true, "监控时间(秒)，默认-1，标示永久");
		opt.setRequired(false);
		options.addOption(opt);

		opt = new Option("offset", null, true, "位置");
		opt.setRequired(false);
		options.addOption(opt);

		opt = new Option("localTest", null, false, "本机测试调试");
		opt.setRequired(false);
		options.addOption(opt);

		PosixParser parser = new PosixParser();
		HelpFormatter hf = new HelpFormatter();
		hf.setWidth(110);
		CommandLine commandLine = null;
		try {
			commandLine = parser.parse(options, args);
			if (commandLine.hasOption('h')) {
				hf.printHelp("queue", options, true);
				return null;
			}
		} catch (org.apache.commons.cli.ParseException e) {
			hf.printHelp("queue", options, true);
			return null;
		}

		return commandLine;
	}

	public static void main(String[] args) throws InterruptedException, ParseException {
		// SimpleDateFormat keyDF = new SimpleDateFormat("yyyyMMddHHmmss.sss");
		// SimpleDateFormat keyDF1 = new SimpleDateFormat("yyyyMMddHHmmsssss");
		// Date d = new Date();
		// System.out.println(keyDF.format(d));
		// System.out.println(keyDF1.format(d));
		// if (keyDF != null) {
		// return;
		// }

		System.setProperty("logFileName", "monitor");
		Utils.sleep(5000);
		CommandLine cmd = buildCommandline(args);
		if (cmd == null) {
			return;
		}
		if (cmd.hasOption("localTest")) {
			SystemConstant.setSYS_CONF_FILE("conf/monitor.properties");
		} else {
			SystemConstant.setSYS_CONF_FILE("../conf/monitor.properties");
		}
		SystemConstant.setLOAD_DB_DATA_SOURCE(false);
		SystemConstant.setLOG_FILE_SUFFIX("client");
		System.out.println("server.zk.url=" + CollectSender.getServerZkUrl());
		CollectSender.init();

		final String path = "D:\\mpp\\dev_workspace\\collectClient";
		// final String path = "/var/log";
		final Map<String, LogParser> parserMap = new HashMap<String, LogParser>();
		parserMap.put("secure", new LogParser(path, "secure", "TEST_SYS_LOG"));
		// parserMap.put("maillog", new LogParser(path, "maillog",
		// "TEST_SYS_LOG"));
		// parserMap.put("cron", new LogParser(path, "cron", "TEST_SYS_LOG"));
		// parserMap.put("messages", new LogParser(path, "messages",
		// "TEST_SYS_LOG"));
		// parserMap.put("spooler", new LogParser(path, "spooler",
		// "TEST_SYS_LOG"));

		try {
			long of = Convert.toLong(cmd.getOptionValue("offset"), 0);
			// 初始读取
			for (final String key : parserMap.keySet()) {
				long len = new File(path + "/" + key).length();
				if (of == -1) {
					parserMap.get(key).setOffset(len);
					LogUtils.info("初始位置[" + key + "]:" + len);
				} else if (of <= len) {
					parserMap.get(key).setOffset(of);
					LogUtils.info("初始位置[" + key + "]:" + of);
				} else {
					parserMap.get(key).setOffset(0);
					LogUtils.info("初始位置[" + key + "]:" + 0);
				}

				parserMap.get(key).start();
			}

			boolean ret = FileWatch.addFileMonitor(path, new JNotifyListener() {
				@Override
				public void fileCreated(int i, String path, String name) {
					if (parserMap.containsKey(name)) {
						LogUtils.info("文件创建:" + name);
						parserMap.get(name).setOffset(0);
						parserMap.get(name).modify(name);
					} else {
						LogUtils.debug("created File:" + name);
					}
				}

				@Override
				public void fileDeleted(int i, String path, String name) {
					LogUtils.info("文件删除:" + name);
				}

				@Override
				public void fileModified(int i, String path, String name) {
					if (parserMap.containsKey(name)) {
						parserMap.get(name).modify(name);
					} else {
						LogUtils.debug("modified File:" + name);
					}
				}

				@Override
				public void fileRenamed(int i, String path, String oldName, String newName) {
					if (parserMap.containsKey(oldName)) {
						LogUtils.info("文件重命名:" + oldName + "->" + newName);
						parserMap.get(oldName).modify(newName);
						parserMap.get(oldName).setOffset(0);
					} else {
						LogUtils.debug("renamed File:" + oldName + "->" + newName);
					}
				}
			});

			long x = Convert.toLong(cmd.getOptionValue("t"), -1);
			if (x != -1) {
				LogUtils.info("监控将在[" + x + "]秒后停止!");
			} else {
				LogUtils.info("监控将在一直运行!");
			}
			int i = 0;
			while (ret && (x == -1 || i < x)) {
				i++;
				Thread.sleep(1000);
			}
		} catch (Throwable e) {
			e.printStackTrace();
		}
		LogUtils.info("系统退出!");
		System.exit(1);
	}

}
