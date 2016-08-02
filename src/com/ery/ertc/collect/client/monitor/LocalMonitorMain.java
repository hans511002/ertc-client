package com.ery.ertc.collect.client.monitor;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;

import sun.misc.Signal;
import sun.misc.SignalHandler;

import com.alibaba.fastjson.JSON;
import com.ery.ertc.collect.client.PropConfig;
import com.ery.ertc.collect.client.monitor.podo.MonitorDAO;
import com.ery.ertc.collect.client.monitor.podo.MonitorPO;
import com.ery.base.support.log4j.LogUtils;
import com.ery.base.support.sys.SystemConstant;
import com.ery.base.support.sys.SystemVariable;
import com.ery.base.support.utils.Convert;
import com.ery.base.support.utils.Utils;

public class LocalMonitorMain {

	private static int PROCESS_ID = 0;// 进程ID

	// 下面的对象用于实现，本地文件内存映射存储，及程序锁
	private final static String LOCK_FILE_NAME = "monitor.lockstore";// 内存映射文件名
	private RandomAccessFile randomAccessFile;
	private FileChannel fileChannel;
	private FileLock fileLock;
	private MappedByteBuffer mappedByteBuffer;
	public Map<String, Object> STORE_HD_OFFSET;
	public boolean isStopd = true;
	// 两种模式的监控逻辑
	private FilePollMonitor filePollMonitor;// 线程轮询
	private FileWatchMonitor fileWatchMonitor;// 操作系统,即时通知

	public final static int HD_OFFSET_SIZE = 2048;
	public final static int HD_ITEM_SIZE = 1024 * 150;// 150kb
	private final static int LOCK_FILE_SIZE = 4 + HD_OFFSET_SIZE + HD_ITEM_SIZE * 8;

	static LocalMonitorMain ins = null;

	public static LocalMonitorMain getIns() {
		if (ins == null) {
			ins = new LocalMonitorMain();
		}
		return ins;
	}

	private void lockFile() throws Exception {
		String usrDir = PropConfig.getCollectTmpDir();
		File file = new File(usrDir, LOCK_FILE_NAME);
		// 若存在，直接获取锁，否则先创建
		if (!file.exists()) {
			if (file.createNewFile()) {
				randomAccessFile = new RandomAccessFile(file, "rw");
				randomAccessFile.write(new byte[LOCK_FILE_SIZE]);
			}
		}
		if (randomAccessFile == null) {
			randomAccessFile = new RandomAccessFile(file, "rw");
		}
		fileChannel = randomAccessFile.getChannel();
		fileLock = fileChannel.tryLock(4, LOCK_FILE_SIZE - 4, false);// 前4位是进程号，不锁
		if (fileLock != null && fileLock.isValid()) {
			PROCESS_ID = Utils.getCurrentProcessId();
			byte[] bs = Convert.toBytes(PROCESS_ID);
			fileChannel.write(ByteBuffer.wrap(bs));
			fileChannel.force(true);// 立即将数据持久化到磁盘

			mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 4, HD_OFFSET_SIZE);
			mappedByteBuffer.load();
			bs = new byte[4];
			mappedByteBuffer.get(bs);
			int len = Convert.toInt(bs);
			STORE_HD_OFFSET = new HashMap<String, Object>();
			if (len > 0) {
				bs = new byte[len];
				mappedByteBuffer.get(bs);
				String str = new String(bs);
				STORE_HD_OFFSET = JSON.parseObject(str);
			}
		} else {
			try {
				byte[] bs = new byte[4];
				randomAccessFile.read(bs);
				int processId = Convert.toInt(bs);
				throw new RuntimeException("程序已经被启动(进程号:" + processId + ")，不可重复启动!");
			} finally {
				randomAccessFile.close();
			}
		}
	}

	public void start() throws Exception {
		this.lockFile();
		MonitorDAO dao = new MonitorDAO();
		List<MonitorPO> monitorPOs = dao.listMonitor(PropConfig.getMonitorIdsKey());
		List<MonitorPO> pollMonitors = new ArrayList<MonitorPO>();
		for (int i = 0; i < monitorPOs.size(); i++) {
			MonitorPO po = monitorPOs.get(i);
			if (po.getMONITOR_FREQUENCY() > 0) {
				pollMonitors.add(monitorPOs.remove(i));
				i--;
			}
		}
		if (monitorPOs.size() > 0) {
			this.fileWatchMonitor = new FileWatchMonitor(monitorPOs, this.fileChannel);
			this.fileWatchMonitor.start();
		}
		if (pollMonitors.size() > 0) {
			this.filePollMonitor = new FilePollMonitor(pollMonitors, this.fileChannel);
			this.filePollMonitor.start();
		}
		this.storeHD();// 存储头map
	}

	public static void main(String[] args) {
		System.setProperty("logFileName", "monitor");
		try {
			CommandLine cmd = buildCommandline(args);
			if (cmd == null) {
				return;
			}
			if (!cmd.hasOption("s"))
				return;
			final LocalMonitorMain main = LocalMonitorMain.getIns();
			SystemConstant.setSYS_CONF_FILE("monitor.properties");
			System.getProperties().putAll(SystemVariable.getConf());
			System.setProperty("logFileName", SystemVariable.getLogFileName());
			// 用于捕获程序终端信号
			SignalHandler sh = new SignalHandler() {
				@Override
				public void handle(Signal signal) {
					String nm = signal.getName();
					if (nm.equals("TERM") || nm.equals("INT") || nm.equals("KILL")) {
						LogUtils.info("程序捕获到[" + nm + "]信号,即将停止!");
						main.stop();

					}
				}
			};
			Signal.handle(new Signal("TERM"), sh);// kill命令
			Signal.handle(new Signal("INT"), sh);// ctrl+c 命令
			LogUtils.info("进程中断监听注册ok(识别普通kill,ctrl+c)!");
			LogUtils.info("监控程序(进程号:" + PROCESS_ID + ")已启动,监控线程运行中……");
			main.start();
			main.loop();// 循环守护进程
		} catch (Throwable e) {
			LogUtils.error(null, e);
			System.exit(-1);
		}
	}

	// 存储map的值
	private void storeHD() {
		String str = JSON.toJSONString(STORE_HD_OFFSET);
		byte[] bs = str.getBytes();
		mappedByteBuffer.position(0);
		mappedByteBuffer.put(Convert.toBytes(bs.length));
		mappedByteBuffer.position(4);
		mappedByteBuffer.put(bs);
		mappedByteBuffer.force();
		LogUtils.info("位置头信息已存储!");
	}

	public void stop() {
		isStopd = true;
		if (fileWatchMonitor != null) {
			fileWatchMonitor.stop();
		}
		if (filePollMonitor != null) {
			filePollMonitor.stop();
		}
	}

	// 释放各种资源
	private void destory() {
		try {
			if (mappedByteBuffer != null) {
				mappedByteBuffer.force();
			}
			if (fileLock != null) {
				fileLock.release();// 释放锁
			}
			if (fileChannel != null) {
				fileChannel.close();// 关闭管道
			}
			if (randomAccessFile != null) {
				randomAccessFile.close();// 关闭流
			}
		} catch (Exception e) {
			LogUtils.error("释放文件锁出错!", e);
		}
	}

	// 是否停止
	private boolean isStoped() {
		boolean stopd = true;
		if (fileWatchMonitor != null && fileWatchMonitor.isStarted()) {
			stopd = false;
		}
		if (filePollMonitor != null && filePollMonitor.isStarted()) {
			stopd = false;
		}
		return stopd;
	}

	// 循环守护
	private void loop() {
		while (!isStoped()) {
			Utils.sleep(100);
		}
		destory();
		LogUtils.info("主线程停止,监控程序(进程号:" + PROCESS_ID + ")退出！");
		System.exit(0);
	}

	public static CommandLine buildCommandline(String[] args) {
		final Options options = new Options();
		Option opt = new Option("h", "help", false, "打印帮助");
		opt.setRequired(false);
		options.addOption(opt);
		opt = new Option("s", "start", false, "启用服务");
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
}
