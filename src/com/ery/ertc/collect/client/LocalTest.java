package com.ery.ertc.collect.client;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;

import com.alibaba.fastjson.JSON;
import com.ery.ertc.collect.client.sender.CollectSender;
import com.ery.ertc.collect.client.sender.HttpSender;
import com.ery.ertc.collect.client.sender.Sender;
import com.ery.ertc.collect.client.sender.SocketSender;
import com.ery.ertc.collect.client.sender.WSSender;
import com.ery.base.support.sys.SystemConstant;
import com.ery.base.support.utils.Convert;
import com.ery.base.support.utils.Utils;

public class LocalTest {

	public static void main(String[] args) throws Exception {
		// SysConstant.setSYS_CONF_FILE("conf/monitor.properties");
		//
		// // String str =
		// "Apr 15 15:54:16 hadoop02 su: pam_unix(su:session): session opened for user root by mq(uid=507)";
		// String str =
		// "Apr 14 10:18:47 hadoop02 /usr/local/ganglia/sbin/gmond[2275]: Error 1 sending the modular data for heartbeat#012";
		// // String str =
		// "Apr 15 15:19:34 hadoop02 sshd[18230]: subsystem request for sftp";
		// // String str =
		// "Apr 14 10:15:10 hadoop02 crmd: [1917]: info: do_state_transition: Membership changed: 78356 -> 78360 - join restart";
		// MonitorDAO dao = new MonitorDAO();
		// MonitorPO po = dao.listMonitor("2").get(0);
		// LogParser logParser = new LogParser("/var/log/","(secure)",po);
		// logParser.parseFieldsAndSend(str,"secure",0,0);
		//
		//
		// if(true)
		// return;
		// Object[] objects = new Object[10];
		// objects[0] = 3;
		// objects[1] = "asfasdf";
		// objects[2] = "很好asdfasdf";
		// objects[3] = 43434.2323;
		// objects[4] = "dasdf打扫的发送到发送端发水电费";
		// objects[5] = System.currentTimeMillis();
		// objects[6] = "ASDF";
		// objects[7] = "DSADFSADF";
		// objects[8] = 234234234;
		// objects[9] = "asdfasdfasdf asdfasdf asdfasdf多发僧发水电费";
		//
		// long st = System.currentTimeMillis();
		// long et = 0;
		// for(int x =0;x<1000;x++){
		// String str = JSON.toJSONString(objects);
		// List<Object> o = JSON.parseArray(str);
		// }
		// et = System.currentTimeMillis();
		// System.out.println("json:"+(et-st));
		//
		//
		// st = System.currentTimeMillis();
		// Object[] oo = new Object[10];
		// for(int x =0;x<1000;x++){
		// byte[] bs = new byte[300];
		// int len = 0;
		// for(int i=0;i<10;i++){
		// byte[] b = objects[i].toString().getBytes();
		// byte[] b_= Convert.toBytes(b.length);
		// System.arraycopy(b_,0,bs,len,4);
		// len += 4;
		// System.arraycopy(b,0,bs,len,b.length);
		// len += b.length;
		// }
		// byte[] b = new byte[len];
		// System.arraycopy(bs,0,b,0,len);
		//
		// //反序列
		// int idx = 0;
		// for(int i=0;i<10;i++){
		// byte[] bx = new byte[4];
		// System.arraycopy(b,idx,bx,0,4);
		// idx += 4;
		// int l = Convert.toInt(bx);
		// bx = new byte[l];
		// System.arraycopy(b,idx,bx,0,l);
		// idx += l;
		// oo[i] = new String(bx);
		// }
		// }
		// et = System.currentTimeMillis();
		//
		// System.out.println("byte:"+(et-st));
		//
		// if(true)return;
		CommandLine cmd = buildCommandline(args);
		if (cmd == null) {
			return;
		}
		if (cmd.hasOption("localTest")) {
			SystemConstant.setSYS_CONF_FILE("conf/monitor.properties");
		} else {
			SystemConstant.setSYS_CONF_FILE("../conf/monitor.properties");
		}
		SystemConstant.setLOAD_DB_DATA_SOURCE(false);// 无需加载系统数据源配置
		SystemConstant.setLOG_FILE_SUFFIX("client");
		System.out.println("collect.zk.url=" + CollectSender.getServerZkUrl());
		CollectSender.init();

		final String t = cmd.getOptionValue("t", "http").toLowerCase();
		final long tn = Convert.toInt(cmd.getOptionValue("tn"), 1);
		final Sender cc;
		if ("http".equals(t)) {
			cc = new HttpSender();
		} else if ("socket".equals(t)) {
			cc = new SocketSender();
			((SocketSender) cc).setIsAsync(false);
		} else {
			cc = new WSSender();
		}

		int s = Convert.toInt(cmd.getOptionValue("s"), 200);
		String[] arr = new String[6];
		arr[0] = "test";
		arr[1] = "20140214093608";
		arr[2] = "192.168.10.157";
		arr[3] = "ttt";
		arr[4] = "-1";
		arr[5] = "";
		for (int i = 0; i < s - 56; i++) {
			arr[5] += "x";
		}
		final String x = JSON.toJSONString(arr);
		System.out.println("每条消息size:" + x.getBytes().length);
		for (int i = 0; i < tn; i++) {
			Thread th = new Thread() {
				public void run() {
					while (true) {
						int ls = x.getBytes().length;
						try {
							String ret = cc.sendData("TEST_" + t.toUpperCase() + "_LOG", x);
							if (ret != null && !"".equals(ret) && !"ok".equals(ret) && !ret.endsWith(",ok")) {
								System.out.println(ret);
								ecnt++;
							} else {
								size += ls;
								cnt++;
							}
							Thread.yield();
						} catch (Exception e) {
							e.printStackTrace();
							ecnt++;
						}
					}
				}
			};
			th.start();
		}

		Thread c = new Thread() {
			public void run() {
				long lc = 0;
				long le = 0;
				long ls = 0;
				while (true) {
					long c_ = cnt;
					long e_ = ecnt;
					long s_ = size;
					System.out.println("类型:" + t + ",线程数:" + tn + "》总数:" + c_ + ",错误数:" + e_ + ",tps:" + (c_ - lc) +
							",tps_size:" + (s_ - ls));
					lc = c_;
					le = e_;
					ls = s_;
					Utils.sleep(1000);
				}
			}
		};
		c.start();

		while (true) {
			Utils.sleep(100);
		}

	}

	static long cnt = 0;
	static long ecnt = 0;
	static long size = 0;

	public static CommandLine buildCommandline(String[] args) {
		final Options options = new Options();
		Option opt = new Option("h", "help", false, "打印帮助");
		opt.setRequired(false);
		options.addOption(opt);

		opt = new Option("t", "type", true, "发送类型");
		opt.setRequired(false);
		options.addOption(opt);

		opt = new Option("tn", "thnum", true, "线程数");
		opt.setRequired(false);
		options.addOption(opt);

		opt = new Option("s", "size", true, "每条消息大小");
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

}
