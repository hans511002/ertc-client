package com.ery.ertc.collect.client.monitor;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.InputStreamReader;
import java.nio.MappedByteBuffer;
import java.nio.charset.Charset;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.alibaba.fastjson.JSON;
import com.googlecode.aviator.AviatorEvaluator;
import com.ery.ertc.collect.client.PropConfig;
import com.ery.ertc.collect.client.monitor.podo.MonitorPO;
import com.ery.ertc.collect.client.sender.HttpSender;
import com.ery.ertc.collect.client.sender.Sender;
import com.ery.ertc.collect.client.sender.SocketSender;
import com.ery.ertc.collect.client.sender.WSSender;
import com.ery.base.support.log4j.LogUtils;
import com.ery.base.support.utils.Convert;
import com.ery.base.support.utils.StringUtils;
import com.ery.base.support.utils.UnicodeInputStream;

public class LogParser {

	public final static String MAC_MONITOR_ID = "MONITOR_ID";// 监控ID
	public final static String MAC_HOST = "HOST";// 本机IP宏变量名
	public final static String MAC_FILE_NAME = "FILE_NAME";// 当前读取文件名
	public final static String MAC_LINE_OFFSET = "LINE_OFFSET";// 消息所在文件位置
	public final static String MAC_LINE_NUMBER = "LINE_NUMBER";// 消息所在文件行
	public final static String MAC_SEND_TIME = "SEND_TIME";// 发送时间 毫秒数

	private String path;
	private String nameRegex;
	private MonitorPO po;
	private Sender sender;

	// 针对每个文件上锁，保证同一时刻只有一个线程真正读取文件
	private Map<String, Boolean> lockMap = new HashMap<String, Boolean>();

	public LogParser(String path, String nameRegex, MonitorPO po) {
		this.path = path;
		this.nameRegex = nameRegex;
		this.po = po;
		if (po.getNOTICE_TYPE().equals("WS")) {
			sender = new WSSender();
		} else if (po.getNOTICE_TYPE().equals("HTTP")) {
			sender = new HttpSender();
		} else {
			SocketSender sender_ = new SocketSender();
			// sender_.setStreamFlag(1);//监控方式（产生的数据一次不会很多，因此可以采用非持续性连接）
			sender = sender_;
		}
	}

	private Map<String, List<Object>> pointorMap;

	// 初始
	public synchronized void init() {
		if (pointorMap != null) {
			return;
		}
		Map<String, Map<String, List<Object>>> pathPonitor = FileMonitor.STORE_POINTER.get(po.getMONITOR_ID());
		pointorMap = pathPonitor.get(path);
		if (pointorMap == null) {
			pointorMap = new HashMap<String, List<Object>>();
			pathPonitor.put(path, pointorMap);
		}
		File f = new File(path);
		final List<String> list = new ArrayList<String>();
		f.list(new FilenameFilter() {
			@Override
			public boolean accept(File dir, String name) {
				File file = new File(dir, name);
				if (file.isFile() && checkMonitor(path, name)) {
					list.add(name);
				}
				return false;// 返回false，标示被过滤掉
			}
		});

		for (String s : list) {
			change(s, s);
			savePointor();
		}

	}

	public void stop() {
		savePointor();
	}

	public boolean checkMonitor(String path, String name) {
		if (this.path.equals(path)) {
			Matcher matcher = Pattern.compile(nameRegex).matcher(name);
			return matcher.matches();
		}
		return false;
	}

	// 按拆分符号拆分
	private void readBySplit(List<Object> pointors, File f, String splitCh, boolean delBlank, String splitAppend)
			throws Exception {
		long size = f.length();
		UnicodeInputStream uis = null;
		InputStreamReader isr = null;
		BufferedReader br = null;
		long offset = Convert.toLong(pointors.get(1));
		long lineNumber = Convert.toLong(pointors.get(2));
		try {
			uis = new UnicodeInputStream(new FileInputStream(f), Charset.defaultCharset().name());
			size -= uis.headSize();// 大小减去 头大小
			if (offset == -1) {
				offset = size;
				pointors.set(1, offset);
			}
			if (offset == size) {
				return;
			}
			if (offset > size) {// 文件重写
				offset = 0;
				pointors.set(1, 0);
			}
			uis.skip(offset);
			if (splitCh.equals("\n") || splitCh.equals("\r\n")) {// 直接换行符
				br = new BufferedReader(new InputStreamReader(uis));
				String str = null;
				while ((str = br.readLine()) != null) {
					long s = (str + "\n").getBytes().length;
					if (delBlank) {
						str = str.trim();
					}
					parseFieldsAndSend(str, f.getName(), offset, lineNumber);
					offset += s;
					lineNumber++;
					pointors.set(1, offset);
					pointors.set(2, lineNumber);
				}
			} else {
				isr = new InputStreamReader(uis);
				long len = size - offset;
				int lds = splitCh.length();
				int chbs = splitCh.getBytes().length;
				len = (len <= 1024 * 64 ? len : 1024 * 4);// 如果整个未读取内容不超过64k（因为中文字符占2b关系，可能不准），一次读完，否则一次读4k
				char[] chs = new char[lds];// 依次存放读取的拆分符那么长的字符
				char[] laveChs = null;// 每次读取对比剩余的字符
				StringBuilder sb = new StringBuilder();
				int rl;// 读取出的字符数量
				int chl_ = 0;// 匹配后，下次匹配需要过滤的字符标记
				while (offset < size) {
					char[] cb = new char[(int) len];
					rl = isr.read(cb);
					if (rl == -1) {
						// 读完了，处理缓冲区剩余的字符
						if (laveChs != null) {
							char[] lave = new char[laveChs.length - chl_];
							System.arraycopy(laveChs, chl_, lave, 0, lave.length);
							sb.append(lave);
						}
						if (sb.length() > 0) {
							// 一条记录
							String data = sb.toString();
							if (data.endsWith("\n")) {
								int s = data.getBytes().length;
								if ("START".equals(splitAppend)) {
									data = splitCh + data;
								} else if ("END".equals(splitAppend)) {
									data += splitCh;
								}
								int idx = -1;
								int ln = 0;
								while ((idx = data.indexOf("\n", idx + 1)) != -1) {
									ln++;
								}
								if (delBlank) {
									data = data.trim();
								}
								parseFieldsAndSend(data, f.getName(), offset, lineNumber);
								offset += s;
								lineNumber += ln;
								pointors.set(1, offset);
								pointors.set(2, lineNumber);
								sb.delete(0, sb.length());
							}
						}
						break;
					}
					int laveLen = laveChs != null ? laveChs.length : 0;
					if (rl < cb.length) {
						// 末次，取出其中有效字符和缓冲区剩余字符
						char[] nch = new char[rl + laveLen];
						if (laveLen > 0) {
							System.arraycopy(laveChs, 0, nch, 0, laveLen);
						}
						System.arraycopy(cb, 0, nch, laveLen, rl);
						cb = nch;
						laveChs = null;
						rl = cb.length;
					} else {
						// 将上次缓冲剩余的字符放到字符前端
						if (laveLen > 0) {
							char[] nch = new char[cb.length + laveLen];
							System.arraycopy(laveChs, 0, nch, 0, laveLen);
							System.arraycopy(cb, 0, nch, laveLen, cb.length);
							cb = nch;
							laveChs = null;
							rl = cb.length;
						}
					}

					for (int x = 0; x < rl;) {
						if (rl - x >= lds) {
							// 依次取出末尾与分割符等长的字符
							System.arraycopy(cb, x, chs, 0, lds);
							String lastCh = new String(chs);
							if (splitCh.equals(lastCh)) {
								// 一条记录
								String data = sb.toString();
								int s = data.getBytes().length + chbs;
								if ("START".equals(splitAppend)) {
									data = splitCh + data;
								} else if ("END".equals(splitAppend)) {
									data += splitCh;
								}
								int idx = -1;
								int ln = 0;
								while ((idx = data.indexOf("\n", idx + 1)) != -1) {
									ln++;
								}
								if (delBlank) {
									data = data.trim();
								}
								parseFieldsAndSend(data, f.getName(), offset, lineNumber);
								offset += s;
								lineNumber += ln;
								pointors.set(1, offset);
								pointors.set(2, lineNumber);
								sb.delete(0, sb.length());
								chl_ = lds - 1;
							} else if (chl_ == 0) {
								sb.append(chs[0]);
							} else if (chl_ > 0) {
								chl_--;
							}
							x++;
						} else {
							laveChs = new char[rl - x];
							System.arraycopy(cb, x, laveChs, 0, laveChs.length);
							x += laveChs.length;
							if (chl_ > 0) {
								chl_ -= (rl - x);
							}
						}
					}

				}
			}
		} finally {
			if (br != null)
				br.close();
			if (isr != null)
				isr.close();
			if (uis != null)
				uis.close();
		}
	}

	// 按起始符拆分
	private void readByStart(List<Object> pointors, File f, String startCh, boolean delBlank) throws Exception {
		long size = f.length();
		UnicodeInputStream uis = null;
		InputStreamReader isr = null;
		long offset = Convert.toLong(pointors.get(1));
		long lineNumber = Convert.toLong(pointors.get(2));
		try {
			uis = new UnicodeInputStream(new FileInputStream(f), Charset.defaultCharset().name());
			size -= uis.headSize();// 大小减去 头大小
			if (offset == -1) {
				offset = size;
				pointors.set(1, offset);
			}
			if (offset == size) {
				return;
			}
			uis.skip(offset);
			isr = new InputStreamReader(uis);
			long len = size - offset;
			len = (len <= 1024 * 64 ? len : 1024 * 4);// 如果整个未读取内容不超过64k（因为中文字符占2b关系，可能不准），一次读完，否则一次读4k
			int rl;
			StringBuilder sb = new StringBuilder();
			while (offset < size) {
				char[] cb = new char[(int) len];
				rl = isr.read(cb);
				if (rl == -1) {
					// 已读完
					if (sb.length() > 0) {
						// 一条记录
						String data = sb.toString();
						int s = data.getBytes().length;
						int idx = -1;
						int ln = 0;
						while ((idx = data.indexOf("\n", idx + 1)) != -1) {
							ln++;
						}
						if (delBlank) {
							data = data.trim();
						}
						parseFieldsAndSend(data, f.getName(), offset, lineNumber);
						offset += s;
						lineNumber += ln;
						pointors.set(1, offset);
						pointors.set(2, lineNumber);
						sb.delete(0, sb.length());
					}
					break;
				}
				if (rl < cb.length) {
					// 末次
					char[] nch = new char[rl];
					System.arraycopy(cb, 0, nch, 0, rl);
					cb = nch;
				}

				String str = new String(cb);
				Matcher matcher = Pattern.compile(getStartChRegex(startCh)).matcher(str);
				int index = 0;
				while (matcher.find()) {
					if (matcher.start() > index) {
						// 前半截需要追加到上次
						sb.append(str.substring(index, matcher.start()));

						// 一条记录
						String data = sb.toString();
						int s = data.getBytes().length;
						int idx = -1;
						int ln = 0;
						while ((idx = data.indexOf("\n", idx + 1)) != -1) {
							ln++;
						}
						if (delBlank) {
							data = data.trim();
						}
						parseFieldsAndSend(data, f.getName(), offset, lineNumber);
						offset += s;
						lineNumber += ln;
						pointors.set(1, offset);
						pointors.set(2, lineNumber);
						sb.delete(0, sb.length());
					}
					sb.append(matcher.group());
					index = matcher.end();
				}
				if (index == 0) {
					sb.append(str);// 整个字符都属于上次
				} else {
					sb.append(str.substring(index, str.length()));
				}
			}

		} finally {
			if (isr != null)
				isr.close();
			if (uis != null)
				uis.close();
		}
	}

	private String startChRegex; // 起始符，替换过宏变量后的结果,可用于正则匹配

	private String getStartChRegex(String startCh) {
		if (startChRegex == null) {
			String regex = "\\$\\{(\\w+)\\}";
			startChRegex = StringUtils.replaceAll(startCh, regex, new StringUtils.ReplaceCall() {
				@Override
				public String replaceCall(String... groupStrs) {
					String d = groupStrs[1].toLowerCase();
					if ("yy".equals(d)) {
						return "(\\\\d{2})";// 2位年
					} else if ("yyyy".equals(d)) {
						return "(\\\\d{4})";// 4位年
					} else if ("mm".equals(d)) {
						return "(0?[1-9]|1[0-2])";// 月
					} else if ("dd".equals(d)) {
						return "(0?[1-9]|[12]\\\\d|3[0-1])";// 日
					} else if ("h".equals(d)) {
						return "([0-1]\\\\d|2[0-3])";// 时
					} else if ("m".equals(d)) {
						return "([0-5]\\\\d)";// 分
					} else if ("s".equals(d)) {
						return "([0-5]\\\\d)";// 秒
					} else if ("sss".equals(d)) {
						return "(\\\\d{3})";// 毫秒
					} else if ("mmm".equals(d)) {
						return "(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sept|Oct|Nov|Dec)";// 英文月
					} else if ("eee".equals(d)) {
						return "(Mon|Tues|Wed|Thurs|Fri|Sat|Sun)";// 英文星期
					} else if ("z".equals(d)) {
						return "(CST|GMT)";// 时间类型（时区）
					}
					return groupStrs[0];
				}
			});
			LogUtils.debug(po.toString() + " 消息头匹配规则:    " + startChRegex);
		}
		return startChRegex;
	}

	// 解析字段信息并发送
	private void parseFieldsAndSend(String content, String fileName, long offset, long lineNumber) throws Exception {
		if (content == null || "".equals(content)) {
			return;
		}
		Map<String, Object> fieldRule = po.getFieldRule();
		Map<String, Object> formatRule = po.getFormatRule();
		int num = Convert.toInt(fieldRule.get(MonitorPO.FIELD_NUM_KEY));
		String splitCh = Convert.toString(fieldRule.get(MonitorPO.SPLIT_KEY));
		String[] arr = content.split(splitCh);
		final Object[] tuples = new Object[num];
		try {
			for (int i = 0; i < num; i++) {
				String rule = Convert.toString(fieldRule.get("" + i));
				if (rule.startsWith("idx:")) {// 值来自拆分符拆分数组
					rule = rule.substring(4);
					int idx;
					if ((idx = rule.indexOf(",")) != -1) {// 需要二次处理
						String ra = rule.substring(0, idx);// 可能是 idx or
															// idx1-idx3 or
															// idx-?
						int idx_ = ra.indexOf("-");
						String v;
						if (idx_ != -1) {
							int s = Convert.toInt(ra.substring(0, idx_));
							int e = Convert.toInt(ra.substring(idx_ + 1), arr.length - 1);
							String str = arr[s];
							for (int x = s + 1; x <= e; x++) {
								str += splitCh + arr[x];
							}
							v = str;
						} else {
							v = arr[Convert.toInt(rule.substring(0, idx))];// 取一次拆分后的值
																			// idx,{rule}
						}
						rule = rule.substring(idx + 1);// type:{rule}
						if (rule.startsWith("split:")) {// 二次拆分
							rule = rule.substring(6);// idx:spch
							idx = rule.indexOf(":");
							int idx2 = Convert.toInt(rule.substring(0, idx));// 二次拆分后的索引
							rule = rule.substring(idx + 1);// 二次拆分符
							String[] aaa = v.split(rule);
							tuples[i] = aaa[idx2];
						} else if (rule.startsWith("substr_s:")) {// 二次提取前半截
							rule = rule.substring(9);// 提取符
							if ((idx = v.indexOf(rule)) != -1) {
								tuples[i] = v.substring(0, idx);
							} else {
								tuples[i] = "";
							}
						} else if (rule.startsWith("substr_e:")) {// 二次提取后半截
							rule = rule.substring(9);// 提取符
							if ((idx = v.indexOf(rule)) != -1) {
								tuples[i] = v.substring(idx + 1);
							} else {
								tuples[i] = "";
							}
						}
					} else if ((idx = rule.indexOf("-")) != -1) {// 范围
						int s = Convert.toInt(rule.substring(0, idx));
						int e = Convert.toInt(rule.substring(idx + 1), arr.length - 1);
						String str = arr[s];
						for (int x = s + 1; x <= e; x++) {
							str += splitCh + arr[x];
						}
						tuples[i] = str;
					} else {
						tuples[i] = arr[Convert.toInt(rule)];
					}
				} else if (rule.startsWith("mac:")) {// 值来自宏变量
					rule = rule.substring(4).toUpperCase();
					if (rule.equals(MAC_HOST)) {
						tuples[i] = PropConfig.getHostName();
					} else if (rule.equals(MAC_FILE_NAME)) {
						tuples[i] = fileName;
					} else if (rule.equals(MAC_LINE_OFFSET)) {
						tuples[i] = offset;
					} else if (rule.equals(MAC_LINE_NUMBER)) {
						tuples[i] = lineNumber;
					} else if (rule.equals(MAC_SEND_TIME)) {
						tuples[i] = System.currentTimeMillis();
					} else if (rule.equals(MAC_MONITOR_ID)) {
						tuples[i] = po.getMONITOR_ID();
					}
				} else if (rule.startsWith("reg,")) {// 值通过正则表达式匹配得出
					rule = rule.substring(4);
					int idx = rule.indexOf(":");
					String regex = rule.substring(idx + 1);// 匹配串
					idx = Convert.toInt(rule.substring(0, idx));// 取matcher.group(idx)
					Matcher matcher = Pattern.compile(regex).matcher(content);
					if (matcher.find()) {
						tuples[i] = Convert.toString(matcher.group(idx));
					}
				} else {
					tuples[i] = "";
				}

				if (tuples[i] != null && formatRule.containsKey(i + "")) {
					Map<String, Object> r = (Map<String, Object>) (formatRule.get(i + ""));
					String type = Convert.toString(r.get("TYPE"));
					if (type.equals("TIME_LONG")) {
						String srcrmt = Convert.toString(r.get("SRC_FMT"), "yyyy-MM-dd HH:mm:ss");
						Date d = Convert.toTime(tuples[i].toString(), srcrmt);
						tuples[i] = d.getTime();
					} else if (type.startsWith("TIME_STR")) {
						String srcrmt = Convert.toString(r.get("SRC_FMT"), "yyyy-MM-dd HH:mm:ss");
						Date d = Convert.toTime(tuples[i].toString(), srcrmt);
						String fmt = type.substring(9, type.length() - 1);
						DateFormat df = new SimpleDateFormat(fmt);
						tuples[i] = df.format(d);
					} else if (type.equals("NUMBER")) {
						tuples[i] = Convert.toLong(tuples[i]);
					} else if (type.startsWith("NUMBER(")) {
						tuples[i] = Convert.toDouble(tuples[i]);
					}
				}

			}

			// 过滤规则
			if (po.getSEND_FILTER_RULE() != null && !"".equals(po.getSEND_FILTER_RULE())) {
				Map<String, Object> filterRule = po.getSendFilterRule();
				String expr = Convert.toString(filterRule.get(MonitorPO.EXPR_KEY));
				final Map<String, Object> val = new HashMap<String, Object>();
				expr = StringUtils.replaceAll(expr, "\\{(\\w+)\\}", new StringUtils.ReplaceCall() {
					@Override
					public String replaceCall(String... groupStrs) {
						String str = groupStrs[1].toUpperCase();
						val.put(str, true);
						return str;
					}
				});
				for (String k : val.keySet()) {
					String s = Convert.toString(fieldRule.get(k));
					if (k.startsWith("C_")) {
						val.put(k, content.contains(s));
					} else if (k.startsWith("NC_")) {
						val.put(k, !content.contains(s));
					} else if (k.startsWith("F_")) {
						final Map<String, Object> subVal = new HashMap<String, Object>();
						s = StringUtils.replaceAll(s, "\\{(\\d+)\\}", new StringUtils.ReplaceCall() {
							@Override
							public String replaceCall(String... groupStrs) {
								String str = Convert.toString(tuples[Convert.toInt(groupStrs[1])]);
								if (StringUtils.isNumeric(str)) {
									subVal.put("MAC_" + groupStrs[1], Convert.toLong(str));
								} else if (StringUtils.isNumeric(str.replace(".", ""))) {
									// 将第一个点(小数点)去掉后看是否是数字，是则说明是double
									subVal.put("MAC_" + groupStrs[1], Convert.toDouble(str));
								} else {
									subVal.put("MAC_" + groupStrs[1], str);
								}
								return "MAC_" + groupStrs[1];
							}
						});
						int idx = 0;// 如果匹配到idx>8，则说明匹配到的是内容，而不是字段变量。【MAC_11 in】
						if ((idx = s.indexOf(" in ")) != -1 && idx < 8) {
							val.put(k, s.substring(idx + 4).contains(subVal.get(s.substring(0, idx)).toString()));
						} else if ((idx = s.indexOf(" start ")) != -1 && idx < 8) {
							val.put(k, s.substring(idx + 7).startsWith(subVal.get(s.substring(0, idx)).toString()));
						} else if ((idx = s.indexOf(" end ")) != -1 && idx < 8) {
							val.put(k, s.substring(idx + 5).endsWith(subVal.get(s.substring(0, idx)).toString()));
						} else {
							// 计算子表达式值
							val.put(k, Convert.toBool(AviatorEvaluator.execute(s, subVal)));
						}
					}
				}

				// 计算整个表达式
				if (!Convert.toBool(AviatorEvaluator.execute(expr, val))) {
					LogUtils.debug("被过滤->" + po.toString() + content);
					return;
				}
			}
		} catch (Exception e) {
			if (Convert.toBool(fieldRule.get(MonitorPO.IGNORE_ERROR_KEY), false)) {
				LogUtils.warn("解析字段数据出错(已忽略),FILE:" + fileName + ",offset:" + offset + ",lineNumber:" + lineNumber +
						"\n    CONTENT>>>:" + content);
				return;
			}
			throw e;
		}
		String ret = sender.sendData(po.getSEND_TOPIC(), tuples);
		if (ret == null || "ok".equals(ret) || ret.endsWith(",ok")) {
			LogUtils.debug(po.toString() + " send ok!");
		} else {
			LogUtils.warn(po.toString() + " send fail->" + ret + "\n    MSG>>>:" + StringUtils.join(tuples, " | "));
		}
	}

	private void read(List<Object> pointors, File f) throws Exception {
		String splitCh = Convert.toString(po.getLineRule().get(MonitorPO.SPLIT_KEY), null);// 拆分符
		String startCh = Convert.toString(po.getLineRule().get(MonitorPO.START_KEY), null);// 起始匹配符
		boolean delBlank = Convert.toBool(po.getLineRule().get(MonitorPO.DEL_BLANK_KEY), true);// 是否清除首尾空白符
		if (splitCh != null && !"".equals(splitCh)) {
			// 按拆分符拆分
			String splitAppend = Convert.toString(po.getLineRule().get(MonitorPO.SPLIT_APPEND_KEY), "NONE");// 需要将拆分符追加
			readBySplit(pointors, f, splitCh, delBlank, splitAppend);
		} else if (startCh != null && !"".equals(startCh)) {// 通过消息头正则表达式匹配
			readByStart(pointors, f, startCh, delBlank);
		} else {
			throw new RuntimeException("未配置行识别规则，无法解析!");
		}
	}

	public void change(final String name, final String oldName) {
		Boolean readed = lockMap.get(oldName);
		if (readed == null) {
			synchronized (lockMap) {
				lockMap.put(oldName, false);
			}
			readed = lockMap.get(oldName);
		}
		synchronized (readed) {
			if (readed) {
				return;
			} else {
				readed = true;
				lockMap.put(oldName, readed);
			}
		}
		String fileName;
		if (path.endsWith("/")) {
			fileName = path + name;
		} else {
			fileName = path + "/" + name;
		}
		List<Object> pointors = pointorMap.get(oldName);
		if (pointors == null) {
			pointors = new ArrayList<Object>() {
				{
					add(0l);
					add(Convert.toLong(po.getINIT_OFFSET(), -1));
					add(0l);
				}
			};
			pointorMap.put(oldName, pointors);
		}
		long lastModifyTime = Convert.toLong(pointors.get(0));
		try {
			File f = new File(fileName);
			if (lastModifyTime < f.lastModified()) {
				lastModifyTime = f.lastModified();
				pointors.set(0, lastModifyTime);
			} else {
				return;
			}
			read(pointors, f);// 读取内容
			change(name, oldName);// 读完后再次尝试，因为可能在在它读取过程中，文件内容又发生改变
		} catch (Exception e) {
			LogUtils.error("读取[" + name + "]出错:" + e.getMessage(), e);
		} finally {
			synchronized (readed) {
				readed = false;
				lockMap.put(oldName, readed);
			}
		}
	}

	// 文件删除触发事件
	public void delete(String name) {
		pointorMap.remove(name);
	}

	// 保存采集点
	public synchronized void savePointor() {
		MappedByteBuffer mbb = FileMonitor.STORE_MAP.get(po.getMONITOR_ID());
		mbb.position(0);
		String str = JSON.toJSONString(FileMonitor.STORE_POINTER.get(po.getMONITOR_ID()));
		byte[] b = str.getBytes();
		mbb.put(Convert.toBytes(b.length));
		mbb.position(4);
		mbb.put(b);
		mbb.force();
	}

	public void setOffset(String name, long offset) {
		List<Object> pointors = pointorMap.get(name);
		if (pointors == null) {
			pointors = new ArrayList<Object>() {
				{
					add(0l);
					add("END".equals(po.getINIT_OFFSET()) ? -1 : 0);
					add(0l);
				}
			};
			pointorMap.put(name, pointors);
		}
		pointors.set(1, offset);
		if (offset == 0) {
			pointors.set(2, 0);
		}
	}

	public String getPath() {
		return path;
	}

	public String getNameRegex() {
		return nameRegex;
	}

	public MonitorPO getPo() {
		return po;
	}
}
