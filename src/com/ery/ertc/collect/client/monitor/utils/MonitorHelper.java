package com.ery.ertc.collect.client.monitor.utils;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.ery.ertc.collect.client.monitor.LogParser;
import com.ery.ertc.collect.client.monitor.podo.MonitorPO;
import com.ery.base.support.utils.StringUtils;

public class MonitorHelper {

	public static Map<String, List<LogParser>> watchMonitorFiles(List<MonitorPO> pos) {
		Map<String, List<LogParser>> map = new HashMap<String, List<LogParser>>();
		for (MonitorPO po : pos) {
			String path = po.getMONITOR_FILE();
			String paths[] = path.split(",");// 多路径配置
			for (String string : paths) {
				path = string.replaceAll("//", "/");// 双斜杠换单的
				path = path.replaceAll("\\*", ".*");// 星号通配符换 正则表达式模式
				String[] arr = path.split("/");// 按目录隔断,递归搜索文件
				searchFiles(0, arr, "/", map, po);
			}
		}
		return map;
	}

	private static void searchFiles(int lv, String[] paths, String parentPath, Map<String, List<LogParser>> map,
			MonitorPO po) {
		String regex = "\\$\\{(\\w*)\\}";
		for (int i = lv; i < paths.length; i++) {
			String name = paths[i];
			if (name.matches(regex) || name.contains(".*")) {// 发现带宏变量或通配符
				name = StringUtils.replaceAll(name, regex, new StringUtils.ReplaceCall() {
					@Override
					public String replaceCall(String... groupStrs) {
						String d = groupStrs[1].toLowerCase();
						if ("yy".equals(d)) {
							return "(\\\\d{2})";
						} else if ("yyyy".equals(d)) {
							return "(\\\\d{4})";
						} else if ("mm".equals(d)) {
							return "(0?[1-9]|1[0-2])";
						} else if ("dd".equals(d)) {
							return "(0?[1-9]|[12]\\\\d|3[0-1])";
						} else if ("h".equals(d)) {
							return "([0-1]\\\\d|2[0-3])";
						} else if ("m".equals(d)) {
							return "([0-5]\\\\d)";
						} else if ("s".equals(d)) {
							return "([0-5]\\\\d)";
						}
						return groupStrs[0];
					}
				});
				if (i != paths.length - 1) {
					File pf = new File(parentPath);
					if (pf.exists()) {
						File[] fs = pf.listFiles();
						for (File f : fs) {
							Matcher matcher = Pattern.compile(name).matcher(f.getName());
							if (matcher.matches() && f.isDirectory()) {
								if (parentPath.endsWith(File.separator)) {
									searchFiles(i + 1, paths, parentPath + f.getName(), map, po);
								} else {
									searchFiles(i + 1, paths, parentPath + File.separator + f.getName(), map, po);
								}
							}
						}
					}
				} else {
					if (new File(parentPath).exists()) {
						// 复合条件。将倒数第二级目录加入监控
						List<LogParser> lgps = map.get(parentPath);
						if (lgps == null) {
							lgps = new ArrayList<LogParser>();
							map.put(parentPath, lgps);
						}
						lgps.add(new LogParser(parentPath, name, po));
					}
				}
			} else {
				if (i != paths.length - 1) {
					if (parentPath.endsWith(File.separator)) {
						parentPath += name;
					} else {
						parentPath += File.separator + name;
					}
				} else {
					if (new File(parentPath).exists()) { // 复合条件。将倒数第二级目录加入监控
						List<LogParser> lgps = map.get(parentPath);
						if (lgps == null) {
							lgps = new ArrayList<LogParser>();
							map.put(parentPath, lgps);
						}
						lgps.add(new LogParser(parentPath, name, po));
					}
				}
			}
		}

	}

}
