package com.ery.ertc.collect.client;

import com.ery.base.support.sys.SystemVariable;
import com.ery.base.support.utils.RemotingUtils;

public class PropConfig extends SystemVariable {

	public final static String SERVER_SUPPORT_PLUGINS_KEY = "server.support.plugins";

	public final static String HOST_NAME_KEY = "host.name";
	public final static String COLLECT_TMP_DIR_KEY = "collect.tmp.dir";
	public final static String MONITOR_IDS_KEY = "monitor.ids";

	public static String getCollectTmpDir() {
		String dir = conf.getProperty(MONITOR_IDS_KEY, System.getProperty("user.dir"));
		if (dir.equals("")) {
			return System.getProperty("user.dir");
		}
		return dir;
	}

	public static String getMonitorIdsKey() {
		return conf.getProperty(MONITOR_IDS_KEY, "1");
	}

	public static String getHostName() {
		String hostName = conf.getProperty(HOST_NAME_KEY);
		if (hostName == null || "".equals(hostName)) {
			hostName = RemotingUtils.getLocalAddress();
			conf.put(HOST_NAME_KEY, hostName);
		}
		return hostName;
	}

}
