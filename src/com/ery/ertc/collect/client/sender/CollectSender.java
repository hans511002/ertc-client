package com.ery.ertc.collect.client.sender;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;

import com.alibaba.fastjson.JSON;
import com.ery.ertc.collect.client.PropConfig;
import com.ery.ertc.collect.client.monitor.podo.MonitorPO;
import com.ery.base.support.log4j.LogUtils;
import com.ery.base.support.utils.Convert;
import com.ery.base.support.utils.StringUtils;
import com.ery.base.support.utils.Utils;

public abstract class CollectSender extends Sender {
	public final static String SERVER_ZK_URL_KEY = "collect.zk.url";
	public final static String SERVER_ZK_CONNECT_TMOS_KEY = "collect.zk.connect.timeout.ms";
	public final static String SERVER_ZK_SESSION_TMOS_KEY = "collect.zk.session.timeout.ms";
	public final static String SERVER_ZK_NODE_PATH_KEY = "collect.zk.node.path";

	private static List<String> httpUrls = new ArrayList<String>();
	private static List<String> socketUrls = new ArrayList<String>();
	private static List<String> wsUrls = new ArrayList<String>();
	private static Map<String, AtomicLong> topicHttpNum = new HashMap<String, AtomicLong>();
	private static Map<String, AtomicLong> topicSocketNum = new HashMap<String, AtomicLong>();
	private static Map<String, AtomicLong> topicWsNum = new HashMap<String, AtomicLong>();

	public void init(MonitorPO m) {
	}

	public static void addUrl(String url, Object http, Object socket, Object ws) {
		if (!httpUrls.contains(url) && http != null && StringUtils.isNumeric(http.toString()))
			httpUrls.add(url);
		if (!socketUrls.contains(url) && socket != null && StringUtils.isNumeric(socket.toString()))
			socketUrls.add(url);
		if (!wsUrls.contains(url) && ws != null && StringUtils.isNumeric(ws.toString()))
			wsUrls.add(url);
	}

	public static void deleteUrl(String url) {
		httpUrls.remove(url);
		socketUrls.remove(url);
		wsUrls.remove(url);
	}

	public static String getUrl(String topic, SendType sendType) {
		return getUrl(topic, sendType, null);
	}

	public static String getUrl(String topic, SendType sendType, Set<String> exUrl) {
		List<String> url = null;
		AtomicLong len = null;
		if (sendType.equals(SendType.WS)) {
			url = wsUrls;
			synchronized (topicWsNum) {
				if (!topicWsNum.containsKey(topic)) {
					topicWsNum.put(topic, new AtomicLong(1));
				}
			}
			len = topicWsNum.get(topic);
		} else if (sendType.equals(SendType.HTTP)) {
			url = httpUrls;
			synchronized (topicHttpNum) {
				if (!topicHttpNum.containsKey(topic)) {
					topicHttpNum.put(topic, new AtomicLong(1));
				}
			}
			len = topicHttpNum.get(topic);
		} else if (sendType.equals(SendType.SOCKET)) {
			url = socketUrls;
			synchronized (topicSocketNum) {
				if (!topicSocketNum.containsKey(topic)) {
					topicSocketNum.put(topic, new AtomicLong(1));
				}
			}
			len = topicSocketNum.get(topic);
		}
		synchronized (len) {
			if (len.get() > 0xFFFFFFFFFFFFFFFl) {
				len.set(1);
			} else {
				len.incrementAndGet();
			}
		}
		if (exUrl != null && exUrl.size() > 0) {
			url = new ArrayList<String>(url);
			url.removeAll(exUrl);
		}
		if (url.size() == 0) {
			return null;
		} else {
			return url.get((int) (len.get() % url.size()));
		}
	}

	public enum SendType {
		WS, HTTP, SOCKET
	}

	private static boolean inited = false;
	private static ZkClient zkClient;
	private static Map<String, String> serverNodes = new HashMap<String, String>();
	private static Map<String, int[]> serverPort = new HashMap<String, int[]>();
	private static String path = "";

	public synchronized static void init() {
		if (!inited) {
			String zkUrl = getServerZkUrl();
			if (zkUrl == null || "".equals(zkUrl)) {
				throw new RuntimeException("参数[collect.zk.url] 未配置 !");
			}
			zkClient = new ZkClient(zkUrl, getServerZkSessionTMOS(), getServerZkConnectTMOS(), new ZkSerializer() {
				@Override
				public byte[] serialize(Object o) throws ZkMarshallingError {
					return o.toString().getBytes();
				}

				@Override
				public Object deserialize(byte[] bytes) throws ZkMarshallingError {
					return new String(bytes);
				}
			});
			// 查询出有哪些节点
			if (!getServerZkNodePath().startsWith("/")) {
				path = "/" + getServerZkNodePath() + "/hosts";
			} else {
				path = getServerZkNodePath() + "/hosts";
			}
			List<String> nodes = zkClient.getChildren(path);
			for (String nn : nodes) {
				try {
					readNodeInfo(nn);
				} catch (Exception e) {
					System.out.println("获取节点URL出错!");
				}
			}

			// 监听节点变化
			zkClient.subscribeChildChanges(path, new IZkChildListener() {
				@Override
				public void handleChildChange(String s, List<String> strings) throws Exception {
					if (strings != null) {
						changeNode(strings);
						LogUtils.debug("节点变更:" + s + ":" + strings);
					} else {
						LogUtils.info("监控节点被删除:" + s);
					}
				}
			});
			inited = true;
		}
	}

	// 获取节点数据
	private synchronized static void changeNode(List<String> strings) {
		// 原来有，现在无，标示删除
		for (Iterator<Map.Entry<String, String>> itmap = serverNodes.entrySet().iterator(); itmap.hasNext();) {
			Map.Entry<String, String> nm = itmap.next();
			if (!strings.contains(nm.getKey())) {
				String hostName = nm.getValue();
				CollectSender.deleteUrl(hostName);
				serverPort.remove(hostName);
				itmap.remove();// 节点被删除
			}
		}

		// 加上新节点URL
		for (final String nn : strings) {
			try {
				if (!readNodeInfo(nn)) {
					LogUtils.warn("节点[" + nn + "]数据为空!1秒后,再试!");
					new Thread() {
						public void run() {
							Utils.sleep(1000);
							if (!readNodeInfo(nn)) {
								LogUtils.error("节点[" + nn + "]数据为空!", new IllegalArgumentException());
							} else {
								LogUtils.info("节点[" + nn + "]数据已刷新!");
							}
						}
					}.start();
				} else {
					LogUtils.info("节点[" + nn + "]数据已刷新!");
				}
			} catch (NullPointerException e) {
				LogUtils.warn("获取节点[" + nn + "]URL出错!1秒后,再试!");
				new Thread() {
					public void run() {
						Utils.sleep(1000);
						try {
							if (!readNodeInfo(nn)) {
								LogUtils.error("节点[" + nn + "]数据为空!", new IllegalArgumentException());
							} else {
								LogUtils.info("节点[" + nn + "]数据已刷新!");
							}
						} catch (Exception ex) {
							LogUtils.error("获取节点[" + nn + "]URL出错!", ex);
						}
					}
				}.start();
			} catch (Exception e) {
				LogUtils.error("获取节点[" + nn + "]URL出错!", e);
			}
		}
	}

	private static boolean readNodeInfo(String nodeId) {
		Object o = zkClient.readData(path + "/" + nodeId);
		Map<String, Object> clusterNodeInfo = JSON.parseObject(o.toString());
		if (clusterNodeInfo != null) {
			String hostName = clusterNodeInfo.get("hostName").toString();
			int http = Convert.toInt(clusterNodeInfo.get("httpPort"), 0);
			int socket = Convert.toInt(clusterNodeInfo.get("socketPort"), 0);
			int ws = Convert.toInt(clusterNodeInfo.get("wsPort"), 0);
			serverPort.put(hostName, new int[] { http, socket, ws });
			serverNodes.put(nodeId, hostName);
			CollectSender.addUrl(hostName, http, socket, ws);
			return true;
		}
		return false;
	}

	public static int getPort(String hostName, SendType sendType) {
		int[] arr = serverPort.get(hostName);
		if (arr != null) {
			if (sendType.equals(SendType.WS)) {
				return arr[2];
			} else if (sendType.equals(SendType.HTTP)) {
				return arr[0];
			} else if (sendType.equals(SendType.SOCKET)) {
				return arr[1];
			}
		}
		return 0;
	}

	public static String getServerZkUrl() {
		return PropConfig.getConf().getProperty(SERVER_ZK_URL_KEY);
	}

	public static int getServerZkConnectTMOS() {
		return Convert.toInt(PropConfig.getConf().getProperty(SERVER_ZK_CONNECT_TMOS_KEY), 30000);
	}

	public static int getServerZkSessionTMOS() {
		return Convert.toInt(PropConfig.getConf().getProperty(SERVER_ZK_SESSION_TMOS_KEY), 8000);
	}

	public static String getServerZkNodePath() {
		return PropConfig.getConf().getProperty(SERVER_ZK_NODE_PATH_KEY);
	}

}
