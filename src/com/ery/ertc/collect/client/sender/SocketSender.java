package com.ery.ertc.collect.client.sender;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.ery.ertc.collect.client.utils.MsgUtils;
import com.ery.base.support.log4j.LogUtils;
import com.ery.base.support.utils.Convert;
import com.ery.base.support.utils.TcpChannel;

public class SocketSender extends CollectSender {

	private static final int timeout = 10000;
	private boolean isAsync = true;// 0发送后，等待结果，1流式异步发送
	private boolean isBinary = true;// 0发送后，等待结果，1流式异步发送

	public void setIsAsync(boolean isAsync) {
		this.isAsync = isAsync;
	}

	public SocketSender() {
		CollectSender.init();
	}

	@Override
	public String sendData(String topic, List<String> contents) {
		for (String content : contents) {
			sendData(topic, content);
		}
		if (!isAsync) {
			return "ok";
		}
		// ////////////////////////等待返回
		return "";
	}

	public String sendData(String topic, String content) {
		String url = getUrl(topic, SendType.SOCKET);
		if (url == null)
			throw new RuntimeException("未找到可用服务端连接!");
		try {
			WRSocket wrSocket = send(topic, content, url, 0);
			if (wrSocket == null) {
				throw new RuntimeException("连接服务器[" + url + ":" + getPort(url, SendType.SOCKET) + "]不成功!");
			}
			wrSocket.addTotal();
			if (!isAsync) {
				return wrSocket.readOne();// 非持续流
			} else {
				wrSocket.start();
			}
			return null;
		} catch (Exception e) {
			clearUrl(url);
			throw new RuntimeException("请求[" + url + "]消息不成功!", e);
		} finally {
			if (!isAsync) {
				clearUrl(url);
			}
		}
	}

	public String sendData(String topic, ArrayList<Object[]> contents) {
		for (Object[] content : contents) {
			sendData(topic, content);
		}
		if (!isAsync) {
			return "ok";
		}
		// ////////////////////////等待返回
		return "";
	}

	public String sendData(String topic, Object... objs) {
		String url = getUrl(topic, SendType.SOCKET);
		if (url == null)
			throw new RuntimeException("未找到可用服务端连接!");
		try {
			WRSocket wrSocket = send(topic, objs, url, 0);
			if (wrSocket == null) {
				throw new RuntimeException("连接服务器[" + url + ":" + getPort(url, SendType.SOCKET) + "]不成功!");
			}
			wrSocket.addTotal();
			if (!isAsync) {
				return wrSocket.readOne();// 非持续流
			} else {
				wrSocket.start();
			}
			return null;
		} catch (Exception e) {
			clearUrl(url);
			throw new RuntimeException("请求[" + url + "]消息不成功!", e);
		} finally {
			if (!isAsync) {
				clearUrl(url);
			}
		}
	}

	private WRSocket send(String topic, String content, String url, int num) throws Exception {
		WRSocket wrSocket = getTcpChannelMap(url);
		if (wrSocket != null) {
			try {
				String sendStr = topic + "," + content;
				wrSocket.writeOne(sendStr.getBytes());
				return wrSocket;
			} catch (SocketTimeoutException ste) {
				clearUrl(url);
				if (num < 3) {
					return send(topic, content, url, num + 1);
				}
				throw new RuntimeException("连接超时!", ste);
			}
		} else {
			return null;
		}
	}

	private WRSocket send(String topic, Object[] msg, String url, int num) throws Exception {
		WRSocket wrSocket = getTcpChannelMap(url);
		if (wrSocket != null) {
			try {
				byte[] ba = topic.getBytes();
				byte[] msgb = MsgUtils.getBytesForTuples(msg);
				byte[] bs = new byte[2 + ba.length + msgb.length];
				System.arraycopy(Convert.toBytes((short) ba.length), 0, bs, 0, 2);
				System.arraycopy(ba, 0, bs, 2, ba.length);
				System.arraycopy(msgb, 0, bs, 2 + ba.length, msgb.length);
				wrSocket.writeOne(bs);
				return wrSocket;
			} catch (SocketTimeoutException ste) {
				clearUrl(url);
				if (num < 3) {
					return send(topic, msg, url, num + 1);
				}
				throw new RuntimeException("连接超时!", ste);
			}
		} else {
			return null;
		}
	}

	private WRSocket getTcpChannelMap(String url) {
		long threadId = Thread.currentThread().getId();
		Map<String, WRSocket> tcpMap = tcpChannelMap.get(threadId);
		if (tcpMap == null) {
			synchronized (tcpChannelMap) {
				tcpChannelMap.put(threadId, tcpMap = new HashMap<String, WRSocket>());
			}
		} else {
			tcpMap = tcpChannelMap.get(threadId);
		}
		WRSocket wrSocket = null;
		try {
			synchronized (tcpMap) {
				wrSocket = tcpMap.get(url);
				if (wrSocket == null) {
					TcpChannel tcpChannel = new TcpChannel(SocketChannel.open(new InetSocketAddress(url, getPort(url,
							SendType.SOCKET))), timeout, SelectionKey.OP_WRITE);
					tcpMap.put(url, new WRSocket(tcpChannel));
					wrSocket = tcpMap.get(url);
					wrSocket.tcpChannel.updateTimeout(timeout);
				}
			}
			return wrSocket;
		} catch (IOException e) {
			LogUtils.error("获取TcpChannel连接通道出错!" + e.getMessage(), e);
			return null;
		}
	}

	public void clearUrl(String url) {
		long threadId = Thread.currentThread().getId();
		Map<String, WRSocket> tcpMap = tcpChannelMap.get(threadId);
		WRSocket wrSocket = tcpMap.remove(url);
		if (wrSocket != null) {
			wrSocket.close();
		}
	}

	private static Map<Long, Map<String, WRSocket>> tcpChannelMap = new HashMap<Long, Map<String, WRSocket>>();

	static class WRSocket {
		TcpChannel tcpChannel;
		public long total = 0;
		public long read = 0;
		public long readError = 0;
		LinkedList<String> rets = new LinkedList<String>();// 返回值列表(按时间倒序)
		Thread thread;
		private boolean started = false;
		public int buffResSize = 10000;

		WRSocket(TcpChannel tcpChannel) throws IOException {
			this.tcpChannel = tcpChannel;
			thread = new Thread() {
				public void run() {
					while (true) {
						if (ready()) {
							try {
								String ret = readOne();
								rets.add(ret);
								if (rets.size() > buffResSize) {
									rets.removeFirst();
								}
							} catch (Exception e) {
								readError++;
								break;
							}
						}
					}
				}
			};
		}

		void addTotal() {
			total++;
		}

		boolean ready() {
			return read < total;
		}

		// 读取一条消息
		String readOne() throws IOException {
			byte[] b = new byte[4];
			tcpChannel.recv(ByteBuffer.wrap(b));
			int len = Convert.toInt(b);
			b = new byte[len];
			tcpChannel.recv(ByteBuffer.wrap(b));
			tcpChannel.updateTimeout(timeout);
			read++;
			return new String(b);
		}

		// 写入一条消息
		void writeOne(byte[] bs) throws IOException {
			tcpChannel.send(ByteBuffer.wrap(Convert.toBytes(bs.length)));
			tcpChannel.send(ByteBuffer.wrap(bs));
			tcpChannel.updateTimeout(timeout);// 重置超时时间
		}

		void close() {
			thread.stop();
			tcpChannel.cleanup();
		}

		public synchronized void start() {
			if (started)
				return;
			thread.start();
			started = true;
		}
	}

}
