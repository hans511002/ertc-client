package com.ery.ertc.collect.client.sender;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.NameValuePair;
import org.apache.commons.httpclient.methods.PostMethod;

import com.alibaba.fastjson.JSON;

public class HttpSender extends CollectSender {
	Map<String, HttpClient> httpClients = new HashMap<String, HttpClient>();
	LinkedList<String> errList = new LinkedList<String>();// 错误列表
	LinkedList<String> bufList = new LinkedList<String>();// 缓存列表
	boolean async = false;// 异步

	public HttpSender() {
		CollectSender.init();
	}

	@Override
	public String sendData(String topic, String content) {
		return send(topic, content, null);
	}

	public String sendData(String topic, List<String> contents) {
		return send(topic, contents, null);
	};

	public String sendData(String topic, Object... objs) {
		if (objs == null || objs.length == 0) {
			throw new IllegalArgumentException("发送字段数据为空!");
		}
		return sendData(topic, JSON.toJSONString(objs));
	}

	private String send(String topic, List<String> content, Set<String> exUrl) {
		HttpClient httpClient = getHttpClient(topic, exUrl);
		if (httpClient != null) {
			PostMethod post = new PostMethod("/" + topic);
			post.getParams().setContentCharset("UTF-8");
			NameValuePair isBetchPar = new NameValuePair("isBatch", "true");
			NameValuePair contentPar = new NameValuePair("content", JSON.toJSONString(content));
			post.setRequestBody(new NameValuePair[] { contentPar, isBetchPar });
			try {
				httpClient.executeMethod(post);
				if (post.getStatusCode() == 200) {
					return new String(post.getResponseBodyAsString().getBytes());// 返回值
				} else {
					return post.getStatusLine().toString();
				}
			} catch (IOException e) {
				throw new RuntimeException("http请求出错!", e);
			} finally {
				post.releaseConnection();// 释放连接
			}
		} else {
			if (exUrl == null || exUrl.isEmpty()) {
				throw new RuntimeException("未找到可用服务端连接!");
			} else {
				throw new RuntimeException("发送【" + topic + "," + content + "】->[" + exUrl.toString() + "]失败!");
			}
		}
	}

	private String send(String topic, String content, Set<String> exUrl) {
		HttpClient httpClient = getHttpClient(topic, exUrl);
		if (httpClient != null) {
			PostMethod post = new PostMethod("/" + topic);
			post.getParams().setContentCharset("UTF-8");
			NameValuePair contentPar = new NameValuePair("content", content);
			post.setRequestBody(new NameValuePair[] { contentPar });
			try {
				httpClient.executeMethod(post);
				if (post.getStatusCode() == 200) {
					return new String(post.getResponseBodyAsString().getBytes());// 返回值
				} else {
					return post.getStatusLine().toString();
				}
			} catch (IOException e) {
				throw new RuntimeException("http请求出错!", e);
			} finally {
				post.releaseConnection();// 释放连接
			}
		} else {
			if (exUrl == null || exUrl.isEmpty()) {
				throw new RuntimeException("未找到可用服务端连接!");
			} else {
				throw new RuntimeException("发送【" + topic + "," + content + "】->[" + exUrl.toString() + "]失败!");
			}
		}
	}

	private HttpClient getHttpClient(String topic) {
		return getHttpClient(topic, null);
	}

	private HttpClient getHttpClient(String topic, Set<String> exUrl) {
		String hostName = getUrl(topic, SendType.HTTP, exUrl);
		if (hostName == null)
			return null;
		HttpClient httpClient = httpClients.get(hostName);
		if (httpClient == null) {
			httpClient = new HttpClient();
			httpClient.getHostConfiguration().setHost(hostName, getPort(hostName, SendType.HTTP), "http");
			httpClient.getHttpConnectionManager().getParams().setConnectionTimeout(10000);// 10秒连接超时
			httpClient.getHttpConnectionManager().getParams().setSoTimeout(30000);// 30秒读取数据超时
			httpClients.put(hostName, httpClient);
		}
		return httpClient;
	}

}
