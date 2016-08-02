package com.ery.ertc.collect.client.sender;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.CharBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.alibaba.fastjson.JSON;
import com.ery.base.support.utils.Convert;

public class WSSender extends CollectSender {

	private Map<String, URL> urlMap = new HashMap<String, URL>();

	public WSSender() {
		CollectSender.init();
	}

	@Override
	public String sendData(String topic, String content) {
		return sendData(topic, content, false);
	}

	@Override
	public String sendData(String topic, List<String> content) {
		return sendData(topic, JSON.toJSONString(content), true);
	}

	public String sendData(String topic, String content, boolean isBetch) {
		HttpURLConnection httpConn = getHttpURLConnection(topic);
		if (httpConn != null) {
			try {
				String xmlContent = "<soapenv:Envelope xmlns:soapenv=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:ser=\"http://server.remote.collect.tydic.com/\">" +
						"<soapenv:Header/>" +
						"<soapenv:Body>" +
						"<ser:sendData>" +
						"<arg0>" +
						topic +
						"</arg0>" +
						"<arg1>" +
						content +
						"</arg1>" +
						"<arg2>" +
						Convert.toString(isBetch) +
						"</arg2>" +
						"</ser:sendData>" + "</soapenv:Body>" + "</soapenv:Envelope>";
				httpConn.setRequestProperty("Content-Length", xmlContent.getBytes().length + "");
				httpConn.setRequestProperty("Content-Type", "text/xml; charset=utf-8");
				httpConn.setRequestProperty("soapActionString", "");
				httpConn.setRequestMethod("POST");
				httpConn.setDoOutput(true);
				httpConn.setDoInput(true);

				// 请求
				OutputStream out = httpConn.getOutputStream();
				out.write(xmlContent.getBytes());
				out.close();

				// 返回
				InputStreamReader isr = new InputStreamReader(httpConn.getInputStream(), "utf-8");
				BufferedReader in = new BufferedReader(isr);
				CharBuffer charBuffer = CharBuffer.allocate(512);
				StringBuilder retXml = new StringBuilder();
				int len = 0;
				while ((len = in.read(charBuffer)) > 0) {
					retXml.append(new String(charBuffer.array(), 0, len));
					charBuffer.clear();
				}
				return parseReturn(retXml.toString());
			} catch (Exception e) {
				String host = httpConn.getURL().getHost();
				throw new RuntimeException("请求[" + host + ":" + getPort(host, SendType.WS) + "]消息不成功!", e);
			} finally {
				httpConn.disconnect();
			}
		} else {
			throw new RuntimeException("未找到可用服务端连接!");
		}
	}

	public String sendData(String topic, Object... objs) {
		if (objs == null || objs.length == 0) {
			throw new IllegalArgumentException("发送字段数据为空!");
		}
		return sendData(topic, JSON.toJSONString(objs));
	}

	private HttpURLConnection getHttpURLConnection(String topic) {
		String url = getUrl(topic, SendType.WS);
		if (url == null)
			return null;
		URL _u = urlMap.get(url);
		try {
			if (_u == null) {
				_u = new URL("http://" + url + ":" + getPort(url, SendType.WS) + "/CollectWS");
				urlMap.put(url, _u);
			}
			return (HttpURLConnection) _u.openConnection();
		} catch (Exception e) {
			return null;
		}
	}

	static String returnReg = "<return>([\\s\\S]*)</return>";

	private String parseReturn(String xml) {
		// LogUtils.debug("解析XML串:" + xml);
		try {
			Matcher matcher = Pattern.compile(returnReg).matcher(xml);
			if (matcher.find()) {
				return matcher.group(1);
			}
			return null;
			// Document doc = DocumentHelper.parseText(xml);
			// Node node = doc.selectSingleNode("//return");
			// return node.getStringValue();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

}
