package com.ery.ertc.collect.client.sender;

import java.io.IOException;
import java.util.List;

import com.ery.ertc.collect.client.monitor.podo.MonitorPO;

public abstract class Sender {

	// 初始化一个数据模型
	public abstract void init(MonitorPO m) throws IOException;

	public abstract String sendData(String topic, String content);

	public abstract String sendData(String topic, List<String> contents);

	public abstract String sendData(String topic, Object... objs);

}
