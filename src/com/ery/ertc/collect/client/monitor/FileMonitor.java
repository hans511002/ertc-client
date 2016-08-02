package com.ery.ertc.collect.client.monitor;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alibaba.fastjson.JSON;
import com.ery.ertc.collect.client.monitor.podo.MonitorPO;
import com.ery.ertc.collect.client.monitor.utils.MonitorHelper;
import com.ery.base.support.utils.Convert;

public abstract class FileMonitor {

	protected List<MonitorPO> pos;

	protected Map<String, List<LogParser>> monitorPathMap;
	protected boolean started = false;

	// 每个监控项对应的内存映射map
	public static Map<Long, MappedByteBuffer> STORE_MAP = new HashMap<Long, MappedByteBuffer>();
	// mot_id path filename offset list
	public static Map<Long, Map<String, Map<String, List<Object>>>> STORE_POINTER = new HashMap<Long, Map<String, Map<String, List<Object>>>>();

	protected FileMonitor(List<MonitorPO> pos, FileChannel fileChannel) throws IOException {
		this.pos = pos;
		this.monitorPathMap = MonitorHelper.watchMonitorFiles(this.pos);// 解析监控目录

		Map<String, Object> offsetMap = LocalMonitorMain.getIns().STORE_HD_OFFSET;
		long p_ = 4 + LocalMonitorMain.HD_OFFSET_SIZE + LocalMonitorMain.HD_ITEM_SIZE * offsetMap.size();
		for (MonitorPO po : pos) {
			MappedByteBuffer mbb;
			if (offsetMap.containsKey(po.getMONITOR_ID() + "")) {
				mbb = fileChannel.map(FileChannel.MapMode.READ_WRITE,
						Convert.toLong(offsetMap.get(po.getMONITOR_ID() + "")), LocalMonitorMain.HD_ITEM_SIZE);
			} else {
				mbb = fileChannel.map(FileChannel.MapMode.READ_WRITE, p_, LocalMonitorMain.HD_ITEM_SIZE);
				offsetMap.put(po.getMONITOR_ID() + "", p_);
				p_ += LocalMonitorMain.HD_ITEM_SIZE;
			}
			STORE_MAP.put(po.getMONITOR_ID(), mbb);
			mbb.load();
			byte[] bs = new byte[4];
			mbb.get(bs);
			int len = Convert.toInt(bs);
			Map<String, Map<String, List<Object>>> ponitor = new HashMap<String, Map<String, List<Object>>>();
			if (len > 0) {
				bs = new byte[len];
				mbb.get(bs);
				String str = new String(bs);
				ponitor = JSON.parseObject(str, ponitor.getClass());
			}
			STORE_POINTER.put(po.getMONITOR_ID(), ponitor);
		}
	}

	public boolean isStarted() {
		return started;
	}

	public abstract void start();

	public abstract void stop();
}
