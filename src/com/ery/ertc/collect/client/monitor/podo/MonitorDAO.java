package com.ery.ertc.collect.client.monitor.podo;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.ery.base.support.sys.podo.BaseDAO;
import com.ery.base.support.utils.Convert;

public class MonitorDAO extends BaseDAO {

	public List<MonitorPO> listMonitor(String idStrs) {
		if (idStrs != null && !"".equals(idStrs)) {
			String sql = "SELECT MONITOR_ID,MONITOR_DESC,MONITOR_FILE,SEND_TOPIC,LINE_RULE," +
					"FIELD_RULE,MONITOR_FREQUENCY,INIT_OFFSET,NOTICE_TYPE,RUN_AUTHOR," +
					"SEND_FILTER_RULE,STATE,CRT_FILE_CYCLE,FIELD_FORMAT_RULE " +
					"FROM ST_LOCAL_MONITOR WHERE STATE=1 AND MONITOR_ID IN(" + idStrs + ")";
			List<Map<String, Object>> maps = getDataAccess().queryForList(sql);
			List<MonitorPO> monitorPOs = new ArrayList<MonitorPO>();
			for (Map<String, Object> map : maps) {
				MonitorPO po = new MonitorPO();
				po.setMONITOR_ID(Convert.toLong(map.get("MONITOR_ID")));
				po.setMONITOR_DESC(Convert.toString(map.get("MONITOR_DESC")));
				po.setMONITOR_FILE(Convert.toString(map.get("MONITOR_FILE")));
				po.setSEND_TOPIC(Convert.toString(map.get("SEND_TOPIC")));
				po.setLINE_RULE(Convert.toString(map.get("LINE_RULE")));
				po.setFIELD_RULE(Convert.toString(map.get("FIELD_RULE")));
				po.setMONITOR_FREQUENCY(Convert.toInt(map.get("MONITOR_FREQUENCY"), 0));
				po.setINIT_OFFSET(Convert.toLong(map.get("INIT_OFFSET")));
				po.setNOTICE_TYPE(Convert.toString(map.get("NOTICE_TYPE"), "HTTP"));
				po.setRUN_AUTHOR(Convert.toString(map.get("RUN_AUTHOR")));
				po.setSEND_FILTER_RULE(Convert.toString(map.get("SEND_FILTER_RULE")));
				po.setSTATE(Convert.toInt(map.get("STATE")));
				po.setCRT_FILE_CYCLE(Convert.toInt(map.get("CRT_FILE_CYCLE"), 0));
				po.setFIELD_FORMAT_RULE(Convert.toString(map.get("FIELD_FORMAT_RULE"), "{}"));
				monitorPOs.add(po);
			}
			return monitorPOs;
		}
		return null;
	}

}
