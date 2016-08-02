package com.ery.ertc.collect.client.monitor.podo;

import java.io.Serializable;
import java.util.Map;

import com.alibaba.fastjson.JSON;

public class MonitorPO implements Serializable {

	public final static String SPLIT_KEY = "splitCh";// 拆分符key
	public final static String START_KEY = "startCh";// 起始符key
	public final static String SPLIT_APPEND_KEY = "splitChAppend";// 行起始符key
	public final static String DEL_BLANK_KEY = "delBlank";// 删除首尾空白符
	public final static String FIELD_NUM_KEY = "fieldNum";// 字段规则，字段数目
	public final static String IGNORE_ERROR_KEY = "ignoreError";// 忽略异常
	public final static String EXPR_KEY = "expr";// 过滤规则，表达式

	private long MONITOR_ID;
	private String MONITOR_DESC;
	private String MONITOR_FILE;
	private String SEND_TOPIC;// 提取的日志最终发送至消息主题
	private String LINE_RULE;
	private String FIELD_RULE;
	private int MONITOR_FREQUENCY;// 监控频率秒数=0表示实时通知，>0轮询
	private long INIT_OFFSET;// 初始第一次识别位置，START,END
	private String NOTICE_TYPE;// 通知服务端方式，WS,HTTP,SOCKET
	private String RUN_AUTHOR;// 运行角色用户（如系统日志需要root用户才能访问读取到）

	private String SEND_FILTER_RULE;// 每条记录按其中的规则匹配，满足才发送，否则丢弃
	private int STATE;// 状态
	private int CRT_FILE_CYCLE;// 创建文件周期
	private String FIELD_FORMAT_RULE;// 字段格式规则

	private Map<String, Object> lineRule;
	private Map<String, Object> fieldRule;
	private Map<String, Object> sendFilterRule;
	private Map<String, Object> formatRule;// 格式规则

	public Map<String, Object> getLineRule() {
		if (lineRule == null) {
			lineRule = JSON.parseObject(LINE_RULE);
		}
		return lineRule;
	}

	public void setLineRule(Map<String, Object> lineRule) {
		this.lineRule = lineRule;
		if (LINE_RULE == null && lineRule != null) {
			LINE_RULE = JSON.toJSONString(lineRule);
		}
	}

	public Map<String, Object> getFieldRule() {
		if (fieldRule == null) {
			fieldRule = JSON.parseObject(FIELD_RULE);
		}
		return fieldRule;
	}

	public void setFieldRule(Map<String, Object> fieldRule) {
		this.fieldRule = fieldRule;
		if (FIELD_RULE == null && fieldRule != null) {
			FIELD_RULE = JSON.toJSONString(fieldRule);
		}
	}

	public Map<String, Object> getSendFilterRule() {
		if (sendFilterRule == null) {
			sendFilterRule = JSON.parseObject(SEND_FILTER_RULE);
		}
		return sendFilterRule;
	}

	public void setSendFilterRule(Map<String, Object> sendFilterRule) {
		this.sendFilterRule = sendFilterRule;
		if (SEND_FILTER_RULE == null && sendFilterRule != null) {
			SEND_FILTER_RULE = JSON.toJSONString(sendFilterRule);
		}
	}

	public Map<String, Object> getFormatRule() {
		if (formatRule == null) {
			formatRule = JSON.parseObject(FIELD_FORMAT_RULE);
		}
		return formatRule;
	}

	public void setFormatRule(Map<String, Object> formatRule) {
		this.formatRule = formatRule;
		if (FIELD_FORMAT_RULE == null && formatRule != null) {
			FIELD_FORMAT_RULE = JSON.toJSONString(formatRule);
		}
	}

	public long getMONITOR_ID() {
		return MONITOR_ID;
	}

	public void setMONITOR_ID(long MONITOR_ID) {
		this.MONITOR_ID = MONITOR_ID;
	}

	public String getMONITOR_DESC() {
		return MONITOR_DESC;
	}

	public void setMONITOR_DESC(String MONITOR_DESC) {
		this.MONITOR_DESC = MONITOR_DESC;
	}

	public String getMONITOR_FILE() {
		return MONITOR_FILE;
	}

	public void setMONITOR_FILE(String MONITOR_FILE) {
		this.MONITOR_FILE = MONITOR_FILE;
	}

	public String getSEND_TOPIC() {
		return SEND_TOPIC;
	}

	public void setSEND_TOPIC(String SEND_TOPIC) {
		this.SEND_TOPIC = SEND_TOPIC;
	}

	public String getLINE_RULE() {
		return LINE_RULE;
	}

	public void setLINE_RULE(String LINE_RULE) {
		this.LINE_RULE = LINE_RULE;
	}

	public String getFIELD_RULE() {
		return FIELD_RULE;
	}

	public void setFIELD_RULE(String FIELD_RULE) {
		this.FIELD_RULE = FIELD_RULE;
	}

	public int getMONITOR_FREQUENCY() {
		return MONITOR_FREQUENCY;
	}

	public void setMONITOR_FREQUENCY(int MONITOR_FREQUENCY) {
		this.MONITOR_FREQUENCY = MONITOR_FREQUENCY;
	}

	public long getINIT_OFFSET() {
		return INIT_OFFSET;
	}

	public void setINIT_OFFSET(long INIT_OFFSET) {
		this.INIT_OFFSET = INIT_OFFSET;
	}

	public String getNOTICE_TYPE() {
		return NOTICE_TYPE;
	}

	public void setNOTICE_TYPE(String NOTICE_TYPE) {
		this.NOTICE_TYPE = NOTICE_TYPE;
	}

	public String getRUN_AUTHOR() {
		return RUN_AUTHOR;
	}

	public void setRUN_AUTHOR(String RUN_AUTHOR) {
		this.RUN_AUTHOR = RUN_AUTHOR;
	}

	public String getSEND_FILTER_RULE() {
		return SEND_FILTER_RULE;
	}

	public void setSEND_FILTER_RULE(String SEND_FILTER_RULE) {
		this.SEND_FILTER_RULE = SEND_FILTER_RULE;
	}

	public int getSTATE() {
		return STATE;
	}

	public void setSTATE(int STATE) {
		this.STATE = STATE;
	}

	public int getCRT_FILE_CYCLE() {
		return CRT_FILE_CYCLE;
	}

	public void setCRT_FILE_CYCLE(int CRT_FILE_CYCLE) {
		this.CRT_FILE_CYCLE = CRT_FILE_CYCLE;
	}

	public String getFIELD_FORMAT_RULE() {
		return FIELD_FORMAT_RULE;
	}

	public void setFIELD_FORMAT_RULE(String FIELD_FORMAT_RULE) {
		this.FIELD_FORMAT_RULE = FIELD_FORMAT_RULE;
	}

	@Override
	public String toString() {
		return "[MONITOR_ID:" + MONITOR_ID + ",SEND_TOPIC:" + SEND_TOPIC + "]";
	}
}
