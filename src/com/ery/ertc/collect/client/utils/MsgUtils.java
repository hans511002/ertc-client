package com.ery.ertc.collect.client.utils;

import com.ery.base.support.utils.Convert;
import com.ery.base.support.utils.Utils;

import java.util.Date;


public class MsgUtils {

    public static byte[] getBytesForTuples(Object[] msg){
        if(msg==null || msg.length==0){
            return null;
        }
        byte[] bs = new byte[1024];
        int len = 0;
        for (Object aMsg : msg) {
            byte[] ba = null;
            if (aMsg instanceof Double) {
                ba = Convert.toBytes(Convert.toDouble(aMsg));
            } else if (aMsg instanceof Number) {
                ba = Convert.toBytes(Convert.toLong(aMsg));
            } else if (aMsg instanceof Date) {
                ba = Convert.toBytes(((Date) aMsg).getTime());
            } else if (aMsg instanceof byte[]) {
                byte[] bb = (byte[]) aMsg;
                ba = new byte[4 + bb.length];
                System.arraycopy(Convert.toBytes(bb.length), 0, ba, 0, 4);
                System.arraycopy(bb, 0, ba, 0, bb.length);
            } else {
                byte[] strb = Convert.toString(aMsg).getBytes();
                ba = new byte[4 + strb.length];
                System.arraycopy(Convert.toBytes(strb.length), 0, ba, 0, 4);
                System.arraycopy(strb, 0, ba, 0, strb.length);
            }

            if (ba.length - len < bs.length) {
                //剩余位置不足以装下本次的值
                len = ba.length;
                bs = Utils.expandVolume(bs, ba.length);//扩大容量
            }
            System.arraycopy(ba, 0, bs, len, ba.length);
            len += ba.length;
        }
        if(len<bs.length){
            byte[] b1 = new byte[len];
            System.arraycopy(bs,0,b1,0,len);
            bs = b1;
        }
        return bs;
    }

}
