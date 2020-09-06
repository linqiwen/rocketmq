package org.apache.rocketmq.client.trace;

/**
 * @author linqw
 */
public class Test {

    public static void main(String[] args) {
        String str = "12345"+ TraceConstants.CONTENT_SPLITOR + 7890;
        System.out.println(str);
        String[] strings = str.split(String.valueOf(TraceConstants.CONTENT_SPLITOR));
        for (String string : strings) {
            System.out.println(string);
        }
    }

}
