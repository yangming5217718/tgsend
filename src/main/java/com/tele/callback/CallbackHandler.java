package com.tele.callback;


public interface CallbackHandler {


    /**
     * 是否处理这个按钮
     */
    boolean support(String data);



    /**
     * 执行业务
     */
    void handle(CallbackContext ctx);


}