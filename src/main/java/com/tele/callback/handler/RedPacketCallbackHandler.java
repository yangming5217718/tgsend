package com.tele.callback.handler;

import com.tele.callback.CallbackContext;
import com.tele.callback.CallbackHandler;
import org.springframework.stereotype.Component;

@Component
public class RedPacketCallbackHandler implements CallbackHandler {

    @Override
    public boolean support(String data){
        return data.startsWith("rp:");

    }

    @Override
    public void handle(CallbackContext ctx){
        //红包业务
    }
}