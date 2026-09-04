package com.tele.callback;


import java.util.List;

import org.springframework.stereotype.Component;


@Component
public class CallbackRouter {

    private final List<CallbackHandler> handlers;

    public CallbackRouter(List<CallbackHandler> handlers){
        this.handlers = handlers;
    }

    public boolean route(CallbackContext ctx){

        for(CallbackHandler handler:handlers){
            if(handler.support(ctx.getData())){
                handler.handle(ctx);
                return true;
            }
        }
        return false;
    }

}