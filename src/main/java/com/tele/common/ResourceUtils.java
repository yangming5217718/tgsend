package com.tele.common;

import java.util.Locale;

import org.springframework.context.support.ResourceBundleMessageSource;
import org.springframework.stereotype.Component;

@Component
public class ResourceUtils {

/*    public static String getEnglishValueByKey(String key){

        Locale locale = new Locale("en", "US");
        //使用指定的英文Locale
        ResourceBundle mySource = ResourceBundle.getBundle("is8n/message", locale);
        return mySource.getString(key);
    }

    public static String getChineseValueByKey(String key){

        Locale locale = Locale.getDefault();
        //使用指定的中文Locale
        ResourceBundle mySource = ResourceBundle.getBundle("is8n/messages", locale);
        return mySource.getString(key);
    }

    public static String getDeafultValueByKey(String key){

        //使用默认的Locale
        ResourceBundle mySource = ResourceBundle.getBundle("is8n/message");
        return mySource.getString(key);
    }

    public static String getValueAndPlaceholder(String key){

        //使用默认的Locale
        ResourceBundle mySource = ResourceBundle.getBundle("is8n/message");

        String beforeValue = mySource.getString(key);

        //填充国家化文件中的占位符
        String afterValue = MessageFormat.format(beforeValue, "安全");
        return afterValue;
    }*/

    /**
     * Spring 方式
     * @param args
     */
    private static ResourceBundleMessageSource messageSource = new ResourceBundleMessageSource();

    static {
        //指定国家化资源文件路径
        messageSource.setBasename("devmessages");
        //指定将用来加载对应资源文件时使用的编码，默认为空，表示将使用默认的编码进行获取。
        messageSource.setDefaultEncoding("UTF-8");
    }

    public static String getChineseValueByKey(String key){

        return messageSource.getMessage(key, null, Locale.CHINA);
    }

    public static String getDeafultValueByKey(String key){

        return messageSource.getMessage(key, null, null);
    }

    public static String getEnglishValueByKey(String key){

        return messageSource.getMessage(key, null, Locale.US);
    }

    public static String getValueAndPlaceholder(String key){

        return messageSource.getMessage(key, new Object[]{"安全"}, null);
    }



    public static void main(String[] args) {
       //System.out.println(get("com.website.operation"));
        System.out.println(ResourceUtils.getChineseValueByKey("index.top"));


        System.out.println(ResourceUtils.getEnglishValueByKey("index.top"));

    }

}
