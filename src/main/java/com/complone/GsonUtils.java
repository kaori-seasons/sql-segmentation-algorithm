package com.complone;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class GsonUtils {

    public static Gson gson = getGson();

    public static Gson getGson() {
        gson = new GsonBuilder().disableHtmlEscaping().setPrettyPrinting().create();
        return gson;
    }
}
