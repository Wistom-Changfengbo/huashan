package com.wangjia.common;


/**
 * 常用事件ID
 */
public final class LogEventId {
    /**
     * 事件APP切入后台
     */
    public static final String APP_ENTER_BACKGROUND = "__sys_app_enter_background";

    /**
     * 事件APP切入前台
     */
    public static final String APP_ENTER_FOREGROUND = "__sys_app_enter_foreground";

    /**
     * 事件用户注册
     */

    public static final String USER_REGISTER = "__sys_user_register";

    /**
     * 事件用户登录
     */
    public static final String USER_LOGIN = "__sys_user_login";

    /**
     * 事件用户注销
     */
    public static final String USER_LOGOUT = "__sys_user_logout";

    /**
     * 事件GPS
     */
    public static final String APP_LOCATION = "__sys_app_location";

    /**
     * 事件应用列表
     */
    public static final String APP_APPLIST = "__sys_app_applist";

    /**
     * 事件提交线索
     */
    public static final String USER_COMMIT_CLUE = "__sys_user_commit_clue";

    /**
     * 事件页面点击
     */
    public static final String UI_CLICK_POINT = "__sys_operate_click_point";

    /**
     * 事件通讯录
     */
    public static final String APP_CONTACT_LIST = "__sys_app_contact_list";

    /**
     * 广告展示
     * ID/类型/标题/URL
     * {id:xxxx, type:xxxx, title:'xxxx', url:'xxxx'}
     */
    public static final String UI_SHOW_ADVERTISEMENT = "__sys_ui_show_advertisement";

    /**
     * 广告点击
     * ID/类型/标题/URL
     * {id:xxxx, type:xxxx, title:'xxxx', url:'xxxx'}
     */
    public static final String UI_CLICK_ADVERTISEMENT = "__sys_ui_click_advertisement";
}
