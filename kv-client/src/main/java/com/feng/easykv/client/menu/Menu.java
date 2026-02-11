package com.feng.easykv.client.menu;

import java.util.Scanner;

/**
 * @Description: 菜单
 * @Author: txf
 * @Date: 2026/2/9
 */
public class Menu {
    private static final Scanner in = new Scanner(System.in);

    public void printCommandLine() {
        System.out.print("client -> ");
    }

    public String getInput() {
        return in.nextLine();
    }

    public void printHelp() {
        System.out.println("============== HELP ============");
        System.out.println("help: show help");
        System.out.println("set: set key value -- 设置kv, 如果已经存在k，就覆盖");
        System.out.println("get: get key -- 根据指定的k，获取v, 如果不存在k,返回null");
        System.out.println("del: del key -- 删除指定的k");
        System.out.println("============== HELP ============");
    }

    public boolean judgeLegal(String command) {
        if ( "help".equals( command) ) return true;
        boolean ok1 =  command.startsWith("set") || command.startsWith("get")
                || command.startsWith("del") || command.startsWith("help");
        if ( !ok1 ) return false;
        if ( command.startsWith("set") ) {
            return command.split(" ").length == 3;
        }
        return (command.startsWith("get")|| command.startsWith("del")) && command.split(" ").length == 2;
    }

    public void printError() {
        System.out.println("error: 输入的命令有误");
    }
}
