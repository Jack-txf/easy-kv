package com.feng.easykv.core.utils;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

/**
 * @Description: 网络工具
 * @Author: txf
 * @Date: 2026/2/11
 */
public class NetworkUtil {
    /**
     * 获取本机所有有效内网IPv4地址（非回环、非虚拟、已启用）
     * @return 有效IPv4地址列表
     */
    public static List<String> getValidLocalIpv4List() {
        List<String> ipList = new ArrayList<>();
        try {
            // 遍历所有网络接口
            Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();
            while (networkInterfaces.hasMoreElements()) {
                NetworkInterface ni = networkInterfaces.nextElement();

                // 过滤条件1：跳过回环接口（如127.0.0.1对应的接口）
                if (ni.isLoopback()) {
                    continue;
                }
                // 过滤条件2：跳过未启用的接口
                if (!ni.isUp()) {
                    continue;
                }
                // 过滤条件3：跳过虚拟接口（如虚拟机、Docker网卡，可选）
                if (ni.isVirtual()) {
                    continue;
                }

                // 遍历当前接口的所有IP地址
                Enumeration<InetAddress> inetAddresses = ni.getInetAddresses();
                while (inetAddresses.hasMoreElements()) {
                    InetAddress ia = inetAddresses.nextElement();
                    // 只保留IPv4地址（排除IPv6）
                    if (ia instanceof Inet4Address) {
                        String ip = ia.getHostAddress();
                        ipList.add(ip);
                    }
                }
            }
        } catch (SocketException e) {
            System.err.println("遍历网卡获取IP失败：" + e.getMessage());
        }
        return ipList;
    }

}
