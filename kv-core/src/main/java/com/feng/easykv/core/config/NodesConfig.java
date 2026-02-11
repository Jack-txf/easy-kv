package com.feng.easykv.core.config;

import com.feng.easykv.core.utils.NetworkUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @Description: 集群结点配置
 * @Author: txf
 * @Date: 2026/2/11
 */
public class NodesConfig {
    private static final Logger log = LoggerFactory.getLogger(NodesConfig.class);
    private static final String NODE_PROPERTIES = "nodes.properties";

    // 存储解析后的集群节点列表（按索引顺序）
    private List<String> nodeList;

    public NodesConfig() {
        // 从node.properties中读取
        // 初始化节点列表
        nodeList = new ArrayList<>();
        // 读取并解析node.properties
        loadNodeProperties();
    }

    public List<String> getNodeList() {
        return nodeList;
    }

    /**
     * 核心方法：读取resource下的node.properties并解析节点配置
     */
    private void loadNodeProperties() {
        // 1. 获取resource目录下的node.properties输入流（ClassLoader从classpath根目录读取）
        try (InputStream inputStream = getClass().getClassLoader().getResourceAsStream(NODE_PROPERTIES)) {
            if (inputStream == null) {
                throw new RuntimeException("未找到node.properties文件，请检查resource目录下是否存在该文件");
            }
            // 2. 加载properties文件
            Properties properties = new Properties();
            properties.load(inputStream);
            // 3. 解析nodes[0]、nodes[1]等配置（按索引排序）
            // 定义正则表达式匹配nodes[数字]格式的key
            Pattern pattern = Pattern.compile("^nodes\\[(\\d+)\\]$");
            // 临时Map存储<索引, 节点地址>，保证按索引排序
            Map<Integer, String> nodeMap = new TreeMap<>();
            // 遍历所有配置项
            for (String key : properties.stringPropertyNames()) {
                Matcher matcher = pattern.matcher(key);
                if (matcher.matches()) {
                    // 提取索引（如nodes[0]中的0）
                    int index = Integer.parseInt(matcher.group(1));
                    // 提取节点地址（如127.0.0.1:7777）
                    String nodeAddress = properties.getProperty(key).trim();
                    nodeMap.put(index, nodeAddress);
                }
            }
            // 4. 将排序后的节点存入列表
            nodeList.addAll(nodeMap.values());
            // 校验：如果没有解析到任何节点，给出警告
            if (nodeList.isEmpty()) {
                log.warn("警告：node.properties中未配置任何nodes[x]节点");
            }
        } catch (IOException e) {
            throw new RuntimeException("读取node.properties文件失败", e);
        } catch (NumberFormatException e) {
            throw new RuntimeException("node.properties中nodes[x]的索引不是合法数字", e);
        }
    }

    public String findSelf(int port) {
        List<String> localIpv4List = NetworkUtil.getValidLocalIpv4List();
        for (String s : this.nodeList) {
            String[] s1 = s.split(":");
            String ip = s1[0];
            int port1 = Integer.parseInt(s1[1]);
            if ( "127.0.0.1".equals(ip) || "localhost".equals(ip) ) {
                if ( port1 == port ) {
                    return s;
                }
            } else {
                for (String ipv4 : localIpv4List) {
                    if ( ipv4.equals(ip) ) {
                        if ( port1 == port ) {
                            return s;
                        }
                    }
                }
            }
        }
        return null;
    }
}
