package com.feng.easykv.client;

import com.feng.easykv.client.client.RaftNettyClient;
import com.feng.easykv.client.menu.Menu;
import com.feng.easykv.protocol.KvRaftProto;

/**
 * @Description: 客户端
 * @Author: txf
 * @Date: 2026/2/9
 */
public class KvClientStarter {

    private final RaftNettyClient client;
    private final Menu menu;


    public  KvClientStarter() {
        // 初始化工作 ...
        menu = new Menu();
        client = new RaftNettyClient();

    }

    public void start() {
        while ( true ) {
            menu.printCommandLine();
            String command = menu.getInput();
            if ( "exit".equals(command) ) break;
            if ( "clear".equals( command)) {
                clearConsole();
                continue;
            }
            if ( "help".equals(command) ) {
                menu.printHelp();
                continue;
            }
            // 解析用户输入的命令
            boolean j = menu.judgeLegal(command);
            if ( !j ) {
                menu.printError();
                continue;
            }
            String[] commands = parse(command);

            // 创建一个KvRaftProto.RaftKvMessage对象
            KvRaftProto.RaftKvMessage message = KvRaftProto.RaftKvMessage.newBuilder()
                    .setType(KvRaftProto.RaftKvMessage.MessageType.KV_REQUEST)
                    .setKvRequest(
                            KvRaftProto.KvRequest.newBuilder()
                                .setKey(commands[1])
                                .setValue(commands[2])
                                .build()
                    )
                    .build();
            client.send(message);
        }

        // client进行收尾工作...
    }

    private void clearConsole() {
        try {
            String os = System.getProperty("os.name").toLowerCase();
            ProcessBuilder processBuilder;
            // 根据操作系统构建不同的命令
            if (os.contains("win")) {
                // Windows：cmd /c cls（ProcessBuilder需要将命令拆分为数组）
                processBuilder = new ProcessBuilder("cmd", "/c", "cls");
            } else {
                // Linux/macOS：clear
                processBuilder = new ProcessBuilder("clear");
            }
            // 将Process的输出流与当前控制台关联（确保命令生效）
            processBuilder.inheritIO();
            // 启动进程并等待执行完成
            Process process = processBuilder.start();
            process.waitFor();
        } catch (Exception e) {
            // 异常兜底：换行模拟清屏
            System.out.println("\n".repeat(50));
            System.out.println();
        }
    }

    public String[] parse( String command ) {
        return command.split(" ");
    }

    public static void main(String[] args) {
        KvClientStarter kvClient = new KvClientStarter();
        kvClient.start();
    }
}
