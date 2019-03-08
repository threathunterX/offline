package com.threathunter.bordercollie.slot.tool;

import java.io.*;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;

public class ITLauncher {

    public static void main(String[] args) throws Exception {
        String javaHome = System.getProperty("java.home");
        final String javaLauncher = new File(javaHome, "bin/java").getPath();
        final String classPath = System.getProperty("java.class.path");
        final String currentDirectory = System.getProperty("user.dir");
//        final String currentClassName = getCurrentClassName();
        final String currentClassName = ITMain.class.getCanonicalName();
        final String[] processArgs = args;
        System.out.println("java home = " + javaHome);
        System.out.println("java launcher = " + javaLauncher);
        System.out.println("current directory  = " + currentDirectory);
        System.out.println("class name = " + currentClassName);
        args = new String[]{"-launch"};
        if (args.length > 0 && args[0].equals("-launch")) {
            final ArrayList<String> argumentList = new ArrayList<String>() {
                {
                    add(javaLauncher);
                    add("-cp");
                    add(classPath);
                    add(currentClassName);
                   /* for(int i= 1; i<processArgs.length; i++) {
                        add(processArgs[i]);
                    }*/
                }
            };
            System.out.println(argumentList);
            CountDownLatch latch = new CountDownLatch(1);
            Runnable processLauncher = new Runnable() {

                @Override
                public void run() {
                    try {
                        ProcessBuilder processBuilder = new ProcessBuilder(argumentList);
                        processBuilder.redirectErrorStream(true);

                        Process process = processBuilder.start();
                        StringBuilder processOutput = new StringBuilder();

                        try (BufferedReader processOutputReader = new BufferedReader(
                                new InputStreamReader(process.getInputStream()));) {
                            String readLine;

                            while ((readLine = processOutputReader.readLine()) != null) {
                                processOutput.append(readLine + System.lineSeparator());
                            }

                            process.waitFor();
                            System.out.println(processOutput);
                            latch.countDown();
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }

            };
            new Thread(processLauncher).start();
            latch.await();
            System.out.println("ITLauncher completed");

        } else {
            FileWriter writer = new FileWriter(File.createTempFile("launch-", ".log", new File(currentDirectory)));
            writer.write("Launched");
            writer.close();
            Thread.sleep(10000);
        }
    }

    private static String getCurrentClassName() throws Exception {
        StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
        int index = stackTrace.length - 2;
        StackTraceElement stackTraceElement = stackTrace[index];
        Class<?> c = Class.forName(stackTraceElement.getClassName());
        return c.getCanonicalName();
    }

}