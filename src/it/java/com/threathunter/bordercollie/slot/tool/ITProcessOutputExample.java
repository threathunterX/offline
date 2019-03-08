package com.threathunter.bordercollie.slot.tool;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class ITProcessOutputExample {
    public static void main(String[] arguments) throws IOException,
            InterruptedException {
        System.out.println(getProcessOutput());
    }

    public static String getProcessOutput() throws IOException, InterruptedException {
        ProcessBuilder processBuilder = new ProcessBuilder("java",
                "-version");

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
        }

        return processOutput.toString().trim();
    }
}