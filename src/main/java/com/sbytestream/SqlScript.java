package com.sbytestream;

import java.io.BufferedReader;
import java.io.FileReader;

public class SqlScript {
    public SqlScript(String path, ISqlScriptClient listener) {
        scriptFilePath = path;
        this.listener = listener;
    }

    public void parse() throws Exception {
        StringBuilder sb = new StringBuilder();
        try (FileReader reader = new FileReader(scriptFilePath); BufferedReader buffReader = new BufferedReader(reader)) {
            String line = buffReader.readLine();
            while (line != null) {
                line = line.trim();
                if (line.equalsIgnoreCase(BLOCK_TERMINATOR)) {
                    listener.statementFound(sb.toString());
                    sb.setLength(0);
                }
                else {
                    sb.append(line);
                    sb.append("\n");
                }
                line = buffReader.readLine();
            }
            if (sb.length() > 0) {
                listener.statementFound(sb.toString());
                sb.setLength(0);
            }
        }
        catch (Exception e) {
            throw e;
        }
    }

    public interface ISqlScriptClient {
        void statementFound(String statement);
    }

    private String scriptFilePath;
    private ISqlScriptClient listener;
    private final String BLOCK_TERMINATOR = "go";
}
