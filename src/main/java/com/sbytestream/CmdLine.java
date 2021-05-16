package com.sbytestream;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class CmdLine
{
    public CmdLine(String[] args)
    {
        this(args, '-');
    }

    public CmdLine(String[] args, char flagCharacter)
    {
        if (args == null)
            m_args = new String[] { };
        else
            m_args = args;

        m_flagCharacter = flagCharacter;
        parse();
    }

    public String getFlagValue(String flag) {
        return m_flagValueDict.get(flag);
    }

    public int getFlagValueInt(String flag, int defaultValue) {
        try {
            return Integer.parseInt(getFlagValue(flag));
        }
        catch(NumberFormatException e) {
            return defaultValue;
        }
    }

    public String getFlagValue(String flag, String defaultValue) {
        if (!m_flagValueDict.containsKey(flag))
            return defaultValue;
        else
            return m_flagValueDict.get(flag);
    }

    public boolean isFlagPresent(String flag)
    {
        return m_flagValueDict.containsKey(flag);
    }

    public String getPositionalArgument(int index)
    {
        return m_positionalArgs.get(index);
    }

    public int getFlagCount()
    {
        return m_flagValueDict.size();
    }

    public int getRawArgCount()
    {
        return m_args.length;
    }

    public int getPositionalArgumentCount()
    {
        return m_positionalArgs.size();
    }

    private void parse()
    {
        String lastFlag = null;

        for(String s : m_args)
        {
            String flag = getFlagFromString(s);
            if (flag != null)
            {
                lastFlag = flag;
                m_flagValueDict.put(flag, null);
            }
            else
            {
                if (lastFlag != null)
                {
                    m_flagValueDict.put(lastFlag, s);
                    lastFlag = null;
                }
                else
                {
                    m_positionalArgs.add(s);
                }
            }
        }
    }

    private String getFlagFromString(String s)
    {
        String flag = (s == null || s.isEmpty()) ? null : s;
        if (s.charAt(0) == m_flagCharacter)
            return flag.substring(1);
        else
            return null;
    }

    private char m_flagCharacter;
    private String[] m_args;
    private List<String> m_positionalArgs = new ArrayList<String>();
    private HashMap<String, String> m_flagValueDict = new HashMap<String, String>();
}