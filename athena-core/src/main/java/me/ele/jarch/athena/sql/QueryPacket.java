package me.ele.jarch.athena.sql;

/**
 * Created by Dal-Dev-Team on 16/12/9.
 */
public interface QueryPacket {
    public void setQuery(String query);

    public String getQuery();

    public byte[] toPacket();
}
