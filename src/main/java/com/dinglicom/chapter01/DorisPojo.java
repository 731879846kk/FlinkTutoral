package com.dinglicom.chapter01;

public class DorisPojo {

    public int siteid;
    public int citycode;
    public String username;
    public int pv;

    public DorisPojo(int siteid, int citycode, String username, int pv) {
        this.siteid = siteid;
        this.citycode = citycode;
        this.username = username;
        this.pv = pv;
    }

    public int getSiteid() {
        return siteid;
    }

    public void setSiteid(int siteid) {
        this.siteid = siteid;
    }

    public int getCitycode() {
        return citycode;
    }

    public void setCitycode(int citycode) {
        this.citycode = citycode;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public int getPv() {
        return pv;
    }

    public void setPv(int pv) {
        this.pv = pv;
    }

    @Override
    public String toString() {
        return "DorisPojo{" +
                "siteid=" + siteid +
                ", citycode=" + citycode +
                ", username='" + username + '\'' +
                ", pv=" + pv +
                '}';
    }
}
