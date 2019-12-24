package com.wangjia.bean;

import org.jetbrains.annotations.NotNull;

import java.io.Serializable;
import java.util.List;

public class AppDesc implements Serializable
{
    private List<AppKv> applist;
    private Long firstTime;
    private Long lastTime;
    private List<Operation> operations;

    public List<AppKv> getApplist() {
        return applist;
    }

    public void setApplist(List<AppKv> applist) {
        this.applist = applist;
    }

    public Long getFirstTime() {
        return firstTime;
    }

    public void setFirstTime(Long firstTime) {
        this.firstTime = firstTime;
    }

    public Long getLastTime() {
        return lastTime;
    }

    public void setLastTime(Long lastTime) {
        this.lastTime = lastTime;
    }

    public List<Operation> getOperations() {
        return operations;
    }

    public void setOperations(List<Operation> operations) {
        this.operations = operations;
    }

    @Override
    public String toString() {
        return "AppDesc{" +
                "applist=" + applist +
                ", firstTime=" + firstTime +
                ", lastTime=" + lastTime +
                ", operations=" + operations +
                '}';
    }

    public static final class Operation implements Comparable<Operation>,Serializable{
        private Long time;
        private List<Op> opes;

        public Long getTime() {
            return time;
        }

        public void setTime(Long time) {
            this.time = time;
        }

        public List<Op> getOps() {
            return opes;
        }

        public void setOps(List<Op> ops) {
            this.opes = ops;
        }

        @Override
        public String toString() {
            return "Operation{" +
                    "time=" + time +
                    ", opes=" + opes +
                    '}';
        }

        @Override
        public int compareTo(@NotNull Operation right) {
            double dv = getTime() - right.getTime();
            if (dv > 0)
                return -1;
            if (dv < 0)
                return 1;
            return 0;
        }
    }

    public static final class Op implements Serializable{
        private String ope;
        private String pkgname;
        private String appname;

        public String getOpe() {
            return ope;
        }

        public void setOpe(String ope) {
            this.ope = ope;
        }

        public String getPkgname() {
            return pkgname;
        }

        public void setPkgname(String pkgname) {
            this.pkgname = pkgname;
        }

        public String getAppname() {
            return appname;
        }

        public void setAppname(String appname) {
            this.appname = appname;
        }

        @Override
        public String toString() {
            return "Op{" +
                    "ope='" + ope + '\'' +
                    ", pkgname='" + pkgname + '\'' +
                    ", appname='" + appname + '\'' +
                    '}';
        }
    }

    public static final class AppKv implements Serializable{
        private String packageName;
        private String appName;

        public String getPackageName() {
            return packageName;
        }

        public void setPackageName(String packageName) {
            this.packageName = packageName;
        }

        public String getAppName() {
            return appName;
        }

        public void setAppName(String appName) {
            this.appName = appName;
        }

        @Override
        public String toString() {
            return "AppKv{" +
                    "packageName='" + packageName + '\'' +
                    ", appName='" + appName + '\'' +
                    '}';
        }
    }
}
