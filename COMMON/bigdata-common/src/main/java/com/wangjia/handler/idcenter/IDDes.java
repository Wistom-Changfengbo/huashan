package com.wangjia.handler.idcenter;

public final class IDDes {
    private int index = 0;
    private int idType = 0;
    private String id = "";

    public IDDes() {
    }

    public IDDes(int index, int idType, String id) {
        this.index = index;
        this.idType = idType;
        this.id = id;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public int getIdType() {
        return idType;
    }

    public void setIdType(int idType) {
        this.idType = idType;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public IDDes newIDDes(String uuid) {
        return new IDDes(index, IDType.ID_UUID, uuid);
    }
}
