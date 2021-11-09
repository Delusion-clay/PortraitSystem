package com.it.userportrait.bean;

public class ProductType {

    private long id;
  private String producttypename;
    private String producttypedesc;
   private long  producttypeparentid;
    private int producttypeleave;

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getProducttypename() {
        return producttypename;
    }

    public void setProducttypename(String producttypename) {
        this.producttypename = producttypename;
    }

    public String getProducttypedesc() {
        return producttypedesc;
    }

    public void setProducttypedesc(String producttypedesc) {
        this.producttypedesc = producttypedesc;
    }

    public long getProducttypeparentid() {
        return producttypeparentid;
    }

    public void setProducttypeparentid(long producttypeparentid) {
        this.producttypeparentid = producttypeparentid;
    }

    public int getProducttypeleave() {
        return producttypeleave;
    }

    public void setProducttypeleave(int producttypeleave) {
        this.producttypeleave = producttypeleave;
    }
}
