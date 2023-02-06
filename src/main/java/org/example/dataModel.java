package org.example;

public class dataModel {
    private String Name;
    private String Phone;
    private String Content;
    dataModel(String Name,String Phone,String Content){
        this.Name=Name;
        this.Phone= Phone;
        this.Content=Content;
    }


    public String getName() {
        return Name;
    }

    public String getPhone() {
        return Phone;
    }

    public void setPhone(String phone) {
        Phone = phone;
    }

    public String getContent() {
        return Content;
    }

    public void setContent(String content) {
        Content = content;
    }

    public void setName(String name) {
        Name = name;
    }
}
