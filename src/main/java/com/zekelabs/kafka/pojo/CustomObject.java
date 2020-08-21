package com.zekelabs.kafka.pojo;

import java.io.Serializable;

public class CustomObject implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private String PassengerId;
	private String Survived;
	private String Pclass;
	private String Name;
	public String getPassengerId() {
		return PassengerId;
	}
	public void setPassengerId(String passengerId) {
		PassengerId = passengerId;
	}
	public String getSurvived() {
		return Survived;
	}
	public void setSurvived(String survived) {
		Survived = survived;
	}
	public String getPclass() {
		return Pclass;
	}
	public void setPclass(String pclass) {
		Pclass = pclass;
	}
	public String getName() {
		return Name;
	}
	public void setName(String name) {
		Name = name;
	}
	public static long getSerialversionuid() {
		return serialVersionUID;
	}

	
}
