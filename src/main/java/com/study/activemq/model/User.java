package com.study.activemq.model;

import java.io.Serializable;

/**
 * Created by WuTing on 2017/11/9.
 */
public class User implements Serializable {
	private Integer age;
	private String name;

	public Integer getAge() {
		return age;
	}

	public void setAge(Integer age) {
		this.age = age;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
}