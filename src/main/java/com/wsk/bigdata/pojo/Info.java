package com.wsk.bigdata.pojo;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

public class Info implements Writable {

	/**
	 * 产品唯一标识id
	 */
	private String pId;

	/**
	 * 产品名称
	 */
	private String pName;

	/**
	 * 产品价格
	 */
	private float price;

	/**
	 * 产品生产地区
	 */
	private String produceArea;

	/**
	 * 用户Id
	 */
	private String uId;

	/**
	 * 用户点击时间戳：yyyyMMddHHmmss
	 */
	private String dateStr;

	/**
	 * 用户点击发生地区
	 */
	private String clickArea;


	/**
	 * flag=0，表示封装用户点击日志数据
	 * flag=1，表示封装产品信息
	 */
	private String flag;

	public String getpId() {
		return pId;
	}

	public void setpId(String pId) {
		this.pId = pId;
	}

	public String getpName() {
		return pName;
	}

	public void setpName(String pName) {
		this.pName = pName;
	}

	public float getPrice() {
		return price;
	}

	public void setPrice(float price) {
		this.price = price;
	}

	public String getProduceArea() {
		return produceArea;
	}

	public void setProduceArea(String produceArea) {
		this.produceArea = produceArea;
	}

	public String getuId() {
		return uId;
	}

	public void setuId(String uId) {
		this.uId = uId;
	}

	public String getDateStr() {
		return dateStr;
	}

	public void setDateStr(String dateStr) {
		this.dateStr = dateStr;
	}

	public String getClickArea() {
		return clickArea;
	}

	public void setClickArea(String clickArea) {
		this.clickArea = clickArea;
	}

	public String getFlag() {
		return flag;
	}

	public void setFlag(String flag) {
		this.flag = flag;
	}

	public Info(String pId, String pName, float price, String produceArea, String uId, String dateStr, String clickArea, String flag) {
		this.pId = pId;
		this.pName = pName;
		this.price = price;
		this.produceArea = produceArea;
		this.uId = uId;
		this.dateStr = dateStr;
		this.clickArea = clickArea;
		this.flag = flag;
	}

	public Info() {
	}

	@Override
	public String toString() {
		String[] fileds = {this.pId,};
		return "pid=" + this.pId + ",pName=" + this.pName + ",price=" + this.price
				+ ",produceArea=" + this.produceArea
				+ ",uId=" + this.uId + ",clickDate=" + this.dateStr + ",clickArea=" + this.clickArea;
	}

	@Override
	public void write(DataOutput out) throws IOException {

		out.writeUTF(this.pId);
		out.writeUTF(this.pName);
		out.writeFloat(this.price);
		out.writeUTF(this.produceArea);
		out.writeUTF(this.uId);
		out.writeUTF(this.dateStr);
		out.writeUTF(this.clickArea);
		out.writeUTF(this.flag);

	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.pId = in.readUTF();
		this.pName = in.readUTF();
		this.price = in.readFloat();
		this.produceArea = in.readUTF();
		this.uId = in.readUTF();
		this.dateStr = in.readUTF();
		this.clickArea = in.readUTF();
		this.flag= in.readUTF();

	}
}
