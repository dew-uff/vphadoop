package uff.dew.svp.catalog.util;

import java.io.Serializable;

public class WrapperLocation implements Serializable {

	protected String uri;
	protected double communicationWeight;
	
	public WrapperLocation(){
	}
	
	public WrapperLocation(String uri){
		this.uri = uri;
	}

	public double getCommunicationWeight() {
		return communicationWeight;
	}

	public void setCommunicationWeight(double value) {
		this.communicationWeight = value;
	}

	public String getUri() {
		return uri;
	}

	public void setUri(String uri) {
		this.uri = uri;
	}
	
	public String getUriHost() {
		if ((uri.indexOf(":") > 0) && (uri.indexOf("/", uri.indexOf(":")+3) > 0)){
			return uri.substring(0, uri.indexOf("/", uri.indexOf(":")+3));
		}
		else
			return uri;
	}
	
	public static WrapperLocation getMediatorLocation(){
		WrapperLocation w = new WrapperLocation();
		w.setUri("MEDIATOR");
		w.setCommunicationWeight(0);
		return w;
	}

	/*
	 *  (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 * 
	 * Comparao do site da localizao
	 * 
	 */
	public boolean equals(Object obj) {
		WrapperLocation w = (WrapperLocation)obj;
		if (w.getUri().equals(this.getUri()))
			return true;
		else
			return false;
	}
	
}
