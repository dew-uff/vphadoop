package uff.dew.svp.algebra.util;

public class IdGenerator {
	
	private int genId;
	private static IdGenerator myInstance;
	
	public IdGenerator(){
		this.genId = 1;
	}
	
	private int getNextIdInternal(){
		return this.genId++;
	}
	
	private static IdGenerator getInstance(){
		if (myInstance == null){
			myInstance = new IdGenerator();
		}
		return myInstance;
	}
	
	/**
	 * Busca o prximo Id (mtodo esttico)
	 * @return Id
	 */
	public static int getNextId(){
		return IdGenerator.getInstance().getNextIdInternal();		
	}
	
	public static void reset(){
		myInstance = new IdGenerator();
	}

}
