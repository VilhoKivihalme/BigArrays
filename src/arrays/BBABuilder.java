package arrays;

import java.io.File;
import java.io.IOException;

@SuppressWarnings("unchecked")
public abstract class BBABuilder<T extends BBABuilder<T>> {

	protected int size = 100;
	protected String id = "default_ID_" + System.nanoTime();
	protected File root = new File(id);
	protected long maxChunksOnDisk = -1;
	protected int maxChunksLoaded = -1;
	private byte defaultvalue = 0;

	public BBABuilder(){

	}

	public T setSize(int size){
		this.size = size;
		return (T) this;
	}

	public T setId(String id){
		this.id = id;
		return (T) this;
	}

	public T setRoot(File root){
		this.root = root;
		return (T) this;
	}

	public T setDefaultValue(byte b){
		this.defaultvalue = b;
		return (T) this;
	}

	public T setMaxChunksOnDisk(int maxChunksOnDisk){
		this.maxChunksOnDisk = maxChunksOnDisk;
		return (T) this;
	}

	public T setMaxChunksLoaded(int maxChunksLoaded){
		this.maxChunksLoaded = maxChunksLoaded;
		return (T) this;
	}

	public BigByteArray build() throws IOException{
		System.out.println("buidling");
		return new BigByteArray(size, id, root, maxChunksOnDisk, maxChunksLoaded, defaultvalue);
	}
}
