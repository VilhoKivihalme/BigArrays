package arrays;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.LinkedList;

import javax.activation.UnsupportedDataTypeException;

//TODO: do not initialize first chunk! then a job can be continued from cache.
public class BigByteArray{

	/*
	 * default dimensions for chunks
	 */
	private static final int DEFAULT_CHUNKSIZE = 100;

	/**
	 * This regex is used to remove characters that are not to be used in the id
	 * name. This should help qualify the id to be a filename.
	 */
	private static String idRegex = "[^\\w^\\.\\-\\_]+";

	/*
	 * id's for all byte arrays. id must be unique within certain root.
	 */
	private static HashMap<String, String> ids = new HashMap<String, String>();

	/*
	 * How many chunks are possible to write on disk -1 is infinite.
	 */
	private int maxChunksOnDisk = -1;

	/*
	 * Maximum number of chunks kept in memory at same time. -1 means no limit.
	 * if reassigned during runtime (TODO: decrease immediatly/next loading/not
	 * at all?)
	 */
	protected int maxChunksLoaded = -1;

	/*
	 * Location on disk for this byte array
	 */
	protected File chunkCacheRoot;

	/*
	 * Chunk that was accessed last time. Decreases lookup time when iterating
	 * chunks
	 */
	protected Chunk current;

	/*
	 * all chunks as a list.
	 */
	protected LinkedList<Chunk> chunks = new LinkedList<Chunk>();

	/*
	 * unique id for this array. used as output folder name.
	 */
	protected String id;

	/*
	 * current count of chunks created
	 */
	public long chunksCreated = 0;

	/*
	 * Size of a single array
	 */
	protected final int SIZE;

	/*
	 * Used to mark if disk has been accessed already on this run
	 */
	private boolean diskUsed;

	/*
	 * Denotes where the [0][0] index is relatively to the actual byte array.
	 * true = actual index matches the byte array that is contained. false=
	 * origo is in the middle. useful when negative indices are required as well
	 * This parameter is not yet used for anything, but I might try that in case
	 * there is a need.
	 */
	private boolean zeroIndexed;// = false;

	/*
	 * How many times this map has been accessed.
	 */
	private long iteration = 0;

	//-----------------CONSTRUCTORS-----------------//

	/*
	 * Initializes a BigByteArray of certain size and id. Using of BBABuilder is
	 * recommended.
	 */
	public BigByteArray(int size, String id, File root, long chunksOnDisk, int chunksInMemory, byte defaultValue) throws IOException{
		root = root == null ? new File("") : root;
		if(!root.isDirectory()){
			//		XXX	throw new IOException("root must be a directory!");
		}
		//	XXX	BigByteArray.setRoot(root, this);
		String idtest = trimID(id);
		if(ids.containsKey(idtest)){
			throw new IllegalArgumentException("Duplicate id:" + id);
		}
		this.id = idtest;
		this.SIZE = size % 2 == 0 ? size : size + 1;
		this.chunkCacheRoot = root;
		init();
	}

	/*
	 * Initializes a BigByteArray of certain size and id.
	 */
	public BigByteArray(int size, String id) throws IOException{
		this(size, id, null, -1, -1, (byte) 0);
	}

	/*
	 * Initializes a BigByteArray of certain size and id.
	 */
	public BigByteArray(String id) throws IOException{
		this(DEFAULT_CHUNKSIZE, id, null, -1, -1, (byte) 0);
	}

	/*
	 * Initializes a BigByteArray of certain size.
	 */
	public BigByteArray(int size) throws IOException{
		this(size, "default_id_" + System.currentTimeMillis() + "", null, -1, -1, (byte) 0);
	}

	//---------------- SETUP METHODS------------------//

	/*
	 * initializes some settings
	 */
	private void init(){
		Chunk current = new Chunk(0 - SIZE / 2, 0 - SIZE / 2, SIZE);
		chunksCreated = 1;
		chunks = new LinkedList<Chunk>();
		this.current = current;
	}

	public void setLoadedChunkLimit(int i){
		if(i == 0){
			i = 1;
		}
		if(i < 0){
			i = -1;
		}
		maxChunksLoaded = i;
	}

	/**
	 * sets how many chunks can be stored on the disk for this instance of the
	 * array.
	 * 
	 * @param i
	 */
	public void setDiskChunkLimit(int i){
		if(i == 0){
			i = 1;
		}
		if(i < 0){
			i = -1;
		}
		maxChunksOnDisk = i;
	}

	//----------------METHODS------------------//

	/**
	 * sets data to a single field. Can be slow if data is set randomly, because
	 * all the chunks have to be checked until correct is found. Overriding this
	 * class and keeping local cache of previous indices might be faster.
	 * 
	 * @param x
	 * @param y
	 */
	public void set(byte value, int x, int y){

		Chunk c = findChunk(x, y);

		int xx = getCoordinateOnChunk(x, c.lowerLeftX);
		int yy = getCoordinateOnChunk(y, c.lowerLeftY);
		current = c;
		c.data[xx][yy] = value;

		c.lastUsed = iteration;
		iteration++;
	}

	/**
	 * Increments the value in given coordinates
	 * 
	 * @param x
	 * @param y
	 */
	public void increment(int x, int y){

		Chunk c = findChunk(x, y);

		int xx = getCoordinateOnChunk(x, c.lowerLeftX);
		int yy = getCoordinateOnChunk(y, c.lowerLeftY);
		current = c;
		c.data[xx][yy]++;
		c.lastUsed = iteration;
		iteration++;

	}

	/**
	 * Decrements the value in given coordinates
	 * 
	 * @param x
	 * @param y
	 */
	public void decrement(int x, int y){

		Chunk c = findChunk(x, y);

		int xx = getCoordinateOnChunk(x, c.lowerLeftX);
		int yy = getCoordinateOnChunk(y, c.lowerLeftY);
		current = c;
		c.data[xx][yy]--;
		c.lastUsed = iteration;
		iteration++;
	}

	/**
	 * gets data from a single field
	 */
	public byte get(int x, int y){
		Chunk c = findChunk(x, y);

		int xx = getCoordinateOnChunk(x, c.lowerLeftX);
		int yy = getCoordinateOnChunk(y, c.lowerLeftY);
		current = c;
		c.lastUsed = iteration;
		iteration++;
		return c.data[xx][yy];
	}

	/**
	 * gets a chunk index from a coordinate. Chunks are indexed by their lowest
	 * index from each side.
	 * 
	 * This method gets the index by converting the coordinate by rounding down
	 * to the nearest chunk. If a chunk size is 100, it means the lower indices
	 * are -50, -50 so chunk accessing coordinate 0,0 should map into index
	 * [50,50] in the actual chunk.
	 * 
	 * @param coordinate
	 * @return index of a chunk
	 */
	protected int getChunkIndex(int coordinate){
		int chunkIndex;
		if(coordinate >= 0){

			int intermediate = (coordinate + (SIZE / 2));
			chunkIndex = (coordinate - (intermediate % SIZE));
		}else{
			int intermediate = ((coordinate + 1) + (SIZE / 2));
			chunkIndex = ((coordinate + 1) - (intermediate % SIZE) - SIZE);
			if(intermediate > 0){
				chunkIndex += SIZE;
			}
		}
		return chunkIndex;
	}

	/**
	 * returns the index of a cell in the specific chunk.
	 * 
	 * @param coordinate
	 * @param chunkindex
	 * @return
	 */
	protected int getCoordinateOnChunk(int coordinate, int chunkindex){
		if(coordinate >= 0){
			return coordinate - chunkindex;
		}
		return (chunkindex * -1) - (coordinate * -1);

	}

	/*
	 * Returns a chunk where certain x y coordinate lies.
	 */
	private Chunk findChunk(int x, int y){
		if(current.contains(x, y)){
			return current;
		}
		//special cases where it is within the current's neighbor.
		if(current.up.contains(x, y)){
			return current;
		}
		if(current.down.contains(x, y)){
			return current;
		}
		if(current.left.contains(x, y)){
			return current;
		}
		if(current.right.contains(x, y)){
			return current;
		}

		int chunkIndex = getChunkIndex(x);
		int chunkIndexy = getChunkIndex(y);

		//iterate rest of the chunks
		for(Chunk c : chunks){
			if(c.contains(x, y)){
				return c;
			}
		}

		//not found, so we shall make a new one.
		Chunk c = new Chunk(chunkIndex, chunkIndexy, SIZE);
		try{
			c = loadOrCreateChunk(chunkIndex, chunkIndexy);
			connectChunkReferences(c);
		}catch (Exception e){
			// TODO This should be thrown further.
			e.printStackTrace();
		}

		return c;

	}

	/**
	 * checks if there exists a file for specific coordinates.
	 * 
	 * @param x
	 * @param y
	 * @return
	 */
	private boolean hasFile(int x, int y){

		if(!diskUsed){
			return false;
		}else{
			int a = getChunkIndex(x);
			int b = getChunkIndex(y);
			String name = a + "," + b;
			File f = new File(FileUtils.pathAssureSlash(chunkCacheRoot.getAbsolutePath()) + name);
			return f.exists();
		}
	}

	/**
	 * returs the list of currently loaded chunks
	 * 
	 * @return
	 */
	public LinkedList<Chunk> getChunks(){
		return chunks;
	}

	/**
	 * Connects a chunk to adjacent chunks. Usually called when chunk is loaded
	 * or created.
	 * 
	 * @param c
	 */
	protected void connectChunkReferences(Chunk c){
		int x = c.getX();
		int y = c.getY();
		for(Chunk cn : chunks){
			if(cn.getX() == x - SIZE && cn.getY() == y){
				cn.right = c;
				c.left = cn;
			}
			if(cn.getX() == x + SIZE && cn.getY() == y){
				cn.left = c;
				c.right = cn;
			}
			if(cn.getY() == y - SIZE && cn.getX() == x){
				cn.up = c;
				c.down = cn;
			}
			if(cn.getY() == y + SIZE && cn.getX() == x){
				cn.down = c;
				c.up = cn;
			}
		}
	}

	/**
	 * Removes all such chunks that are empty (which contain only zeros)
	 */
	private void cleanLoadedChunks(){

		LinkedList<Chunk> toRemove = new LinkedList<Chunk>();

		nextchunk:
		for(int i = 0; i < chunks.size(); i++){
			Chunk cn = chunks.get(i);
			cn.empty = true;
			byte[][] chunk = cn.data;
			for(int j = 0; j < chunk.length; j++){
				for(int k = 0; k < chunk[0].length; k++){
					if(chunk[j][k] != 0){
						cn.empty = false;
						continue nextchunk;
					}
				}
			}

			//if chunk is to be deleted, remove references from adjacent chunks
			if(cn.empty){
				cn.disconnect();
				toRemove.add(cn);
			}
		}

		for(Chunk r : toRemove){
			chunks.remove(r);
		}
	}

	/**
	 * TODO: throw exception loads a chunk from this filename if exists
	 *
	 * @param path
	 * @return
	 * @throws Exception
	 */
	private Chunk loadOrCreateChunk(int x, int y) throws Exception{
		//Return new chunk if there is nothing on disk.
		int xx = getChunkIndex(x);
		int yy = getChunkIndex(y);
		Chunk c = new Chunk(xx, yy, SIZE); //empty chunk is created for the index
		//if disk has not been accessed yet, no need to try and load a file
		if(!diskUsed){
			chunksCreated++;
			return c;
		}
		File f = new File(FileUtils.pathAssureSlash(chunkCacheRoot.getAbsolutePath()) + xx + "," + yy);
		//if disk is used and spesific file is found
		if(f.exists()){
			try{
				byte[] bytes = loadData(f.toPath());
				// System.out.println(bytes.length);
				byte[][] chunk = new byte[SIZE][SIZE];
				int byteIndex = 0;
				// chunk exists, so load the chunk into memory
				for(int i = 0; i < chunk.length; i++){
					for(int j = 0; j < chunk[i].length; j++){
						chunk[i][j] = bytes[byteIndex];
						byteIndex++;
					}
				}
				c.data = chunk;

				return c;

			}catch (IOException e){
				e.printStackTrace();
			}
			diskUsed = true;
		}
		return c;
	}

	private byte[] loadData(Path path) throws Exception{
		byte[] bytes = Files.readAllBytes(path);
		if(bytes.length != SIZE * SIZE){
			throw new UnsupportedDataTypeException("Data size: " + bytes.length + " expected: " + (SIZE * SIZE));
		}
		return null;
	}

	/**
	 * Writes a single chunk to disk.
	 * 
	 * @param chunk
	 * @param fileName
	 */
	private void saveChunk(byte[][] chunk, String fileName) throws Exception{
		diskUsed = true;
		File f = new File(fileName);
		System.out.println(fileName);
		if(f.exists()){
			f.delete();
		}else{
			f.mkdirs();
			f.delete();// horrible workaround
		}

		FileOutputStream fos = new FileOutputStream(f);
		for(int i = 0; i < chunk.length; i++){
			fos.write(chunk[i]);
		}
		fos.close();

	}

	/**
	 * Clears all data for this ruleset on the disk
	 * 
	 * @param r
	 */
	public void clearData(String id){
		chunks = new LinkedList<Chunk>();
		current = new Chunk(0 - SIZE / 2, 0 - SIZE / 2, SIZE);
		chunks.add(current);

		File f = new File(chunkCacheRoot + id);
		if(!f.exists()){
			return;
		}
		File[] fi = f.listFiles();
		for(File c : fi){
			c.delete();

		}
		f.delete();
	}

	//	@Deprecated
	/*
	 * just get the name from parameters that are inside chunknode.
	 */
	//	private String getFileName(int x, int y){
	//		String name = "";
	//		if(x >= 0){
	//			name += x - x % SIZE + ",";
	//		}else{
	//			name += ((1 + x) / SIZE - 1) * SIZE + ","; // this might be a bit odd and might not work with everything
	//
	//		}
	//		if(y >= 0){
	//			name += y - y % SIZE;
	//		}else{
	//			name += ((1 + y) / SIZE - 1) * SIZE;
	//		}
	//
	//		return name;
	//	}

	/**
	 * Returns the total raw size of bytes for this byte array
	 * 
	 * @return
	 */
	public long getTotalSizeBytes(){
		return SIZE * SIZE * chunksCreated;
	}

	/**
	 * Clean loaded chunks and save rest of them to disk.
	 */
	public void dump() throws Exception{
		cleanLoadedChunks();
		for(Chunk c : chunks){
			saveChunk(c.data, chunkCacheRoot + id + "/" + (c.lowerLeftX) + "," + (c.lowerLeftY));
		}
	}

	public void convert(int toSize){
		System.out.println("TODO: convert not implemented");
	}

	public void trim(int minx, int miny, int maxx, int maxy){
		//todo: for all chunks on disk, if they are not within area, delete.
		//todo: for all chunks on disk, if they are partially on area, set remaining parts to zero.
		System.out.println("TODO: trim not implemented");
	}

	/*
	 * wtf
	 */
	public byte[] zip(boolean getBytes, boolean toDisk){
		byte[] bytes = new byte[0];
		return getBytes ? bytes : null;
	}
	//---------------- STATIC METHODS -----------------------//

	/**
	 * sets root for chunk data. for a certain BigByteArray. This method is
	 * static and thread safe for multiple ByteArrays
	 * 
	 * @param root
	 *            A root directory
	 * @param ref
	 *            the ByteArray calling the method
	 */
	private static synchronized void setRoot(File root, BigByteArray ref) throws IOException{

		if(root.exists() && root.isFile())
			throw new IOException("root must be a directory!");
		if(!root.exists()){
			boolean success = root.mkdirs();
			if(!success){
				throw new IOException("Root could not be created! Path: " + root.getAbsolutePath());
			}
		}
		ref.chunkCacheRoot = root;
	}

	/**
	 * sets the id after removing special characters from it. Id is used as
	 * folder so it must be alphanumerical
	 * 
	 * @param id
	 */
	private static String trimID(String id){
		return id.replaceAll(idRegex, "");
	}

	//---------------- CHUNK CLASS-----------------------//

	/**
	 * Represents a single array in the (in)finite grid. Has references to
	 * adjacent chunks for fast access.
	 * 
	 * @author Vilho
	 *
	 */
	public class Chunk{

		public Chunk(int i, int j, int size){
			data = new byte[size][size];
			lowerLeftX = i;
			lowerLeftY = j;
			this.size = size;
		}

		protected byte[][] data;

		/*
		 * Neighboring chunks
		 */
		protected Chunk up;
		protected Chunk down;
		protected Chunk left;
		protected Chunk right;

		/*
		 * Assuming 0,0 is origo and positive values expand up and right on a
		 * traditional coordinates
		 */
		private int lowerLeftX;
		private int lowerLeftY;

		/*
		 * The iteration when this chunk was accessed previously
		 */
		private long lastUsed; //potential overflow if program is running for 292 years assuming 1 iteration per nanosecond.

		/*
		 * size of this chunk. used for calculating if certain value is within
		 * bounds.
		 */
		private int size;

		/*
		 * denotes if this chunk has no values in it. This is not change during
		 * runtime on every access (which is a bit bad) but will be marked empty
		 * while doing cleaning.
		 */
		private boolean empty;

		public byte[][] getData(){
			// TODO Auto-generated method stub
			return data;
		}

		public int getX(){
			return lowerLeftX;
		}

		public int getSize(){
			return size;
		}

		public void setIndexx(int indexx){
			this.lowerLeftX = indexx;
		}

		public int getY(){
			return lowerLeftY;
		}

		public void setIndexy(int indexy){
			this.lowerLeftY = indexy;
		}

		public Chunk getUp(){
			return up;
		}

		public Chunk getDown(){
			return down;
		}

		public Chunk getLeft(){
			return left;
		}

		public Chunk getRight(){
			return right;
		}

		public void setUp(Chunk up){
			this.up = up;
		}

		public void setDown(Chunk down){
			this.down = down;
		}

		public void setLeft(Chunk left){
			this.left = left;
		}

		public void setRight(Chunk right){
			this.right = right;
		}

		/**
		 * Removes the chunks connections from adjacent chunks
		 */
		private void disconnect(){
			if(left != null){
				left.right = null;
			}
			if(right != null){
				right.left = null;
			}
			if(up != null){
				up.down = null;
			}
			if(down != null){
				down.up = null;
			}
		}

		/**
		 * returns wether or not this chunk begins from these coordinates.
		 * 
		 * @param xx
		 * @param yy
		 * @return
		 */
		public boolean matches(int xx, int yy){
			return this.lowerLeftX == xx && this.lowerLeftY == yy;
		}

		/**
		 * returns whether or not a certain coordinate is within the bounds of
		 * this chunk.
		 * 
		 * @param x
		 * @param y
		 * @return
		 */
		public boolean contains(int x, int y){
			if(x >= lowerLeftX && x < lowerLeftX + size && y >= lowerLeftY && y < lowerLeftY + size){
				return true;
			}
			return false;
		}

		@Override
		public String toString(){
			return lowerLeftX + "," + lowerLeftY;
		}
	}

	//---------------- SWAPPINGPOLICIES -----------------------//

	/*
	 * These are not currently in use.
	 */
	public enum SwappingPolicy {
		LRU, //Least recently used
		FIFO, //first in first out
		RR; //random replacement
	}

	/*
	 * For assuring path integrity
	 */
	private static class FileUtils{

		private static String pathRemoveSlash(String path){
			if(path.endsWith("/") || path.endsWith("\\")){
				return path.substring(0, path.length() - 1);
			}
			return path;
		}

		private static String pathAssureSlash(String path){
			if(!path.endsWith("/") && !path.endsWith("\\")){
				return path + "/";
			}
			return path;
		}

	}

}
