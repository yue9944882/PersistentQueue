package com.kimmin.util.queue;

/**
 * Created by min.jin on 2016/2/19.
 */

import sun.nio.ch.DirectBuffer;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

import static java.nio.channels.FileChannel.MapMode.READ_WRITE;


import sun.nio.ch.DirectBuffer;

import java.io.*;

import java.nio.channels.FileChannel;
import java.nio.MappedByteBuffer;
import java.nio.ByteBuffer;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

import static java.nio.channels.FileChannel.MapMode.READ_WRITE;


/**
 * Created by min.jin on 2016/1/29.
 */

// To map A in-memory queue to disk storage via managing physical stores on specific directories

public class MappedFileQueue<T extends Serializable> implements PersistentQueue<T>{

    public MappedFileQueue(){
    }

    private static final int PAGE_SIZE_MB=128;
    private static final int FILE_CLEAN_INTERVAL_SECONDS= 20;
    /** Page size in bytes **/
    private static final int PAGE_SIZE=1024*1024*PAGE_SIZE_MB;
    /** 32 MB Object size limitation **/
    private static final int OBJECT_SIZE_LIMIT=1024*1024*32;


    private RandomAccessFile readDataFile;
    private RandomAccessFile writeDataFile;
    private RandomAccessFile indexFile;

    private FileChannel readDataChannel;
    private FileChannel writeDataChannel;
    private FileChannel indexChannel;

    private MappedByteBuffer readMbb;
    private MappedByteBuffer writeMbb;
    private MappedByteBuffer indexMbb;

    private boolean backlogAvailable=false;

    final private int headerLength=5;
    final private byte READ=(byte)0;
    final private byte NOT_READ=(byte)1;
    final private byte MM_EOF=(byte)2;

    final private ByteBuffer header=ByteBuffer.allocate(headerLength);
    final private int endingLength=5;

    private int readFileIndex=0;
    private int writeFileIndex=0;

    final private static int SIZE_OF_INT=4;

    private Timer fileCleanTimer;
    private long maxFileSize;

    final private static String DATA_FILE_SUFFIX=".dat";
    final private static String INDEX_FILE_SUFFIX=".idx";

    private String fileName;
    private File queueDir;

    class FileCleanTask extends TimerTask {

        // Checking directory and find out those already used files
        public void run(){
            File[] queueFiles=queueDir.listFiles();
            List<File> toBeDeletedFiles=new ArrayList<File>();
            if(queueDir!=null&&queueFiles.length>0){
                for(File queueFile: queueFiles){
                    String fileName=queueFile.getName();
                    if(fileName.endsWith(DATA_FILE_SUFFIX)){
                        int beginIndex=fileName.lastIndexOf('-');
                        beginIndex++;
                        int endIndex=fileName.lastIndexOf(DATA_FILE_SUFFIX);
                        String szIndex=fileName.substring(beginIndex,endIndex);
                        // Potential danger of unclean dir
                        int index = Integer.parseInt(szIndex);
                        if(readFileIndex<=writeFileIndex){
                            if(index<readFileIndex||index>writeFileIndex){
                                toBeDeletedFiles.add(queueFile);
                            }
                        }else{
                            if(index<readFileIndex&&index>writeFileIndex){
                                toBeDeletedFiles.add(queueFile);
                            }
                        }
                    }
                }
            }
            for(File file: toBeDeletedFiles){
                file.delete();
            }
        }
    }

    public MappedFileQueue(String dir,String queueName,int maxSize) throws IOException {
        // Checking if the parameter is correct
        if(dir==null||dir.trim().length()==0){
            throw new IllegalArgumentException("Parameter - dir - is Empty!");
        }
        if(queueName==null||queueName.trim().length()==0){
            throw new IllegalArgumentException("Parameter - queueName - is Empty!");
        }

        // Formatting argument file name & queue name
        String dirName=dir;
        if(!dirName.endsWith("/")){
            dirName+=File.separator;
        }

        dirName+=queueName;
        queueDir=new File(dirName);
        if(!queueDir.exists()){
            queueDir.mkdirs();
        }
        this.fileName=queueDir.getPath();
        if(!fileName.endsWith("/")){
            this.fileName+=File.separator;
        }
        this.fileName+="data";
        if(maxSize<0||maxSize%PAGE_SIZE_MB!=0){
            throw new IllegalArgumentException("Member - maxFileSize - must be a positive number and a multiple of "+PAGE_SIZE_MB+" MB");
        }
        this.maxFileSize=maxSize;

        // Do initialization
        this.init();
    }

    private void init() throws IOException{
        this.initIndex();
        this.initReadBuffer();
        this.initWriteBuffer();
        // Init scanning directory timer
        fileCleanTimer=new Timer();
        fileCleanTimer.schedule(new FileCleanTask(),1000,FILE_CLEAN_INTERVAL_SECONDS*1000);
    }

    private void initIndex() throws IOException{
        indexFile=new RandomAccessFile(this.fileName+INDEX_FILE_SUFFIX,"rw");
        indexChannel=indexFile.getChannel();
        indexMbb=indexChannel.map(FileChannel.MapMode.READ_WRITE,0,SIZE_OF_INT*2);
        readFileIndex=indexMbb.getInt();
        writeFileIndex=indexMbb.getInt();
        indexMbb.position(0);
    }

    private void initReadBuffer() throws IOException{
        readDataFile=new RandomAccessFile(this.fileName+"-"+readFileIndex+DATA_FILE_SUFFIX,"rw");
        readDataChannel=readDataFile.getChannel();
        readMbb=readDataChannel.map(FileChannel.MapMode.READ_WRITE,0,PAGE_SIZE);
        int position=readMbb.position();
        byte active=readMbb.get();
        int length=readMbb.getInt();

        while(active==READ&&length>0){
            readMbb.position(position+headerLength+length);
            position=readMbb.position();
            active=readMbb.get();
            length=readMbb.getInt();
        }

        if(active==NOT_READ){
            backlogAvailable=true;
        }
        readMbb.position(position);
    }

    private void initWriteBuffer() throws IOException{
        writeDataFile=new RandomAccessFile(this.fileName+"-"+writeFileIndex+DATA_FILE_SUFFIX,"rw");
        writeDataChannel=writeDataFile.getChannel();
        writeMbb=writeDataChannel.map(FileChannel.MapMode.READ_WRITE,0,PAGE_SIZE);
        int position=writeMbb.position();
        byte active=writeMbb.get();
        int length=writeMbb.getInt();
        while(length>0){
            writeMbb.position(position+headerLength+length);
            position=writeMbb.position();
            active=writeMbb.get();
            length=writeMbb.getInt();
        }
        writeMbb.position(position);
    }

    @Override
    public T consume() {
        return this.consumeFromDiskFile();
    }

    @Override
    public boolean produce(T t) {
        return this.produceToDiskFile(t);
    }


    private synchronized T consumeFromDiskFile(){
        try{
            int currentPosition=readMbb.position();
            byte active=readMbb.get();
            int length;

            if(active==MM_EOF){
                unMap(readMbb);
                closeResource(readDataChannel);
                closeResource(writeDataFile);

                if(readFileIndex==Integer.MAX_VALUE){
                    readFileIndex=0;
                }else{
                    readFileIndex+=1;
                }
                readDataFile=new RandomAccessFile(this.fileName+"-"+readFileIndex+DATA_FILE_SUFFIX,"rw");
                readDataChannel=readDataFile.getChannel();
                readMbb=readDataChannel.map(FileChannel.MapMode.READ_WRITE,0,PAGE_SIZE);
                indexMbb.putInt(0,readFileIndex);
                currentPosition=readMbb.position();
                active=readMbb.get();
                length=readMbb.getInt();
            }else{
                length=readMbb.getInt();
            }

            if(length<=0){
                readMbb.position(currentPosition);
                return null;
            }

            byte[] bytes=new byte[length];
            readMbb.get(bytes);
            readMbb.put(currentPosition,READ);
            return (T)toObject(bytes);

        }catch(Throwable e){
            e.printStackTrace();
            return null;
        }
    }

    private static void unMap(MappedByteBuffer buffer){
        if(buffer==null)return;
        sun.misc.Cleaner cleaner=((DirectBuffer)buffer).cleaner();
        if(cleaner!=null){
            cleaner.clean();
        }
    }

    private synchronized boolean produceToDiskFile(T t){
        try{
            int backFileSize=this.getBackFileSize();
            if(backFileSize>maxFileSize){
                return false;
            }
            byte[] oBytes=getBytes(t);
            int length=oBytes.length;
            if(length==0){
                return false;
            }
            if(length>OBJECT_SIZE_LIMIT){
                return false;
            }
            header.clear();
            header.put(NOT_READ);
            header.putInt(length);
            header.flip();
            if(writeMbb.remaining()<headerLength+length+endingLength){
                writeMbb.put(MM_EOF); // the end
//                writeMbb.force();
                unMap(writeMbb);
                closeResource(writeDataChannel);
                closeResource(writeDataFile);

                if (writeFileIndex == Integer.MAX_VALUE) {
                    writeFileIndex = 0;
                } else {
                    writeFileIndex += 1;
                }
                writeDataFile = new RandomAccessFile(this.fileName + "-" + writeFileIndex + DATA_FILE_SUFFIX, "rw");
                writeDataChannel = writeDataFile.getChannel();
                writeMbb = writeDataChannel.map(READ_WRITE, 0, PAGE_SIZE); // start the write buffer with writePosition 0 initially
                indexMbb.putInt(SIZE_OF_INT, writeFileIndex); // update write file index
            }
            writeMbb.put(header);
            writeMbb.put(oBytes);
            return true;
        }catch(Throwable e){
            e.printStackTrace();
            return false;
        }
    }

    private static void closeResource(Closeable c){
        try{
            if(c!=null)c.close();
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    private <T> T toObject(byte[] bytes) throws IOException,ClassNotFoundException{
        ByteArrayInputStream bis=null;
        ObjectInputStream ois=null;
        try{
            bis=new ByteArrayInputStream(bytes);
            ois=new ObjectInputStream(bis);
            return (T) ois.readObject();
        }finally {
            closeResource(bis);
            closeResource(ois);
        }
    }

    private byte[] getBytes(T o) throws IOException{
        ByteArrayOutputStream bos=null;
        ObjectOutputStream oos=null;
        try{
            bos=new ByteArrayOutputStream();
            oos=new ObjectOutputStream(bos);
            oos.writeObject(o);
            oos.flush();
            return bos.toByteArray();
        }finally{
            closeResource(bos);
            closeResource(oos);
        }
    }

    @Override
    public long getRemainingCapacity(){
        /** To be completed **/
        return Integer.MAX_VALUE;
    }
    @Override
    public long getTotalCapacity(){
        /** To be completed **/
        return maxFileSize;
    }

    public int getBackFileSize(){
        if(writeFileIndex>=readFileIndex){
            return (writeFileIndex-readFileIndex+1)*PAGE_SIZE_MB;
        }else{
            return (Integer.MAX_VALUE-readFileIndex+writeFileIndex+2)*PAGE_SIZE_MB;
        }
    }

    @Override
    public boolean isEmpty(){
        return this.readFileIndex==this.writeFileIndex&&readMbb.position()==writeMbb.position();
    }

    public boolean isBacklogAvailable(){
        return backlogAvailable;
    }

    public void shutdown(){
        fileCleanTimer.cancel();

        if (writeMbb != null) {
            writeMbb.force();
            unMap(writeMbb);
        }
        if (readMbb != null) {
            readMbb.force();
            unMap(readMbb);
        }
        if (indexMbb != null) {
            indexMbb.force();
            unMap(indexMbb);
        }

        closeResource(readDataChannel);
        closeResource(readDataFile);
        closeResource(writeDataChannel);
        closeResource(writeDataFile);
        closeResource(indexChannel);
        closeResource(indexFile);
    }

    @Override
    public long getOverflowCount(){
        return 0;
    }

    @Override
    public long getUsedCapacity(){
        return getBackFileSize();
    }

}

