package com.nextmining.course.hadoop.hdfs;

import java.io.InputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class FileSystemCat {

    public static void main(String[] args) throws Exception {
        //String uri = args[0];
        String uri = "hftp://nextmining04:50070/coll/input/ncdc/metadata/stations-fixed-width.txt";
        //String uri = "/coll/input/ncdc/metadata/stations-fixed-width.txt";
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri), conf);
        InputStream in = null;
        try {
            in = fs.open(new Path(uri));
            IOUtils.copyBytes(in, System.out, 4096, false);
        } finally {
            IOUtils.closeStream(in);
        }
    }
}
