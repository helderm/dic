package org.helderm.dic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created by helder on 8/31/16.
 */
public class TopTen{

    public static class TopTenMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
        // Stores a map of user reputation to the record
        public TreeMap<Double, Text> repToRecordMap = new TreeMap<Double, Text>();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] lines = value.toString().split(System.getProperty("line.separator"));

            for (String line : lines) {
                if (line.toString().contains("xml version") ||
                    (line.toString().contains("<users>")) ||
                    (line.toString().contains("</users>"))){

                    continue;
                }

                TopTen.addRecordToMap(repToRecordMap, line);

                // If we have more than ten records, remove the one with the lowest reputation.
                if (repToRecordMap.size() > 10) {
                    repToRecordMap.remove(repToRecordMap.firstKey());
                }
           }

            for (Map.Entry<Double, Text> entry : repToRecordMap.entrySet()) {
                context.write(NullWritable.get(), entry.getValue());
            }
         }
    }

    public static class TopTenReducer extends Reducer<NullWritable, Text, NullWritable, Text> {
        // Stores a map of user reputation to the record
        private TreeMap<Double, Text> repToRecordMap = new TreeMap<Double, Text>();

        public void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text val : values) {
                TopTen.addRecordToMap(repToRecordMap, val.toString());
                // If we have more than ten records, remove the one with the lowest reputation
                if (repToRecordMap.size() > 10) {
                    repToRecordMap.remove(repToRecordMap.firstKey());
                }
            }

            for (Text t : repToRecordMap.descendingMap().values()) {
                // Output our ten records to the file system with a null key
                context.write(NullWritable.get(), t);
            }
        }

   }

   public static Map<String, String> transformXmlToMap(String xml) {
       Document doc = null;
       try {
           DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
                factory.setNamespaceAware(false);

           DocumentBuilder bldr = factory.newDocumentBuilder();


           doc = bldr.parse(new ByteArrayInputStream(xml.getBytes()));
       } catch (Exception e) {
           e.printStackTrace();
           System.out.println("-- xml = " + xml);
           return null;
       }

       Map<String, String> map = new HashMap<String, String>();
       NamedNodeMap attributeMap = doc.getDocumentElement().getAttributes();

       for (int i = 0; i < attributeMap.getLength(); ++i) {
           Attr n = (Attr) attributeMap.item(i);

           map.put(n.getName(), n.getValue());
       }

       return map;
   }

   public static void addRecordToMap(TreeMap<Double, Text> map, String record) {
       Map<String, String> parsed = TopTen.transformXmlToMap(record);

       // Add this record to our map with the reputation as the key
       Integer userId = Integer.parseInt(parsed.get("Id"));
       Integer reputation = Integer.parseInt(parsed.get("Reputation"));

       Double mapKey = reputation + (userId * Math.pow(10.0, -6.0));

       map.put(mapKey, new Text(record));
   }

   public static void main(String[] args) throws Exception {
       Configuration conf = new Configuration();

       Job job = new Job(conf, "topten");
       MultiLineInputFormat.setNumLinesPerSplit(job, 1000);

       job.setOutputKeyClass(Text.class);
       job.setOutputValueClass(IntWritable.class);

       job.setMapperClass(TopTenMapper.class);
       job.setReducerClass(TopTenReducer.class);

       job.setInputFormatClass(MultiLineInputFormat.class);
       job.setOutputFormatClass(TextOutputFormat.class);

       job.setMapOutputKeyClass(NullWritable.class);
       job.setMapOutputValueClass(Text.class);
       job.setOutputKeyClass(NullWritable.class);
       job.setOutputValueClass(Text.class);

       FileInputFormat.addInputPath(job, new Path(args[0]));
       FileOutputFormat.setOutputPath(job, new Path(args[1]));

       job.waitForCompletion(true);
   }
}
