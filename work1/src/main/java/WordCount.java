import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;

import java.io.*;
import java.net.URI;
import java.util.*;
import java.util.regex.Pattern;

public class WordCount {

    public static Set<String> pathNameSet = new HashSet<String>();
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>
    {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        private boolean caseSensitive;
        private Set<String> patternsToSkip = new HashSet<String>();
        private Configuration conf;
        private BufferedReader fis;

        private Scanner scanner;
        private Set<String> stopWordsSet = new HashSet<String>();

        @Override
        public void setup(Context context) throws IOException, InterruptedException
        {
            conf = context.getConfiguration();
            caseSensitive = conf.getBoolean("wordcount.case.sensitive", false);
            if (conf.getBoolean("wordcount.skip.patterns", false))
            {
                URI[] patternsURIs = Job.getInstance(conf).getCacheFiles();
                for (URI patternsURI : patternsURIs) {
                    Path patternsPath = new Path(patternsURI.getPath());
                    String patternsFileName = patternsPath.getName();
                    parseSkipFile(patternsFileName);
                }
            }
        }
        private void parseSkipFile(String fileName)
        {
            try {
                fis = new BufferedReader(new FileReader(fileName));
                String pattern = null;
                while ((pattern = fis.readLine()) != null)
                {
                    patternsToSkip.add(pattern);
                }
            } catch (IOException ioe) {
                System.err
                        .println("Caught exception while parsing the cached file '"
                                + StringUtils.stringifyException(ioe));
            }
        }
        // ????????????????????????????????????
        Pattern pattern = Pattern.compile("^[-\\+]?[\\d]*$");
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException
        {
            //???????????????????????????
            FileSplit inputSplit = (FileSplit) context.getInputSplit();
            String pathname = inputSplit.getPath().getName();
            pathNameSet.add(pathname);

            String line = (caseSensitive) ? value.toString() : value.toString().toLowerCase();
            for (String pattern : patternsToSkip)
            {
                line = line.replaceAll(pattern, " ");
            }
            /*???????????????:*/
            File fileStopWord = new File("stop-word-list.txt");
            try
            {
                scanner = new Scanner(fileStopWord);
                String stringStopWord = scanner.nextLine();
                for (; scanner.hasNextLine(); stringStopWord = scanner.nextLine())
                    stopWordsSet.add(stringStopWord);
            }
            catch (FileNotFoundException e)
            {
                e.printStackTrace();
            }
            StringTokenizer itr = new StringTokenizer(line);
            while (itr.hasMoreTokens())
            {
                String hhh=itr.nextToken();
                word.set(hhh);
                //word.set(itr.nextToken());
                //???????????????????????????
                if (pattern.matcher(hhh).matches())
                {
                    continue;
                }
                if(!stopWordsSet.contains(hhh) && word.getLength()>=3)
                {
                    String hhh1 = pathname + " " + hhh;
                    context.write(new Text(hhh1), one);
                }
            }
        }
    }
    public static class Partition extends Partitioner<Text, IntWritable>
    {
        @Override
        public int getPartition(Text key, IntWritable value, int numPartitions)
        {
            String[] key2 = key.toString().split(" ");
            int partition=40;
            int k=0;
            for (String i : pathNameSet)
            {
                if(key2[0].equals(i))
                    partition=k;
                k=k+1;
            }
            return partition;
        }
    }
    public static class Partition2 extends Partitioner<IntWritable, Text>
    {
        @Override
        public int getPartition(IntWritable value, Text key, int numPartitions)
        {
            String[] key2 = key.toString().split(" ");
            int partition=40;
            int k=0;
            for (String i : pathNameSet)
            {
                if(key2[0].equals(i))
                    partition=k;
                k=k+1;
            }
            return partition;
        }
    }
    private static class IntWritableDecreasingComparator extends IntWritable.Comparator
    {
        public int compare(WritableComparable a, WritableComparable b)
        {
            return -super.compare(a, b);
        }
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)
        {
            return -super.compare(b1, s1, l1, b2, s2, l2);
        }
    }
    public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable>
    {
        private IntWritable result = new IntWritable();
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
        {
            //System.out.println("hhhhhhhhhh "+key);
            String[] key2 = key.toString().split(" ");
            int sum = 0;
            for (IntWritable val : values)
            {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }
    public static void Write(String name)
    {
        try
        {
            Configuration conf=new Configuration();
            //conf.set("fs.defaultFS", "hdfs://localhost:9000");
            //conf.set("fs.hdfs.omp", "org.apache.hadoop.hdfs.DistributedFileSystem");
            FileSystem fs=FileSystem.get(conf);
            FSDataOutputStream os=fs.create(new Path(name));
            for(int i=0;i<40;i++)
            {
                String str_i=i+"";
                BufferedReader br;
                if(i<10)
                    br=new BufferedReader(new FileReader("output//part-r-0000"+str_i));
                else
                    br=new BufferedReader(new FileReader("output//part-r-000"+str_i));
                String str_name;
                String str;
                int k=0;
                while((str=br.readLine())!=null)
                {
                    k=k+1;
                    System.out.println("hhhhhhhhh  " + str);
                    String[] str1=str.split(" ");
                    String[] str2=str1[0].split("\t");
                    if(k==1)
                    {
                        str_name=str2[1]+"\n";
                        byte[] buff=str_name.getBytes();
                        os.write(buff,0,buff.length);
                    }
                    str=k+":"+" "+str1[1]+","+str2[0]+"\n";
                    byte[] buff=str.getBytes();
                    os.write(buff,0,buff.length);
                    if(k==100)
                        break;
                }
                String str_fin="---------------------------"+"\n";
                byte[] buff=str_fin.getBytes();
                os.write(buff,0,buff.length);
                br.close();
            }
            os.close();
            fs.close();
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();
        GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
        String[] remainingArgs = optionParser.getRemainingArgs();
//        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
//        if (otherArgs.length != 2) {
//            System.err.println("Usage: wordcount <in> <out>");
//            System.exit(2);
//        }
        //????????????????????????
        Path tempDir = new Path("wordcount-temp1-" + Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));
        Job job = Job.getInstance(conf, "seperate word count");
        job.setJarByClass(WordCount.class);
        try
        {
            job.setMapperClass(TokenizerMapper.class);
            job.setCombinerClass(IntSumReducer.class);
            job.setReducerClass(IntSumReducer.class);

            job.setPartitionerClass(Partition.class);
            job.setNumReduceTasks(40);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            List<String> otherArgs = new ArrayList<String>();
            for (int i = 0; i < remainingArgs.length; ++i)
            {
                if ("-skip".equals(remainingArgs[i]))
                {
                    for (int k = i+1; k < remainingArgs.length; ++k)
                    {
                        job.addCacheFile(new Path(remainingArgs[k]).toUri());
                        job.getConfiguration().setBoolean("wordcount.skip.patterns",true);
                    }
                }
                else
                {
                    otherArgs.add(remainingArgs[i]);
                }
            }
            //MultipleInputs.addInputPath(job, new Path("shak-txt"), TextInputFormat.class);
            FileInputFormat.addInputPath(job, new Path("shak-txt"));
            FileOutputFormat.setOutputPath(job, tempDir);//????????????????????????????????????????????????????????????, ???????????????????????????????????????????????????
            job.setOutputFormatClass(SequenceFileOutputFormat.class);
            if(job.waitForCompletion(true))
            {
                Job sortJob = Job.getInstance(conf, "sort");
                sortJob.setJarByClass(WordCount.class);
                FileInputFormat.addInputPath(sortJob, tempDir);
                sortJob.setInputFormatClass(SequenceFileInputFormat.class);
                /*InverseMapper???hadoop???????????????????????????map()?????????????????????key???value??????*/
                sortJob.setMapperClass(InverseMapper.class);
                sortJob.setPartitionerClass(Partition2.class);
                /*??? Reducer ??????????????????1, ???????????????????????????????????????*/
                sortJob.setNumReduceTasks(40);
                FileOutputFormat.setOutputPath(sortJob,new Path("output"));
                sortJob.setOutputKeyClass(IntWritable.class);
                sortJob.setOutputValueClass(Text.class);
                sortJob.setSortComparatorClass(IntWritableDecreasingComparator.class);
                if(sortJob.waitForCompletion(true))
                {
                    Write("result");
                }
                else{
                    System.out.println("1-- not");
                    System.exit(1);
                }
                FileSystem.get(conf).deleteOnExit(tempDir);
                System.exit(sortJob.waitForCompletion(true) ? 0 : 1);
            }
        } catch (IllegalStateException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}

