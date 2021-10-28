import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
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

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>
    {
        static enum CountersEnum
        {
            INPUT_WORDS
        }
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
            if (conf.getBoolean("wordcount.skip.patterns", false)) {
                URI[] patternsURIs = Job.getInstance(conf).getCacheFiles();
                for (URI patternsURI : patternsURIs) {
                    Path patternsPath = new Path(patternsURI.getPath());
                    String patternsFileName = patternsPath.getName().toString();
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
        // 用来判断字符串是否为数字
        Pattern pattern = Pattern.compile("^[-\\+]?[\\d]*$");
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException
        {
            String line = (caseSensitive) ? value.toString() : value.toString().toLowerCase();
            for (String pattern : patternsToSkip)
            {
                line = line.replaceAll(pattern, " ");
            }
            /*读取停用词:*/
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
            //File punctuation = new File("punctuation.txt");
            //StringTokenizer itr = new StringTokenizer(value.toString(),punctuation+"\t\n\r");
            while (itr.hasMoreTokens())
            {
                String hhh=itr.nextToken();
                word.set(hhh);
                //word.set(itr.nextToken());
                // 如果是数字则不保存
                if (pattern.matcher(hhh).matches())
                {
                    continue;
                }
                if(!stopWordsSet.contains(hhh) && word.getLength()>=3)
                {
                    context.write(word, one);
                }
                Counter counter = context.getCounter(
                        CountersEnum.class.getName(),
                        CountersEnum.INPUT_WORDS.toString());
                counter.increment(1);
            }
        }
    }
    private static class IntWritableDecreasingComparator extends IntWritable.Comparator {
        public int compare(WritableComparable a, WritableComparable b) {
            //System.out.println("ss");
            return -super.compare(a, b);
        }
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            //System.out.println("ss1");
            return -super.compare(b1, s1, l1, b2, s2, l2);
        }
    }
    public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable>
    {
        private IntWritable result = new IntWritable();
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
        {
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
            // conf.set("fs.defaultFS", "hdfs://localhost:9000");
            // conf.set("fs.hdfs.omp", "org.apache.hadoop.hdfs.DistributedFileSystem");
            FileSystem fs=FileSystem.get(conf);
            FSDataOutputStream os=fs.create(new Path(name));
            BufferedReader br=new BufferedReader(new FileReader("output//part-r-00000"));
            String str=null;
            int k=0;
            while((str=br.readLine())!=null){
                k=k+1;
                //System.out.println("hhhhhhhhh  " + str);
                String[] str2=str.split("\t");
                str=k+":"+" "+str2[1]+","+str2[0]+"\n";
                byte[] buff=str.getBytes();
                os.write(buff,0,buff.length);
                /*if(n<=10){
                    System.out.println(str);
                }n++;*/
                if(k==100)
                    break;
            }
            br.close();
            os.close();
            fs.close();
        }
        catch(Exception e)
        {
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
        Path tempDir = new Path("wordcount-temp1-" + Integer.toString(new Random().nextInt(Integer.MAX_VALUE))); //定义一个临时目录
        Job job = Job.getInstance(conf, "word count");
        //Job job = new Job(conf, "word count");
        job.setJarByClass(WordCount.class);
        try {
            job.setMapperClass(TokenizerMapper.class);
            job.setCombinerClass(IntSumReducer.class);
            job.setReducerClass(IntSumReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            List<String> otherArgs = new ArrayList<String>();
            for (int i = 0; i < remainingArgs.length; ++i) {
                if ("-skip".equals(remainingArgs[i])) {
                    for (int k = i + 1; k < remainingArgs.length; ++k) {
                        job.addCacheFile(new Path(remainingArgs[k]).toUri());
                        job.getConfiguration().setBoolean("wordcount.skip.patterns", true);
                    }
                } else {
                    otherArgs.add(remainingArgs[i]);
                }
            }
            FileInputFormat.addInputPath(job, new Path("shak-txt"));
            FileOutputFormat.setOutputPath(job, tempDir);//先将词频统计任务的输出结果写到临时目录中, 下一个排序任务以临时目录为输入目录
            job.setOutputFormatClass(SequenceFileOutputFormat.class);
            if (job.waitForCompletion(true)) {
                Job sortJob = Job.getInstance(conf, "sort");
                //Job sortJob = new Job(conf, "sort");
                sortJob.setJarByClass(WordCount.class);
                FileInputFormat.addInputPath(sortJob, tempDir);
                sortJob.setInputFormatClass(SequenceFileInputFormat.class);
                /*InverseMapper由hadoop库提供，作用是实现map()之后的数据对的key和value交换*/
                sortJob.setMapperClass(InverseMapper.class);
                /*将 Reducer 的个数限定为1, 最终输出的结果文件就是一个*/
                sortJob.setNumReduceTasks(1);
                FileOutputFormat.setOutputPath(sortJob, new Path("output"));
                sortJob.setOutputKeyClass(IntWritable.class);
                sortJob.setOutputValueClass(Text.class);
                sortJob.setSortComparatorClass(IntWritableDecreasingComparator.class);
                // 实现数据对的key和value交换
                // sortJob.setMapperClass(InverseMapper.class);
                if (sortJob.waitForCompletion(true)) {
                    Write("result");
                } else {
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
