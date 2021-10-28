# 注意：输出结果存放在work1/result.txt和work2/result.txt文件中！



## 作业5 - 编写MapReduce程序进行词频统计并排序</font>

#### 1 作业简述

​       在HDFS上加载莎士比亚文集的数据文件（shakespeare-txt.zip解压后目录下的所有文件），编写MapReduce程序进行词频统计，并按照单词出现次数从大到小排列。输出：

- 每个作品的前100个高频单词；
- 所有作品的前100个高频单词；

​       要求忽略大小写，忽略标点符号（punctuation.txt），忽略停词（stop-word-list.txt），忽略数字，单词长度>=3。输出格式为"<排名>：<单词>，<次数>“，输出可以根据作品名称不同分别写入不同的文件，也可以合并成一个文件。

#### 2 设计思路

##### 2.1 功能及难点分析

核心功能：

- 对单词进行词频统计
- 对<key,value>进行降序排序

难点：

- 两个不同功能job的串行处理

- 在一个job里对文件分别进行mapreduce操作

##### 2.2 所有作品的前100个高频单词

​       选择首先对所有作品进行词频统计，统计一个文件后多个文件的操作同理。

- **总体结构**

​       Mapper：

```java
public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>
{
    @Override
    public void setup(Context context) throws IOException, InterruptedException{...}
    
    private void parseSkipFile(String fileName){...}
    
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException{...}    
}
```

​       Reducer：

```java
public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable>
{
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{...}
}
```

​        排序类：

```java
private static class IntWritableDecreasingComparator extends IntWritable.Comparator
{
    public int compare(WritableComparable a, WritableComparable b){...}
    public int compare(byte[] b1,int s1,int l1,byte[] b2,int s2,int l2){...}
}
```

​        写入结果函数：

```java
public static void Write(String name){...}
```

​        main 函数：

```java
public static void main(String[] args) throws Exception{...}
```

​     

- **具体实现细节**

​       忽略大小写（map函数）：将读入的字符串使用 toLowerCase() 全部转换为小写

```Java
  String line = (caseSensitive) ? value.toString() : value.toString().toLowerCase();
```

​       过滤标点符号（map函数）：在 parseSkipFile() 函数中将 punctuation.txt 中的标点符号读入 HashSet 中，在读文本的过程中将其替换为空格。

```Java
  for (String pattern : patternsToSkip)
  {
      line = line.replaceAll(pattern, " ");
  }
```

​       忽略数字（map函数）：使用正则表达式过滤数字

```Java
  Pattern pattern = Pattern.compile("^[-\\+]?[\\d]*$");
```

​       排序类的实现：快速排序（降序）

```java
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
```

​       Reducer：累加计数

```Java
  public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable>
  {
      private IntWritable result = new IntWritable();
      public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
      {
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

```

​       输出排名前100的<key,value>：在 Write 函数中控制输出行数

```Java
public static void Write(String name)
{
    try
    {
        Configuration conf=new Configuration();  
        FileSystem fs=FileSystem.get(conf);
        FSDataOutputStream os=fs.create(new Path(name));
        BufferedReader br=new BufferedReader(new FileReader("output//part-r-00000"));
        String str=null;
        int k=0;
        while((str=br.readLine())!=null)
        {
            k=k+1;
            str=str+"\n";
            byte[] buff=str.getBytes();
            os.write(buff,0,buff.length);       
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
}
```

​         两个job的串行操作：新建中间过程临时目录，并从中读入数据到第二个job

```Java
 public static void main(String[] args) throws Exception
 {
     Configuration conf = new Configuration();
     GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
     String[] remainingArgs = optionParser.getRemainingArgs();
     Path tempDir = new Path("wordcount-temp1-" + Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));       //定义一个临时目录
     Job job = Job.getInstance(conf, "word count");
     job.setJarByClass(WordCount.class);
     try
     {
         job.setMapperClass(TokenizerMapper.class);
         job.setCombinerClass(IntSumReducer.class);
         job.setReducerClass(IntSumReducer.class);
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
         FileInputFormat.addInputPath(job, new Path("shak-txt"));
         FileOutputFormat.setOutputPath(job, tempDir); //先将词频统计任务的输出结果写到临时目录中, 下一个排序任务以临时目录为输入目录
         job.setOutputFormatClass(SequenceFileOutputFormat.class);
         if(job.waitForCompletion(true))
         {
             Job sortJob = Job.getInstance(conf, "sort");
             sortJob.setJarByClass(WordCount.class);
             FileInputFormat.addInputPath(sortJob, tempDir);
             sortJob.setInputFormatClass(SequenceFileInputFormat.class);
             /*InverseMapper由hadoop库提供，作用是实现map()之后的数据对的key和value交换*/
             sortJob.setMapperClass(InverseMapper.class);
             /*将 Reducer 的个数限定为1, 最终输出的结果文件就是一个*/
             sortJob.setNumReduceTasks(1);
             FileOutputFormat.setOutputPath(sortJob,new Path("output"));
             sortJob.setOutputKeyClass(IntWritable.class);
             sortJob.setOutputValueClass(Text.class);
             sortJob.setSortComparatorClass(IntWritableDecreasingComparator.class);
             if(sortJob.waitForCompletion(true))
             {
                 Write("result");
             }
             else
             {
                 System.out.println("1-- not");
                 System.exit(1);
             }
             //删除临时文件
             FileSystem.get(conf).deleteOnExit(tempDir);
             System.exit(sortJob.waitForCompletion(true) ? 0 : 1);
            }
        }
    }
}
```

##### 2.3 各个作品的前100个单词

​       为解决在一个job里分别统计40个文件的词频并排序，我将单词所属的文件名称加入key，并自定义了 Partiton 类和 Partition2 类，分别解决第一个job和第二个job里<key,value>的reduce问题。因为第二个job使用了 InverseMapper.class 来交换 key 和 value，所以 Partition2 与 Partiton 的区别在于交换接收参数的顺序。

​       自定义 key：<key,value> -> <pathname key,value>

```Java
StringTokenizer itr = new StringTokenizer(line);
while (itr.hasMoreTokens())
{
    String hhh=itr.nextToken();
    word.set(hhh);
    //如果是数字则不保存
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
```

​       在 map 函数中获取所属的文件名称并将其加入到 HashSet 中：

```Java
public void map(Object key, Text value, Context context) throws IOException, InterruptedException
{
    //获取所属的文件名称
    FileSplit inputSplit = (FileSplit) context.getInputSplit();
    String pathname = inputSplit.getPath().getName();
    pathNameSet.add(pathname);
    ...
}
```

​       Partition：将 key 分词，判断其所属的文件，并根据 HashSet 中排序的不变性将文件名与 0~39 这40个partition对应。

```java
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
```

​        Partition2：接收参数顺序交换

```Java
public static class Partition extends Partitioner<IntWritable, Text>
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
```

​        在 Write 函数中，根据规律读取相应文件，交换 key 和 value 的位置并写入至result.txt文件中：

```Java
public static void Write(String name)
{
    try
    {
        Configuration conf=new Configuration();  
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
    }
    catch(Exception e)
    {
        e.printStackTrace();
    }
    }
}
```



#### 3 实验结果

- **所有作品的前100个高频单词（result.txt）**

```
1: thou,5589
2: thy,4004
3: shall,3536
4: thee,3204
5: lord,3134
6: king,3101
7: sir,2976
8: good,2837
9: come,2492
10: let,2317
11: love,2285
12: enter,2257
13: man,1977
14: hath,1931
15: like,1893
16: know,1764
17: say,1698
18: make,1676
19: did,1670
20: tis,1392
21: speak,1189
22: time,1181
23: tell,1086
24: heart,1083
25: henry,1076
26: duke,1075
27: think,1061
28: doth,1056
29: father,1051
30: lady,1019
31: exeunt,985
32: men,956
33: day,952
34: night,947
35: art,934
36: queen,922
37: look,921
38: exit,913
39: hear,897
40: great,886
41: life,876
42: death,872
43: hand,865
44: god,860
45: sweet,852
46: master,832
47: act,832
48: true,829
49: fair,822
50: away,816
51: mistress,812
52: eyes,772
53: scene,771
54: prince,762
55: pray,753
56: old,710
57: second,706
58: honour,703
59: world,694
60: fear,687
61: son,677
62: heaven,672
63: poor,662
64: blood,654
65: leave,643
66: till,642
67: brother,633
68: caesar,625
69: comes,620
70: falstaff,604
71: better,598
72: noble,597
73: gentleman,592
74: way,583
75: hast,583
76: myself,579
77: nay,575
78: stand,569
79: antony,562
80: grace,561
81: bear,559
82: house,556
83: dead,551
84: gloucester,535
85: richard,534
86: live,515
87: thing,514
88: wife,513
89: brutus,509
90: eye,506
91: word,506
92: mark,505
93: peace,505
94: head,504
95: little,500
96: john,498
97: hamlet,494
98: fool,493
99: madam,488
100: thine,485
```

- **各个作品的前100个高频单词（result.txt 部分展示）**

```
shakespeare-antony-23.txt
1: antony,428
2: cleopatra,315
3: caesar,292
4: mark,264
5: thou,184
6: enobarbus,153
7: domitius,140
8: octavius,133
9: shall,127
10: enter,112
11: thee,110
12: thy,110
13: charmian,105
14: let,99
15: good,95
16: come,93
17: pompey,81
18: sir,79
19: soldier,75
20: make,74
21: did,69
22: messenger,65
23: eros,64
24: lepidus,62
25: tis,59
26: say,59
27: like,56
28: exeunt,55
29: lord,54
30: menas,54
31: agrippa,52
32: madam,52
33: act,49
34: know,49
35: great,48
36: queen,48
37: egypt,46
38: man,45
39: world,45
40: scene,44
41: hath,44
42: octavia,43
43: guard,42
44: iras,42
45: love,40
46: speak,39
47: heart,38
48: hear,38
49: dolabella,38
50: exit,37
51: gods,35
52: alexas,35
53: death,34
54: rome,34
55: noble,33
56: time,33
57: friends,32
58: sea,31
59: think,31
60: way,31
61: tell,30
62: second,29
63: night,29
64: does,28
65: hand,28
66: look,28
67: mecaenas,28
68: fortune,27
69: hast,26
70: till,26
71: fight,26
72: aside,26
73: men,26
74: nay,26
75: dead,25
76: best,24
77: sword,24
78: follow,24
79: day,24
80: women,23
81: bear,23
82: land,23
83: war,22
84: honour,22
85: proculeius,22
86: eyes,22
87: thyreus,21
88: soothsayer,21
89: farewell,21
90: scarus,20
91: gone,20
92: bring,20
93: leave,20
94: myself,20
95: better,20
96: alexandria,19
97: canidius,19
98: pray,19
99: dear,19
100: art,19
---------------------------
shakespeare-comedy-7.txt
1: syracuse,232
2: dromio,221
3: antipholus,219
4: ephesus,169
5: thou,134
6: sir,111
7: adriana,90
8: thee,65
9: thy,59
10: man,57
11: luciana,56
12: did,55
13: come,48
14: chain,47
15: master,47
16: know,45
17: duke,43
18: home,42
19: angelo,41
20: enter,40
21: wife,37
22: time,36
23: let,36
24: hath,34
25: say,33
26: day,30
27: merchant,29
28: husband,29
29: good,28
30: mistress,28
31: tell,28
32: solinus,28
33: sister,27
34: aegeon,27
35: mad,27
36: officer,26
37: money,26
38: shall,26
39: till,25
40: house,25
41: make,25
42: dinner,24
43: think,24
44: art,22
45: like,22
46: bear,21
47: gold,20
48: second,19
49: love,19
50: life,18
51: sent,18
52: courtezan,17
53: nay,17
54: pray,17
55: tis,17
56: end,17
57: villain,17
58: long,17
59: doth,16
60: aemelia,16
61: came,16
62: exit,15
63: bound,15
64: god,15
65: speak,15
66: break,14
67: hear,14
68: exeunt,14
69: away,14
70: gave,14
71: fetch,14
72: saw,14
73: errors,14
74: comes,14
75: hast,13
76: hour,13
77: hand,13
78: sure,13
79: face,13
80: sweet,13
81: fair,13
82: scene,12
83: door,12
84: eye,12
85: town,12
86: pinch,12
87: rope,12
88: wrong,12
89: quoth,12
90: comedy,12
91: gone,12
92: didst,12
93: heart,12
94: look,12
95: mart,11
96: men,11
97: unto,11
98: dost,11
99: luce,11
100: return,11
---------------------------
shakespeare-tragedy-57.txt
1: king,282
2: thou,177
3: thy,175
4: richard,175
5: duke,166
6: bolingbroke,150
7: lord,143
8: henry,130
9: york,122
10: shall,102
11: thee,88
12: aumerle,71
13: hath,68
14: god,67
15: let,62
16: enter,62
17: northumberland,62
18: gaunt,59
19: come,56
20: make,53
21: good,53
22: say,51
23: queen,47
24: like,46
25: death,44
26: blood,43
27: men,43
28: cousin,41
29: heart,41
30: john,40
31: duchess,40
32: time,39
33: did,37
34: mowbray,36
35: love,35
36: hand,34
37: speak,34
38: son,33
39: life,33
40: man,33
41: noble,32
42: uncle,32
43: hereford,32
44: doth,32
45: fair,31
46: know,31
47: day,30
48: earth,30
49: heaven,30
50: land,29
51: true,29
52: bushy,28
53: tongue,28
54: pardon,28
55: sorrow,27
56: fear,27
57: way,27
58: grief,27
59: exeunt,27
60: thomas,27
61: tis,27
62: myself,26
63: soul,26
64: art,26
65: away,26
66: majesty,25
67: look,25
68: percy,25
69: face,25
70: green,25
71: lords,24
72: old,24
73: royal,23
74: live,23
75: head,23
76: banish,23
77: honour,23
78: traitor,23
79: grace,22
80: set,22
81: scene,22
82: arms,21
83: farewell,21
84: crown,21
85: great,21
86: world,21
87: norfolk,21
88: father,20
89: woe,20
90: sir,20
91: liege,20
92: little,20
93: act,20
94: stand,19
95: hands,19
96: till,19
97: sweet,19
98: word,19
99: bagot,19
100: marshal,19
---------------------------
```

- **提交作业运行成功的WEB页面截图（使用BDKIT）**

![](C:\Users\THINK\Desktop\屏幕截图 2021-10-28 183309.jpg)

![](C:\Users\THINK\Desktop\屏幕截图 2021-10-28 140501.jpg)



#### 4 不足与改进

- 这次作业的两个要求是分别实现的，放在两个项目中，没有很好地融合，随着后续的学习以及更深的理解，相信可以在一个job中同时统计分别和总体，从而提高项目的普适性；
- 可以使用自定义数据类型，在reduce的过程中自动排序，这样既不需要自定义 Partition，也不需要使用排序算法另外排序，从而提高程序的性能，简化代码；
- 为了提高扩展性，可以完善和优化代码细节，这一点在本次程序中已有体现，如不把文件路径写死，读取"-skip"后的文件名并将其存储在数组中等。
