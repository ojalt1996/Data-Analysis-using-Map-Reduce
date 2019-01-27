import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public class WordCount {


  public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    public int i = 0;
    String[] gen;






    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    IntWritable year;
    Text g;
    FileSplit fileSplit = (FileSplit)context.getInputSplit();
    String filename = fileSplit.getPath().getName();



      // Splitting the string with the delimiter semicolon
      String[] itr = value.toString().split(";");
      try{
      //converting the year to int writtable
      year = new IntWritable(Integer.parseInt(itr[3]));
      if(Integer.parseInt(itr[3]) >= 2000)
      { 
          //iterating through the genre list
         gen =itr[4].split(","); 
          for (i = 0; i<gen.length; i++)
          { 
            //Here we are setting the output of the reducer
            g = new Text(gen[i]);
            context.write(g, year);
          }
      }

    }

    catch(Exception e){
     
    }
      
  }
  

  }




  public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();
    public int[] yearCount = new int[19];
    public int j = 0;
    public int count = 2000; 
    

    


    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
     count = 2000;
     Text k;
    
      //Initializing all the values of the yearCount[] to zero
     try{
            for (j = 0; j<yearCount.length; j++)
          { 
            yearCount[j]=0;
            
          }

   

   
      
      //Here we are iterating through all the values assigned to a key adn then 
      //based on the value calculation the count associated with each year    
      for (IntWritable val : values) {

        switch(val.get())
        {

            case 2000: 
                        yearCount[0] = yearCount[0] + 1;
                        break;

            case 2001: yearCount[1] = yearCount[1] + 1;
                        break;

            case 2002: yearCount[2]++;
                        break;

            case 2003: yearCount[3]++;
                        break;

            case 2004: yearCount[4]++;
                        break;

            case 2005: yearCount[5]++;
                        break;

            case 2006: yearCount[6]++;
                        break;

            case 2007: yearCount[7]++;
                        break;

            case 2008: yearCount[8]++;
                        break;

            case 2009: yearCount[9]++;
                        break;

            case 2010: yearCount[10]++;
                        break;

            case 2011: yearCount[11]++;
                        break;
            
             case 2012: yearCount[12]++;
                        break;

            case 2013: yearCount[13]++;
                        break;

            case 2014: yearCount[14]++;
                        break;

            case 2015: yearCount[15]++;
                        break;

            case 2016: yearCount[16]++;
                        break;

            case 2017: yearCount[17]++;
                        break;

            case 2018: yearCount[18]++;
                        break;

            default: 
                      break;

        }  

      }

      }

    catch(Exception e)
    {
     

    } 
       
               
      //Here we are outputting the results of the reducers to the file
      while(count <= 2018){
      
      
        switch(Integer.toString(count))
        { 

            case "2000": { 
                          result = new IntWritable(yearCount[0]) ;
                          k = new Text("2000 " + key.toString());
                          context.write(k, result);
                          break;
                        }


            case "2001": {
              result = new IntWritable(yearCount[1]) ;
                         k = new Text("2001 " + key.toString());
                          context.write(k, result);
                          break;
                        }


            case "2002": {result = new IntWritable(yearCount[2]) ;
                          k = new Text("2002 " + key.toString());
                          context.write(k, result);
                          break;
                        }


            case "2003": {result = new IntWritable(yearCount[3]) ;
                        k = new Text("2003 " + key.toString());
                          context.write(k, result);
                          break;
                        }

            case "2004": {result = new IntWritable(yearCount[4]) ;
                           k = new Text("2004 " + key.toString());
                          context.write(k, result);
                          break;
                        }

            case "2005": {result = new IntWritable(yearCount[5]) ;
                          k = new Text("2005 " + key.toString());
                          context.write(k, result);
                          break;
                        }


            case "2006": {result = new IntWritable(yearCount[6]) ;
                          k = new Text("2006 " + key.toString());
                          context.write(k, result);
                          break;
                        }


            case "2007": {result = new IntWritable(yearCount[7]) ;
                          k = new Text("2007 " + key.toString());
                          context.write(k, result);
                          break;
                        }


            case "2008": {result = new IntWritable(yearCount[8]) ;
                           k = new Text("2008 " + key.toString());
                          context.write(k, result);
                          break;
                        }


            case "2009": {result = new IntWritable(yearCount[9]) ;
                          k = new Text("2009 " + key.toString());
                          context.write(k, result);
                          break;
                        }


            case "2010": {result = new IntWritable(yearCount[10]) ;
                           k = new Text("2010 " + key.toString());
                          context.write(k, result);
                          break;
                        }


            case "2011": {result = new IntWritable(yearCount[11]) ;
                           k = new Text("2011 " + key.toString());
                          context.write(k, result);
                          break;
                        }

            
             case "2012": {result = new IntWritable(yearCount[12]) ;
                           k = new Text("2012 " + key.toString());
                          context.write(k, result);
                          break;
                        }


            case "2013": {result = new IntWritable(yearCount[13]) ;
                           k = new Text("2013 " + key.toString());
                          context.write(k, result);
                          break;
                        }


            case "2014": {result = new IntWritable(yearCount[14]) ;
                          k = new Text("2014 " + key.toString());
                          context.write(k, result);
                          break;
                        }


            case "2015": {result = new IntWritable(yearCount[15]) ;
                           k = new Text("2015 " + key.toString());
                          context.write(k, result);
                          break;
                        }

            case "2016": {result = new IntWritable(yearCount[16]) ;
                          k = new Text("2016 " + key.toString());
                          context.write(k, result);
                          break;
                        }


            case "2017": {result = new IntWritable(yearCount[17]) ;
                           k = new Text("2017 " + key.toString());
                          context.write(k, result);
                          break;
                        }


            case "2018": {result = new IntWritable(yearCount[18]) ;
                           k = new Text("2018 " + key.toString());
                          context.write(k, result);
                          break;
                        }


            default:  {
                      break;
                    }
          }
          count++;        
        }
    
  

  }

 }




  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
   
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

}

