//Required Packages installed
import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
class Elem implements Writable{
public short tag;
public int index;
public double value;
Elem () {}
Elem ( short tag, int index, double value) {
this.tag = tag;
this.index = index;
this.value = value;
}
public void write ( DataOutput obj1 ) throws IOException {
obj1.writeShort(tag);
obj1.writeInt(index);
obj1.writeDouble(value);
}
public void readFields ( DataInput obj2 ) throws IOException {
tag = obj2.readShort();
index= obj2.readInt();
value = obj2.readDouble();
}
}
class Pair implements WritableComparable<Pair> {
public int i;
public int j;
Pair () {}
Pair ( int i, int j ) {
this.i = i;
this.j = j;
}
public void write ( DataOutput obj1 ) throws IOException {
obj1.writeInt(i);
obj1.writeInt(j);
}
public void readFields ( DataInput obj2 ) throws IOException {
i= obj2.readInt();
j = obj2.readInt();
}
// Converting an object to string
public String toString() {
return i + " " + j + " ";
}
/*...*/
@Override
// Sorting the objects based on keys
public int compareTo(Pair obj) {
// TODO Auto-generated method stub
//return if(i==obj.i)? Integer.compare(j,obj.j): Integer.compare(i,obj.i);
if(i==obj.i){
    return Integer.compare(j,obj.j);
}
else{
    return Integer.compare(i,obj.i);
}
}
}
public class Multiply extends Configured {
/* ... */
// Mapper---M-Matrix
public static class Mapper_1 extends Mapper<Object,Text,IntWritable,Elem > {
@Override
public void map ( Object key, Text value, Context context )
throws IOException, InterruptedException {
String[] map1 = value.toString().split(",");
Short tag=0; // Considered tag=0 for Matrix M
int x = Integer.parseInt(map1[0]);
double y = Double.parseDouble(map1[2]);

context.write(new IntWritable(Integer.parseInt(map1[1])),new
Elem (tag, x, y));
}
}
// Mapper ---N-Matrix
public static class Mapper_2 extends Mapper<Object,Text,IntWritable,Elem > {
@Override
public void map ( Object key, Text value, Context context )
throws IOException, InterruptedException {
String[] map2 = value.toString().split(",");
Short tag=1; // Considered as tag=1 for Matrix 
int x = Integer.parseInt(map2[1]);
double y = Double.parseDouble(map2[2]);
context.write(new

IntWritable(Integer.parseInt(map2[0])),new Elem(tag, x, y));
}
}
public static class Reducer1 extends
Reducer<IntWritable,Elem,Pair,DoubleWritable> {
ArrayList<Elem> list1 = new ArrayList<Elem>(); // for Matrix M
ArrayList<Elem> list2 = new ArrayList<Elem>(); // for Matrix N

@Override
public void reduce ( IntWritable key, Iterable<Elem> values, Context
context )
throws IOException, InterruptedException {
list1.clear();
list2.clear();

for(Elem elem1:values) {
//In the case of tag=0 insert into list1
if(elem1.tag==0)
list1.add(new Elem((short)0,elem1.index,elem1.value));
//Insert into list2
else
list2.add(new Elem((short)1,elem1.index,elem1.value));
}
for (Elem elem1 : list1){
for(Elem elem2 : list2) {

context.write(new Pair(elem1.index, elem2.index),new

DoubleWritable(elem1.value*elem2.value));

}
}
}
}
public static class Mapper_Final extends
Mapper<Object,Text,Pair,DoubleWritable > {
@Override


public void map ( Object key, Text value, Context context )
throws IOException, InterruptedException {
String[] map2= value.toString().split(" ");
Pair pair = new
Pair(Integer.parseInt(map2[0]),Integer.parseInt(map2[1]));
context.write(pair,new
DoubleWritable(Double.parseDouble(map2[2])));
}
}
public static class Reducer_Final extends
Reducer<Pair,DoubleWritable,Pair,DoubleWritable> {
@Override
public void reduce ( Pair key, Iterable<DoubleWritable> values,
Context context )
throws IOException, InterruptedException {
// Add the values that are matched with same key
double res = 0.0;

for(DoubleWritable res1 : values) {
res = res+ res1.get();
}
context.write(key, new DoubleWritable(res));

}
}

public static void main ( String[] args ) throws Exception {
// To configure the job, control its execution and query the state, submit it.
Job j= Job.getInstance();
j.setJobName("Initial Job");
j.setJarByClass(Multiply.class);
MultipleInputs.addInputPath(j,new
Path(args[0]),TextInputFormat.class,Mapper_1.class);
MultipleInputs.addInputPath(j,new
Path(args[1]),TextInputFormat.class,Mapper_2.class);
j.setMapOutputKeyClass(IntWritable.class);

j.setMapOutputValueClass(Elem.class);
//Configure the reducer class

j.setReducerClass(Reducer1.class);

j.setOutputKeyClass(Pair.class);
j.setOutputValueClass(DoubleWritable.class);
j.setOutputFormatClass(TextOutputFormat.class);
FileOutputFormat.setOutputPath(j,new Path(args[2]));
j.waitForCompletion(true);
Job j1 = Job.getInstance();
j1.setJobName("Second Job");
j1.setJarByClass(Multiply.class);
//Configure Mapper class and Reducer class
j1.setMapperClass(Mapper_Final.class);
j1.setReducerClass(Reducer_Final.class);

j1.setInputFormatClass(TextInputFormat.class);
FileInputFormat.setInputPaths(j1, new Path(args[2]));
j1.setMapOutputKeyClass(Pair.class);
j1.setMapOutputValueClass(DoubleWritable.class);
j1.setOutputKeyClass(Pair.class);
j1.setOutputValueClass(DoubleWritable.class);
j1.setOutputFormatClass(TextOutputFormat.class);
FileOutputFormat.setOutputPath(j1,new Path(args[3]));
j1.waitForCompletion(true);
}
}
