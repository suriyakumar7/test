package test;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import java.util.*;


public class StreamingJob
{

	static Map<Tuple3<String,String,String>,NavigableMap<Integer,Tuple6<String,Integer,LinkedList,String,Integer,Integer>>> fa=new HashMap<>();
	static Map<Tuple2<String,String>, NavigableMap<Integer,Tuple5<LinkedList,Integer,LinkedList,String,Integer>>> faa=new HashMap<>();
	public static  final class Work implements FlatMapFunction<String,String>
	{

		public int number(String k)
		{
			int num=0;
			for(int i = 0; i < k.length(); i++)
			{
				num = num * 10 + ((int)k.charAt(i) - 48);
			}
			return  num;
		}

		protected static String time(int n)
		{
			int h=n/60;
			int m=n%60;
			String t;
			if(h>9 && m>9)
			{
				t=String.format("%d:%d",h,m);
			}
			else if(h<10 && m<10)
			{
				t=String.format("0%d:0%d",h,m);
			}
			else if(h<10)
			{
				t=String.format("0%d:%d",h,m);
			}
			else if(m<10)
			{
				t=String.format("%d:0%d",h,m);
			}
			else
			{
				t=null;
			}
			return t;
		}

		@Override
		public void flatMap(String value, Collector<String> out)
		{
				String[] li=value.split(",");
				String[] lii=li[0].split(" ");

				String k=lii[1].substring(0,2);
				Integer num=number(k);

				String k2=lii[1].substring(3,5);
				Integer num2=number(k2);

				String Date=lii[0];
				Integer time=(num*60)+num2;
				String Device_name=li[1];
				String Name=li[2];
				String status=li[3];

				Tuple3<String,String,String> key=new Tuple3<>(Date,Device_name,Name);
				Tuple2<String,String> key2=new Tuple2<>(Date,Device_name);

				if(status.equals("success"))
				{
					if(fa.containsKey(key)==false)
					{
						LinkedList<Integer> tim=new LinkedList<>();
						tim.add(time);
						Tuple6<String,Integer,LinkedList,String,Integer,Integer> val=new Tuple6<>(Device_name,time,tim,status,1,0);
						NavigableMap<Integer,Tuple6<String,Integer,LinkedList,String,Integer,Integer>> sub=new TreeMap<>();
						sub.put(time,val);
						fa.put(key,sub);
					}
					else
					{
							int na = fa.get(key).floorKey(time);
							if (time - fa.get(key).get(na).f1 <= 10 && time - fa.get(key).get(na).f1 >= 0)
							{
								int co = fa.get(key).get(na).f4 + 1;
								LinkedList<Integer> tim = new LinkedList<>(fa.get(key).get(na).f2);
								tim.add(time);
								Tuple6<String, Integer, LinkedList, String, Integer,Integer> val = new Tuple6<>(Device_name, fa.get(key).get(na).f1, tim, status, co,0);
								fa.get(key).replace(na, val);
								if(co>=3)
								{
									fa.get(key).get(na).f5=1;
								}

							}
							else if (time - fa.get(key).get(na).f1 > 10)
							{
								int co = fa.get(key).get(na).f4;
								LinkedList<Integer> tim = new LinkedList<>(fa.get(key).get(na).f2);
								tim.add(time);
								tim.pollFirst();
								Tuple6<String, Integer, LinkedList, String, Integer,Integer> val =
										new Tuple6<>(Device_name, tim.getFirst(), tim, status, co,0);
								fa.get(key).put(tim.getFirst(), val);
								if(co>=3)
								{
									fa.get(key).get(na).f5=1;
								}
							}
					}
					if(faa.get(key2)!=null)
					{
						int naa=faa.get(key2).floorKey(time);
						int timm;
						if(time-faa.get(key2).get(naa).f1<=10 && time-faa.get(key2).get(naa).f1>=0)
						{
							int co = faa.get(new Tuple2<>(Date, Device_name)).get(naa).f4 + 1;
							LinkedList<Integer> tim = new LinkedList<>(faa.get(key2).get(naa).f2);
							LinkedList<String> username = new LinkedList<>(faa.get(key2).get(naa).f0);

							if(tim.size()==0)
							{
								timm=time;
							}
							else
							{
								timm= (int) faa.get(key2).get(naa).f2.get(0);
							}
							tim.add(time);

							if (username.contains(Name) == false)
							{
								username.add(Name);
								Tuple5<LinkedList, Integer, LinkedList, String, Integer> val =
										new Tuple5<>(username, timm, tim, status, co);
								faa.get(key2).replace(naa,val);
								//out.collect(co+"");
								if (co >= 3)
								{
									out.collect("err");
								}
							}
						}
						else
						{
							faa.remove(key2);
						}
					}
				}
				else if(status.equals("failed"))
				{
					if(fa.get(key)!=null)
					{
						int na=fa.get(key).floorKey(time);
						if(fa.get(key).get(na).f5==1) {

							int v = (int) fa.get(key).get(na).f2.getLast();
							if (time - v <= 10 && time - v >= 0) {
								LinkedList<Integer> tim = new LinkedList<>();
								LinkedList<String> username = new LinkedList<>();
								username.add(Name);
								Tuple5<LinkedList, Integer, LinkedList, String, Integer> val =
										new Tuple5<>(username, time, tim, status, 0);
								NavigableMap<Integer, Tuple5<LinkedList, Integer, LinkedList, String, Integer>> sub = new TreeMap<>();
								sub.put(time, val);
								faa.put(key2, sub);
							}
						}
					}
				}
		}
	}


	public static void main(String[] args) throws Exception
	{

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream failed=env.socketTextStream("localhost", 9004, "\n")
						.flatMap(new Work());

		failed.print();

		env.execute("Brute Force");
	}

}
