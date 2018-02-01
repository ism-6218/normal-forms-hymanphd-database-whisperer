import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.io.BufferedWriter;
import java.io.IOException;
import java.sql.*;
public class verify2NF {
public boolean NF(List<String> attributes,List<String> keys,List<String> decomp_list,Statement st, String name,int count,BufferedWriter writer,BufferedWriter queries) throws SQLException, IOException
{   boolean flag=false;
	//int x,y;
	ResultSet rs = null;
	List<String> dependencies =new ArrayList<String>();
	if(keys.size()==2)
	{	
		for(int i=0;i < keys.size();i++)
		{
			for(int j=0;j<attributes.size();j++)
			{
				int x=0,y=0;
				String sql=String.format("select count(*) from (select %s,%s from %s GROUP BY %s,%s) as result; ",keys.get(i),attributes.get(j),name,keys.get(i),attributes.get(j));
				queries.newLine();
			    queries.write(String.format("select count(*) from (select %s,%s from %s GROUP BY %s,%s) as result; ",keys.get(i),attributes.get(j),name,keys.get(i),attributes.get(j)));
				rs=st.executeQuery(sql);
		        while(rs.next())
			      {
			    	x=rs.getInt(1);
			      }
			      sql=String.format("select count(*) from (select DISTINCT %s from %s) as result; ",keys.get(i),name);
			      queries.newLine();
				  queries.write(String.format("select count(*) from (select DISTINCT %s from %s) as result; ",keys.get(i),name));
			      rs=st.executeQuery(sql);
			      while(rs.next())
			      {
			    	y=rs.getInt(1);
			    	}
			      if(x==y)
			      {
			    	  count++;
				    	 // System.out.println(attributes.get(i)+"->"+attributes.get(j));
				    	  dependencies.add(keys.get(i)+"->"+attributes.get(j));
				         // break;
			      }
				}
			}
		
		List<String> attr_change =new ArrayList<String>();
		for(int i =0;i<attributes.size();i++)
			attr_change.add(attributes.get(i));
		if(count>0)
		{   HashMap join = new HashMap();
			int no=2;
			writer.newLine();
			writer.write(name+"\tY\tN\t\t\t"+dependencies);
			//System.out.println(dependencies);
		    	    		
			for(int i=0;i<dependencies.size();i++)
			{
			String[] decomp=dependencies.get(i).split("->");
			//System.out.println(decomp[0]+" "+decomp[1]);
			for(int j=0;j<attr_change.size();j++)
			{
				
				if(attr_change.contains(decomp[1]))
					attr_change.remove(decomp[1]);
				
			}
			st.executeUpdate(String.format("DROP TABLE IF EXISTS team14schema.%s%d",name,no));
			queries.newLine();
			queries.write(String.format("DROP TABLE IF EXISTS team14schema.%s%d",name,no));
			st.executeUpdate(String.format("create table team14schema.%s%d as (select %s,%s from %s GROUP BY %s,%s);", name,no,decomp[0],decomp[1],name,decomp[0],decomp[1]));
			queries.newLine();
			queries.write(String.format("create table team14schema.%s%d as (select %s,%s from %s GROUP BY %s,%s);", name,no,decomp[0],decomp[1],name,decomp[0],decomp[1]));
			decomp_list.add(name+no+"("+decomp[0]+","+decomp[1]+")");
			join.put(name+no, decomp[0]);
			//writer.newLine();
			//writer.write(name+no+"("+decomp[0]+","+decomp[1]+")");
			//System.out.println(name+no+" "+decomp[0]+","+decomp[1]);
			no++;
			} 
			
			/*Iterator<Map.Entry<String, String>> entri = join.entrySet().iterator();
			while (entri.hasNext()) {
			    Map.Entry<String, String> entry = entri.next();
			    System.out.println("Key = " + entry.getKey() + ", Value = " + entry.getValue());
			}*/
			
			//writer.newLine();
			//writer.write(name+"1"+"("+keys+" "+attr_change+")");
			//System.out.println(name+"1"+" "+keys+" "+attr_change);
			st.executeUpdate(String.format("DROP TABLE IF EXISTS team14schema.%s1",name));
			queries.newLine();
			queries.write(String.format("DROP TABLE IF EXISTS team14schema.%s1",name));
			String attr="";
			for(int i=0;i<attr_change.size();i++)
			{  
				attr=attr+attr_change.get(i)+",";
			}
			if(attr!="")
			attr=attr.substring(0, attr.lastIndexOf(","));
			String allkeys="";
			for(int i=0;i<keys.size();i++)
			{
			allkeys=allkeys+keys.get(i)+",";	
			}
			allkeys=allkeys.substring(0,allkeys.lastIndexOf(","));
			
			if(attr=="")
			{	
				st.executeUpdate(String.format("create table team14schema.%s1 as (select %s from %s);", name,allkeys,name));
				queries.newLine();
				queries.write(String.format("create table team14schema.%s1 as (select %s from %s);", name,allkeys,name));
			}
			else
			{
				st.executeUpdate(String.format("create table team14schema.%s1 as (select %s,%s from %s);", name,allkeys,attr,name));
				queries.newLine();
				queries.write(String.format("create table team14schema.%s1 as (select %s,%s from %s);", name,allkeys,attr,name));
			}
			decomp_list.add(name+"1"+"("+allkeys+","+attr+")");
			Iterator<Map.Entry<String, String>> entries = join.entrySet().iterator();
			int k=1,j1=0,j2=0;
			String join_query1="SELECT count(*) from "+name+";";
			String join_query2="SELECT count(*) from team14schema."+name+"1";
			String join_verify=name+"1";
			while (entries.hasNext()) 
			{
			    Map.Entry<String, String> entry = entries.next();
			   	if(k==1)
			    {  
			   		join_query2=join_query2+" JOIN team14schema."+entry.getKey()+" ON team14schema."+name+"1."+entry.getValue()+"=team14schema."+entry.getKey()+"."+entry.getValue();
			   		join_verify=join_verify+","+entry.getKey();
			   	//	System.out.println(join_query2);
			    }
			   	else
			   	{  
			   		join_query2=join_query2+" JOIN team14schema."+entry.getKey()+" ON team14schema."+name+"1"+"."+entry.getValue()+"=team14schema."+entry.getKey()+"."+entry.getValue();	
			   		join_verify=join_verify+","+entry.getKey();
			   	//	System.out.println(join_query2);
			   		
				   	   
			   	}
			  k++;
			}
			rs=st.executeQuery(join_query1);
			queries.newLine();
			queries.write(join_query1);
			while(rs.next())
				j1=rs.getInt(1);
			rs=st.executeQuery(join_query2);
			queries.newLine();
			queries.write(join_query2);
			while(rs.next())
				j2=rs.getInt(1);
			if(j1==j2)
				{
				//System.out.println("Join Successfull");
				decomp_list.add(name+"= Join("+join_verify+") =? Yes");
				}
			flag=false;
			return false;
		
			}
		else
		{
			flag=true;
			return true;
		}
		
		}
		
		if(keys.size()==3)
		{
		for(int i=0;i < keys.size();i++)
		{
			for(int k=i+1;k<keys.size();k++)
			{
				for(int j=0;j<attributes.size();j++)
				{
					int x=0,y=0;
					String sql=String.format("select count(*) from (select %s,%s,%s from %s GROUP BY %s,%s,%s) as result; ",keys.get(i),keys.get(k),attributes.get(j),name,keys.get(i),keys.get(k),attributes.get(j));
					queries.newLine();
				    queries.write(String.format("select count(*) from (select %s,%s,%s from %s GROUP BY %s,%s,%s) as result; ",keys.get(i),keys.get(k),attributes.get(j),name,keys.get(i),keys.get(k),attributes.get(j)));
					rs=st.executeQuery(sql);
			        while(rs.next())
				      {
				    	x=rs.getInt(1);
				      }
				      sql=String.format("select count(*) from (select DISTINCT %s,%s from %s) as result; ",keys.get(i),keys.get(k),name);
				      queries.newLine();
					  queries.write(String.format("select count(*) from (select DISTINCT %s,%s from %s) as result; ",keys.get(i),keys.get(k),name));
				      rs=st.executeQuery(sql);
				      while(rs.next())
				      {
				    	y=rs.getInt(1);
				    	}
				      if(x==y)
				      {
				    	  count++;
					    	 // System.out.println(attributes.get(i)+"->"+attributes.get(j));
					    	  dependencies.add(keys.get(i)+","+keys.get(k)+"->"+attributes.get(j));
					         // break;
				      }
				
				}
			}
		}
		
		for(int i=0;i < keys.size();i++)
		{
			for(int j=0;j<attributes.size();j++)
			{
				int x=0,y=0;
				String sql=String.format("select count(*) from (select %s,%s from %s GROUP BY %s,%s) as result; ",keys.get(i),attributes.get(j),name,keys.get(i),attributes.get(j));
				queries.newLine();
			    queries.write(String.format("select count(*) from (select %s,%s from %s GROUP BY %s,%s) as result; ",keys.get(i),attributes.get(j),name,keys.get(i),attributes.get(j)));
				rs=st.executeQuery(sql);
		        while(rs.next())
			      {
			    	x=rs.getInt(1);
			      }
			      sql=String.format("select count(*) from (select DISTINCT %s from %s) as result; ",keys.get(i),name);
			      queries.newLine();
				  queries.write(String.format("select count(*) from (select DISTINCT %s from %s) as result; ",keys.get(i),name));
			      rs=st.executeQuery(sql);
			      while(rs.next())
			      {
			    	y=rs.getInt(1);
			    	}
			      if(x==y)
			      {
			    	  count++;
				    	 // System.out.println(attributes.get(i)+"->"+attributes.get(j));
				    	  dependencies.add(keys.get(i)+"->"+attributes.get(j));
				        //  break;
			      }
				}
			}
		
		List<String> attr_change =new ArrayList<String>();
		for(int i =0;i<attributes.size();i++)
			attr_change.add(attributes.get(i));
		
		if(count>0)
		{   HashMap join = new HashMap();
			int no=2;
					
			writer.newLine();
			writer.write(name+"\tY\tN\t\t\t"+dependencies);
			//System.out.println(dependencies);
		    	    		
			for(int i=0;i<dependencies.size();i++)
			{
			String[] decomp=dependencies.get(i).split("->");
				String[] key_decomp={};int key_count=0;
				if(decomp[0].contains(","))
					{key_decomp=decomp[0].split(",");
					key_count=2;}
				
				//System.out.println(decomp[0]+" "+decomp[1]);
				for(int j=0;j<attr_change.size();j++)
				{
					
					if(attr_change.contains(decomp[1]))
						attr_change.remove(decomp[1]);
					
				}
				st.executeUpdate(String.format("DROP TABLE IF EXISTS team14schema.%s%d",name,no));
				queries.newLine();
				queries.write(String.format("DROP TABLE IF EXISTS team14schema.%s%d",name,no));
				
				if(key_count==2)
				{
				st.executeUpdate(String.format("create table team14schema.%s%d as (select %s,%s,%s from %s GROUP BY %s,%s,%s);", name,no,key_decomp[0],key_decomp[1],decomp[1],name,key_decomp[0],key_decomp[1],decomp[1]));
				queries.newLine();
				queries.write(String.format("create table team14schema.%s%d as (select %s,%s from %s GROUP BY %s,%s);", name,no,key_decomp[0],key_decomp[1],decomp[1],name,key_decomp[0],key_decomp[1],decomp[1]));
				decomp_list.add(name+no+"("+key_decomp[0]+","+key_decomp[1]+","+decomp[1]+")");
				join.put(name+no, key_decomp[0]+","+key_decomp[1]);
				//writer.newLine();
				//writer.write(name+no+"("+decomp[0]+","+decomp[1]+")");
				//System.out.println(name+no+" "+decomp[0]+","+decomp[1]);
				no++;
				}
				if(key_count==0)
				{
					st.executeUpdate(String.format("create table team14schema.%s%d as (select %s,%s from %s GROUP BY %s,%s);", name,no,decomp[0],decomp[1],name,decomp[0],decomp[1]));
					queries.newLine();
					queries.write(String.format("create table team14schema.%s%d as (select %s,%s from %s GROUP BY %s,%s);", name,no,decomp[0],decomp[1],name,decomp[0],decomp[1]));
					decomp_list.add(name+no+"("+decomp[0]+","+decomp[1]+")");
					join.put(name+no, decomp[0]);
					no++;
				}
			
			} 
			
			/*Iterator<Map.Entry<String, String>> entri = join.entrySet().iterator();
			while (entri.hasNext()) {
			    Map.Entry<String, String> entry = entri.next();
			    System.out.println("Key = " + entry.getKey() + ", Value = " + entry.getValue());
			}*/
			
			//writer.newLine();
			//writer.write(name+"1"+"("+keys+" "+attr_change+")");
			//System.out.println(name+"1"+" "+keys+" "+attr_change);
			st.executeUpdate(String.format("DROP TABLE IF EXISTS team14schema.%s1",name));
			queries.newLine();
			queries.write(String.format("DROP TABLE IF EXISTS team14schema.%s1",name));
			String attr="";
			for(int i=0;i<attr_change.size();i++)
			{  
				attr=attr+attr_change.get(i)+",";
			}
			if(attr!="")
			attr=attr.substring(0, attr.lastIndexOf(","));
			String allkeys="";
			for(int i=0;i<keys.size();i++)
			{
			allkeys=allkeys+keys.get(i)+",";	
			}
			allkeys=allkeys.substring(0,allkeys.lastIndexOf(","));
			
			if(attr=="")
			{	
				st.executeUpdate(String.format("create table team14schema.%s1 as (select %s from %s);", name,allkeys,name));
				queries.newLine();
				queries.write(String.format("create table team14schema.%s1 as (select %s from %s);", name,allkeys,name));
			}
			else
			{
				st.executeUpdate(String.format("create table team14schema.%s1 as (select %s,%s from %s);", name,allkeys,attr,name));
				queries.newLine();
				queries.write(String.format("create table team14schema.%s1 as (select %s,%s from %s);", name,allkeys,attr,name));
			}
			decomp_list.add(name+"1"+"("+allkeys+","+attr+")");
			Iterator<Map.Entry<String, String>> entries = join.entrySet().iterator();
			int k=1,j1=0,j2=0;
			String join_query1="SELECT count(*) from "+name+";";
			String join_query2="SELECT count(*) from team14schema."+name+"1";
			String join_verify=name+"1";
			while (entries.hasNext()) 
			{
			    Map.Entry<String, String> entry = entries.next();
			    if(entry.getValue().contains(","))
			    { 
			    	String[] key_string=entry.getValue().split(",");
				   	if(k==1)
				    {   
				   		join_query2=join_query2+" JOIN team14schema."+entry.getKey()+" ON team14schema."+name+"1."+key_string[0]+"=team14schema."+entry.getKey()+"."+key_string[0]+" and team14schema."+name+"1."+key_string[1]+"=team14schema."+entry.getKey()+"."+key_string[1];
				   		join_verify=join_verify+","+entry.getKey();
				   	//	System.out.println(join_query2);
				    }
				   	else
				   	{  
				   		join_query2=join_query2+" JOIN team14schema."+entry.getKey()+" ON team14schema."+name+k+"."+key_string[0]+"=team14schema."+entry.getKey()+"."+key_string[0]+" and team14schema."+name+k+"."+key_string[1]+"=team14schema."+entry.getKey()+"."+key_string[1];	
				   		join_verify=join_verify+","+entry.getKey();
				   	//	System.out.println(join_query2);
				   		
					   	   
				   	}
				   	}
			    else{
			    	if(k==1)
				    {  
				   		join_query2=join_query2+" JOIN team14schema."+entry.getKey()+" ON team14schema."+name+"1."+entry.getValue()+"=team14schema."+entry.getKey()+"."+entry.getValue();
				   		join_verify=join_verify+","+entry.getKey();
				   	//	System.out.println(join_query2);
				    }
				   	else
				   	{  
				   		join_query2=join_query2+" JOIN team14schema."+entry.getKey()+" ON team14schema."+name+k+"."+entry.getValue()+"=team14schema."+entry.getKey()+"."+entry.getValue();	
				   		join_verify=join_verify+","+entry.getKey();
				   	//	System.out.println(join_query2);
				   		
					   	   
				   	}
			    }
			  k++;
			}
			rs=st.executeQuery(join_query1);
			queries.newLine();
			queries.write(join_query1);
			while(rs.next())
				j1=rs.getInt(1);
			rs=st.executeQuery(join_query2);
			queries.newLine();
			queries.write(join_query2);
			while(rs.next())
				j2=rs.getInt(1);
			if(j1==j2)
				{
				//System.out.println("Join Successfull");
				decomp_list.add(name+"= Join("+join_verify+") =? Yes");
				}
			flag=false;
			return false;
		}
		else
		{
			flag=true;
			return true;
		}
		
	}

	return flag;
}
}
