import java.io.BufferedWriter;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.sql.*;
public class BCNF {
public boolean verify(List<String> attributes,List<String> keys,List<String> decomp_list,Statement st, String name,int count,BufferedWriter writer,BufferedWriter queries) throws IOException, SQLException
{ 
	boolean flag=false;
	
ResultSet rs=null;
List<String> dependencies =new ArrayList<String>();
	for(int i=0;i < attributes.size();i++)
	{
		for(int j=0;j<keys.size();j++)
		{
			int x=0,y=0;
			String sql=String.format("select count(*) from (select %s,%s from %s GROUP BY %s,%s) as result; ",attributes.get(i),keys.get(j),name,attributes.get(i),keys.get(j));
			queries.newLine();
		    queries.write(String.format("select count(*) from (select %s,%s from %s GROUP BY %s,%s) as result; ",attributes.get(i),keys.get(j),name,attributes.get(i),keys.get(j)));
			rs=st.executeQuery(sql);
	        while(rs.next())
		      {
		    	x=rs.getInt(1);
		      }
		      sql=String.format("select count(*) from (select DISTINCT %s from %s) as result; ",attributes.get(i),name);
		      queries.newLine();
			  queries.write(String.format("select count(*) from (select DISTINCT %s from %s) as result; ",attributes.get(i),name));
		      rs=st.executeQuery(sql);
		      while(rs.next())
		      {
		    	y=rs.getInt(1);
		    	}
		      if(x==y)
		      {
		    	  count++;
			    	 // System.out.println(attributes.get(i)+"->"+attributes.get(j));
			    	  dependencies.add(attributes.get(i)+"->"+keys.get(j));
			          break;
		      }
		      if((i+1)<attributes.size())
		      {
		    	   x=0;y=0;
		    	   sql=String.format("select count(*) from (select %s,%s,%s from %s GROUP BY %s,%s,%s) as result; ",attributes.get(i),attributes.get(i+1),keys.get(j),name,attributes.get(i),attributes.get(i+1),keys.get(j));
					queries.newLine();
				    queries.write(String.format("select count(*) from (select %s,%s,%s from %s GROUP BY %s,%s,%s) as result; ",attributes.get(i),attributes.get(i+1),keys.get(j),name,attributes.get(i),attributes.get(i+1),keys.get(j)));
					rs=st.executeQuery(sql);
			        while(rs.next())
				      {
				    	x=rs.getInt(1);
				      }
				      sql=String.format("select count(*) from (select DISTINCT %s,%s from %s) as result; ",attributes.get(i),attributes.get(i+1),name);
				      queries.newLine();
					  queries.write(String.format("select count(*) from (select DISTINCT %s,%s from %s) as result; ",attributes.get(i),attributes.get(i+1),name));
				      rs=st.executeQuery(sql);
				      while(rs.next())
				      {
				    	y=rs.getInt(1);
				    	}
				      if(x==y)
				      {
				    	  count++;
					    	 // System.out.println(attributes.get(i)+"->"+attributes.get(j));
					    	  dependencies.add(attributes.get(i)+","+attributes.get(i+1)+"->"+keys.get(j));
					          break;
				      }
		      }
		      if((i+2)<attributes.size())
		      {
		    	  x=0;y=0;
		    	   sql=String.format("select count(*) from (select %s,%s,%s,%s from %s GROUP BY %s,%s,%s,%s) as result; ",attributes.get(i),attributes.get(i+1),attributes.get(i+2),keys.get(j),name,attributes.get(i),attributes.get(i+1),attributes.get(i+2),keys.get(j));
					queries.newLine();
				    queries.write(String.format("select count(*) from (select %s,%s,%s from %s GROUP BY %s,%s,%s) as result; ",attributes.get(i),attributes.get(i+1),attributes.get(i+2),keys.get(j),name,attributes.get(i),attributes.get(i+1),attributes.get(i+2),keys.get(j)));
					rs=st.executeQuery(sql);
			        while(rs.next())
				      {
				    	x=rs.getInt(1);
				      }
				      sql=String.format("select count(*) from (select DISTINCT %s,%s,%s from %s) as result; ",attributes.get(i),attributes.get(i+1),attributes.get(i+2),name);
				      queries.newLine();
					  queries.write(String.format("select count(*) from (select DISTINCT %s,%s,%s from %s) as result; ",attributes.get(i),attributes.get(i+1),attributes.get(i+2),name));
				      rs=st.executeQuery(sql);
				      while(rs.next())
				      {
				    	y=rs.getInt(1);
				    	}
				      if(x==y)
				      {
				    	  count++;
					    	 // System.out.println(attributes.get(i)+"->"+attributes.get(j));
					    	  dependencies.add(attributes.get(i)+","+attributes.get(i+1)+","+attributes.get(i+2)+"->"+keys.get(j));
					          break;
				      }
		      }
		}
		
	}
	if(count>0)
	{
		writer.newLine();
		writer.write(name+"\tY\tY\tY\tN\t"+dependencies);
		flag=false;
		return false;
	}
	else{
		writer.newLine();
		writer.write(name+"\tY\tY\tY\tY\t");
		flag=true;
		return true;
	}
	
}
}
