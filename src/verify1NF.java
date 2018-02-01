import java.io.BufferedWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class verify1NF {
	public boolean NF(List<String> attributes,List<String> keys,Statement stmt1, String name,BufferedWriter writer,BufferedWriter queries) throws SQLException, IOException
	{
	 boolean flag=false;
		int x=0,y=0;
		ResultSet rs = null;
		int null_count=0;
		for(int i=0;i<keys.size();i++)
		{
			String null_check=String.format("select count(*) from %s where %s IS NULL ", name, keys.get(i));
			queries.newLine();
			queries.write(String.format("select count(*) from %s where %s IS NULL ", name, keys.get(i)));
			rs=stmt1.executeQuery(null_check);
			while(rs.next())
			{
				null_count=rs.getInt(1);
			}
			if(null_count!=0)
			{
				//System.out.println(String.format("Null in %s",keys.get(i)));
				writer.newLine();
				writer.write(name+"\tN\t\t\t\t"+keys.get(i)+" has null");
				flag= false;
				return flag;
			}
		}
		//duplicates check
		if(keys.size()==1)
		{
			
			
			String queryP = String.format("SELECT COUNT(*) from (select DISTINCT %s FROM %s ) as result;",keys.get(0),name);
			queries.newLine();
			queries.write(String.format("SELECT COUNT(*) from (select DISTINCT %s FROM %s ) as result;",keys.get(0),name));
			rs=stmt1.executeQuery(queryP);
		      while(rs.next())
		      {
		    	x=rs.getInt(1);
		      }
		      String queryA = String.format("SELECT COUNT(*) FROM %s;",name);
		      queries.newLine();
		      queries.write(String.format("SELECT COUNT(*) FROM %s;",name));
		      rs=stmt1.executeQuery(queryA);
		      while(rs.next())
		      {
		    	y=rs.getInt(1);
		      }
			if(x==y)
			{
				//System.out.println(String.format("First Normal Form Satisfied"));
				flag= true;
				return flag;
			}
			else{
				//System.out.println("Normal from Failed");
				writer.newLine();
				writer.write(name+"\tN\t\t\t\t"+keys.get(0)+" has duplicate key values");
				flag= false;
				return flag;
			}

		}
		else 
			if(keys.size()==2)
			{
				String queryP = String.format("SELECT COUNT(*) from (select DISTINCT %s,%s FROM %s ) as result;",keys.get(0),keys.get(1),name);
				queries.newLine();
			    queries.write(String.format("SELECT COUNT(*) from (select DISTINCT %s,%s FROM %s ) as result;",keys.get(0),keys.get(1),name)); 
				rs=stmt1.executeQuery(queryP);
			      while(rs.next())
			      {
			    	x=rs.getInt(1);
			      }
			      String queryA = String.format("SELECT COUNT(*) FROM %s;",name);
			      queries.newLine();
				  queries.write(String.format("SELECT COUNT(*) FROM %s;",name));
			      rs=stmt1.executeQuery(queryA);
			      while(rs.next())
			      {
			    	y=rs.getInt(1);
			      }
				if(x==y)
				{
					//System.out.println(String.format("First Normal Form Satisfied"));
					flag= true;	
					return flag;
				}
				else{
					//System.out.println("Normal from Failed");
					writer.newLine();
					writer.write(name+"\tN\t\t\t\t"+keys.get(0)+","+keys.get(1)+" has duplicate key values");
					flag= false;
					return flag;
				}
			}
			else if(keys.size()==3)
			{
				String queryP = String.format("SELECT COUNT(*) from (select DISTINCT %s,%s,%s FROM %s ) as result;",keys.get(0),keys.get(1),keys.get(2),name);
				queries.newLine();
			    queries.write(String.format("SELECT COUNT(*) from (select DISTINCT %s,%s,%s FROM %s ) as result;",keys.get(0),keys.get(1),keys.get(2),name)); 
				rs=stmt1.executeQuery(queryP);
			      while(rs.next())
			      {
			    	x=rs.getInt(1);
			      }
			      String queryA = String.format("SELECT COUNT(*) FROM %s;",name);
			      queries.newLine();
				  queries.write(String.format("SELECT COUNT(*) FROM %s;",name));
			      rs=stmt1.executeQuery(queryA);
			      while(rs.next())
			      {
			    	y=rs.getInt(1);
			      }
				if(x==y)
				{
					//System.out.println(String.format("First Normal Form Satisfied"));
					flag= true;	
					return flag;
				}
				else{
					//System.out.println("Normal from Failed");
					writer.newLine();
					writer.write(name+"\tN\t\t\t\t"+keys.get(0)+","+keys.get(1)+ ","+keys.get(2)+" has duplicate key values");
					flag= false;
					return flag;
				}
			}
	
		return flag;
	}
	}
