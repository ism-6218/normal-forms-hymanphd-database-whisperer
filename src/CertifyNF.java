import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
public class CertifyNF {

	public static void main(String[] args) throws SQLException {
		// TODO Auto-generated method stub
	
		      Connection c = null;
		      try {
		         Class.forName("com.vertica.jdbc.Driver");
		         c = DriverManager
		            .getConnection("jdbc:vertica://129.7.243.243:5433/cosc6340s17",
		            "team14", "7K8RnzSg");
		      } catch (Exception e) {
		         e.printStackTrace();
		         System.err.println(e.getClass().getName()+": "+e.getMessage());
		         System.exit(0);
		      }
		      System.out.println("Opened database successfully");
		      Statement st=c.createStatement();
		      String sql="";
		      ResultSet rs;		     
		      try
		      {
		    	  BufferedReader br = new BufferedReader(new FileReader(args[0])); 
		    	  BufferedWriter writer=new BufferedWriter(new FileWriter("NF.txt"));
		    	  BufferedWriter queries=new BufferedWriter(new FileWriter("NF.sql"));
		    	  BufferedWriter decompose=new BufferedWriter(new FileWriter("Decomposition.txt"));
		    	  writer.write("Table\t1NF\t2NF\t3NF\tBCNF\tReason");
		    	  
		    	    String line;
		    	    List<String> tab=new ArrayList<String>();
		    	    List<String> decomp_list=new ArrayList<String>();
		    	    while ((line = br.readLine()) != null) {
		    	    if(line.contains("("))
		    	    {	int count=0; //count no of dependencies
		    	    	int a=line.indexOf("(");
		    	    	String name=line.substring(0, a);
		    	    	//System.out.println(name);
		    	    	tab.add(name);
		    	    	int b=line.lastIndexOf(")");
		    	    	String t=line.substring(a+1,b).trim();
		    	    	//System.out.println(t);
		    	    	String[] s=t.split(",");
		    	    	List<String> keys=new ArrayList<String>();
		    	    	List<String> attributes=new ArrayList<String>();
		    	    	for(int i=0;i<s.length;i++)
		    	    	{
		    	    		if(s[i].trim().contains("(k)")){
		    	    			keys.add(s[i].subSequence(0, s[i].indexOf("(")).toString().trim());
		    	    			//System.out.print(s[i].trim()+",");
		    	    		}
		    	    		else
		    	    			attributes.add(s[i].trim());
		    	    	}
		    	    	//System.out.println("Key"+keys);
		    	    	//System.out.println("Attributes"+attributes);
						try {
		    	    		String check_keys="";
		    	    		String check_attributes="";
		    	    		for(int i=0;i<keys.size();i++)
		    	    			check_keys=check_keys+keys.get(i)+",";
		    	    		for(int i=0;i<attributes.size();i++)
		    	    			check_attributes=check_attributes+attributes.get(i)+",";
		    	    		if(check_keys!="")
		    	    			check_keys=check_keys.substring(0, check_keys.length());
		    	    		if(check_attributes!="")
		    	    			check_attributes=check_attributes.substring(0, check_attributes.length()-1);
		    	    		
		    	    		
		    	    		if(check_attributes=="")
		    	    			st.executeQuery(String.format("select %s from %s", check_keys.substring(0,check_keys.length()-1),name));
		    	    		else if(check_keys=="")
		    	    			st.executeQuery(String.format("select %s from %s", check_attributes,name));
		    	    		
		    	    		else 
		    	    			st.executeQuery(String.format("select %s%s from %s", check_keys,check_attributes,name));
		    	    		
						
		    	    	if(keys.size()>=1)
		    	    	{	
		    	    	verify1NF verify1=new verify1NF();
		    	    	if(verify1.NF(attributes, keys, st, name, writer,queries))
		    	    	{	int check=1;
		    	    	
		    	    		if(keys.size()>1)
		    	    		{ 
			    	    		//CHECK second normal form
			    	    		verify2NF verify2=new verify2NF();
			    	    		if(!verify2.NF(attributes, keys, decomp_list, st, name, count, writer,queries))
			    	    			check=0;
		    	    		}	    	    		
				    	    //CHECK Third Normal Form
		    	    		if(check==1)
				    		{;
		    	    		verify3NF verify3=new verify3NF();
				    		if(verify3.NF(attributes, keys, decomp_list, st, name, count, writer,queries))
		    	    		{  
					    		BCNF bcnf=new BCNF();
					    		bcnf.verify(attributes, keys, decomp_list, st, name, count, writer, queries);
				    			//System.out.println(name+"----3NF satisfied");
						    	//writer.newLine();
			    	    		//writer.write(name+"\t"+"Y\tY\tY\t");
		    	    		}
				    		}
			    	    		
		    	    		}
		    	    		
		    	    	}
						
		    	    	else
		    	    		{
		    	    		writer.newLine();
		    	    		writer.write(name+"\tN\tN\tN\tN\tNo Keys given");
		    	    		}
		    	    		
		    	    	
		    	      } 
						catch (Exception e)
			    	    {
			    	    	writer.newLine();
		    	    		writer.write(name+"\t\t\t\t\t"+e.toString()/*.substring(e.toString().indexOf("ERROR:"),e.toString().length())*/);
			    	    }
					  
		    	    }
		    	    
		    	    }
		    	    writer.newLine();
		    	    decompose.write("Decompositions");
		    	    
		    	    for(int i=0;i<decomp_list.size();i++)
		    	    {
		    	    	decompose.newLine();
		    	    	decompose.write(decomp_list.get(i));
		    	    }
		    	    
		    	    writer.close();queries.close();decompose.close();
		    	    
		    	    
		    	    
		      }
		      catch(Exception e){
		    	  System.out.println(e);
		      }
		      
		
	
	}
}
