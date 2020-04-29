package cass;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;


import com.datastax.driver.core.*;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.utils.Bytes;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

public class CassandraConnector {
	
	public void insert_pdf()
	{
		System.out.println("Cassandra Java Connection");
		Cluster cluster;
		Session session;
		cluster=Cluster.builder().addContactPoint("localhost").build();
		session=cluster.connect("ecommerce");
		FileInputStream fis = null;
		try {
			fis = new FileInputStream("F:\\python4.pdf");
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		byte[] b=null;
		try {
			b = new byte[fis.available()+1];
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			fis.read(b);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		ByteBuffer buffer=ByteBuffer.wrap(b);
		PreparedStatement ps=session.prepare("Insert into test (courseid, syllabus) values (?,?)");
		BoundStatement boundstatement=new BoundStatement(ps);
		session.execute(boundstatement.bind("Python",buffer));
		cluster.close();
	}
	
	public  byte[] getpdf(String cid) {
		System.out.println("Cassandra Java Connection");
		Cluster cluster;
		Session session;
		cluster=Cluster.builder().addContactPoint("localhost").build();
		session=cluster.connect("ecommerce");
		FileInputStream fis = null;
		try {
			fis = new FileInputStream("C:\\Users\\HEMAPRIYA\\Desktop\\CN.pdf");
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		byte[] b=null;
		try {
			b = new byte[fis.available()+1];
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		int length=b.length;
		try {
			fis.read(b);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		PreparedStatement ps1;
		BoundStatement boundStatement;
		ResultSet rs = null;
		
		if(cid.equals("SW"))
		{
			 ps1 = session.prepare("select syllabus from test where courseid=?");
			 boundStatement = new BoundStatement(ps1);
			 rs =session.execute ( boundStatement.bind("SW"));
		}
		
		if(cid.equals("DS"))
		{
			 ps1 = session.prepare("select syllabus from test where courseid=?");
			 boundStatement = new BoundStatement(ps1);
			 rs =session.execute ( boundStatement.bind("DS"));
		}
		if(cid.equals("OS"))
		{
			 ps1 = session.prepare("select syllabus from test where courseid=?");
			 boundStatement = new BoundStatement(ps1);
			 rs =session.execute ( boundStatement.bind("OS"));
		}
		if(cid.equals("CN"))
		{
			 ps1 = session.prepare("select syllabus from test where courseid=?");
			 boundStatement = new BoundStatement(ps1);
			 rs =session.execute ( boundStatement.bind("CN"));
		}
		if(cid.equals("DB"))
		{
			 ps1 = session.prepare("select syllabus from test where courseid=?");
			 boundStatement = new BoundStatement(ps1);
			 rs =session.execute ( boundStatement.bind("DB"));
		}
		
		
		ByteBuffer bpdf=null;
		for (Row row : rs) {
		 bpdf = row.getBytes("syllabus") ;
		}
		
		byte pdf[]= new byte[length];
		pdf=Bytes.getArray(bpdf);
		
		cluster.close();
		return pdf;
	}
	
	public void pdf_save(byte[] pdf)
	{
		
		OutputStream out = null;
		try {
			out = new FileOutputStream("out.pdf");
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			out.write(pdf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			out.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
		
		
}






