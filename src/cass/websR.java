package cass;
import com.datastax.driver.core.Statement;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.ApplicationPath;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import javax.ws.rs.PathParam;
import cass.CassandraConnector;

@Path("/hello/{cid}")
public class websR {
	
	
	private static String FILE_PATH=null;
	
	@GET
	@Produces("application/pdf")
	public Response getFile(@PathParam("cid") String cid) {
		CassandraConnector cc =  new CassandraConnector();
		
		byte pdf[]=cc.getpdf(cid);
		OutputStream out = null;
		try {
			out = new FileOutputStream("G:\\Cassandra-Webservice\\out.pdf");
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
		
		System.out.println(cid);
		
		//if(cid.equals("1"))
		  // FILE_PATH = "C:\\Users\\HEMAPRIYA\\Desktop\\OS.pdf";
		//if(cid.equals("2"))
		FILE_PATH = "G:\\Cassandra-Webservice\\out.pdf";
		File file = new File(FILE_PATH);
		ResponseBuilder response = Response.ok((Object) file);
		response.header("Content-Disposition",
			"attachment; filename=\"syllabus.pdf\"");
		return response.build();


	}
	

}

