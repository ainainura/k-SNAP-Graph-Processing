import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
// Author: Ainur Ainabekova. Transforming KSnap algorithm into SQL queries. 
public class KSnap {

	// =========================================================================================
	// Parameters user can adjust! 
	static int k = 9; // number of groups in the summary that user wants 
	static String[] aggrF = {"type", "subtype"}; // vertex attributes that should be aggregated
	static String[] attrVertices = {"source"}; // the rest of vertex attributes 

	// for now this program only considers one type of edge attribute and only one value for it
	static String[] attrEdges = {"operation"};  // edge attribute
	static String[] attrEdgeValue = {"update"};  // value of the edge attribute that should be considered 
	// =========================================================================================
	
	// not using these two! 
	static ArrayList<String> A = new ArrayList<String>(); // set of vertex attributes
	static ArrayList<String> R = new ArrayList<String>(); // set of edge attributes

	static int numOfAggregateFields = 0;
	static ArrayList<String> aggrFieldNames = new ArrayList<String>();
	static ArrayList<String> namesOfAggrTables = new ArrayList<String>();

	public static void main( String args[] ) {

		// Displaying time spent
		long startTime = System.currentTimeMillis(); 

		// Connection to a database
		Connection c = null;

		try {
			// Establishing connection with the database
			Class.forName("org.postgresql.Driver");
			c = DriverManager
					.getConnection("jdbc:postgresql://localhost:5432/TestDB",
							"postgres", "postgre");
			c.setAutoCommit(false);
			System.out.println("1. Opened database successfully");

			// run k-snap algorithm on the data from this database
			System.out.println("2. Started running K-Snap on this dataset");

			// Vertex Attributes to be considered during k-snap
			// create aggregate field out of: subtype and type
			ArrayList<String> aggrFields = new ArrayList<String>();
			for (int m=0; m<aggrF.length; m++){
				aggrFields.add(aggrF[m]);
			}

			if (aggrFields.size()!=0){
				createAggregateField(c, aggrFields);
			}
			// edge attributes
			R.add("operation");

			// run k-snap algorithm
			kSnap(c);

		} catch ( Exception e ) {
			System.err.println("Failed to open database! - error: " +  e.getClass().getName()+": "+ e.getMessage() );
			System.exit(0);
		}
		System.out.println("Operation done successfully");
		long endTime   = System.currentTimeMillis();
		long totalTime = endTime - startTime;
		System.out.println("Total time spent  = " + TimeUnit.MILLISECONDS.toMinutes(totalTime) + " minutes");

	}

	public static void createAggregateField(Connection c, ArrayList<String> A){
		Statement stmt = null;
		numOfAggregateFields++;

		// always assume that size of arrayList A is 2;
		try {
			stmt = c.createStatement();
			String up = "create table R" + numOfAggregateFields + " as (select case when K1.id is null then K2.id else K1.id end as aggrId"+ numOfAggregateFields +", \'"+ A.get(0)+A.get(1)+"\' as aggrField" + numOfAggregateFields +", CONCAT(K1.value,' ',K2.value) as aggrValue" + numOfAggregateFields +" from "
					+ "(select * from vertex_anno where field=\'" + A.get(0) + "\')K1 full outer join (select * from vertex_anno where field=\'" + A.get(1) + "\')K2 on K1.id=K2.id);";
			System.out.println(up);
			stmt.executeUpdate(up);

			namesOfAggrTables.add("R" + numOfAggregateFields);

		} catch ( Exception e ) {
			System.err.println("Failed to create an aggregate field! - error: " + e.getClass().getName()+": "+ e.getMessage() );
			System.exit(0);
		}
	}

	public static String helper(ArrayList<String> tableNames, int counter2){
		System.out.println("Creating large inner join statement!");
		String largeInnerJoin = "CREATE TABLE A" + counter2+ " as ( select A1.id1, ";
		for (int i=1; i<=tableNames.size(); i++){
			if (i==tableNames.size()){
				largeInnerJoin += "field" + i + ", value" + i + " ";
			} else {
				largeInnerJoin += "field" + i + ", value" + i + ", ";
			}
		}
		largeInnerJoin += "from A1";
		for (int i=1; i<=tableNames.size()-1; i++){
			if (i==tableNames.size()-1){
				largeInnerJoin += " inner join A" + (i+1) + " on A1.id1=A"+(i+1)+".id"+(i+1) + ");";
			} else {
				largeInnerJoin += " inner join A" + (i+1) + " on A1.id1=A"+(i+1)+".id"+(i+1) + " ";
			}
		}
		System.out.println(largeInnerJoin);
		return largeInnerJoin;
	}

	public static String helper2(ArrayList<String> tableNames, int counter2){
		System.out.println("Creating large inner join statement in helper2!");
		String largeInnerJoin = "CREATE TABLE R" + counter2+ " as ( select R1.aggrId1, ";
		for (int i=1; i<=tableNames.size(); i++){
			if (i==tableNames.size()){
				largeInnerJoin += "aggrField" + i + ", aggrValue" + i + " ";
			} else {
				largeInnerJoin += "aggrField" + i + ", aggrValue" + i + ", ";
			}
		}
		largeInnerJoin += "from R1";
		for (int i=1; i<=tableNames.size()-1; i++){
			if (i==tableNames.size()-1){
				largeInnerJoin += " inner join R" + (i+1) + " on R1.aggrId1=R"+(i+1)+".aggrId"+(i+1) + ");";
			} else {
				largeInnerJoin += " inner join R" + (i+1) + " on R1.aggrId1=R"+(i+1)+".aggrId"+(i+1) + " ";
			}
		}
		System.out.println(largeInnerJoin);
		return largeInnerJoin;
	}

	public static void splitGroups(Connection c, int numOfGroups){
		System.out.println("\n3. Starting algorithm to split groups!!! ");
		Statement stmt = null;
		int counter = numOfGroups;
		boolean noMoreSplittingsPossible = false;

		try {
			// first create tables that contain data from edge for only this type of edge
			stmt = c.createStatement();
			for (int i=1; i<attrEdges.length + 1; i++){
				String qry = "CREATE TABLE E" + i + " as (select id as edgeId"+ i + ", field as edgeField" + i +  ", value as edgeValue" + i + " from edge_anno where field = \'"+ attrEdges[i-1] + "\' and value = \'" + attrEdgeValue[i-1] + "\');";
				System.out.println(qry);
				stmt.executeUpdate(qry);
				stmt.close();
			}

			stmt = c.createStatement();
			for (int i=1; i<attrEdges.length + 1; i++){
				String qry = "CREATE TABLE G" + i + " as (select edgeId"+ i + ", edgeField" + i +  ", edgeValue" + i + ", src, dst from E"+i+", edge where E" +i+ ".edgeId" + i+" = edge.id);";
				System.out.println(qry);
				stmt.executeUpdate(qry);
				stmt.close();
			}
			
			stmt = c.createStatement();
			String part_ratio = "CREATE TABLE ratios as (select K1.groupId1, K2.groupId2, 0.0 as partic1, 0.0 as partic2,"
					+ "0.0 as sum_partic, 0.0 as size_group1, 0.0 as size_group2,"
					+ "0.0 as total_size, 0.0 as ratio1, 0.0 as ratio2, 0.0 as ratio_total, 0.0 as delta1, 0.0 as delta2, 0.0 as max_delta, 0.0 as gr_with_max, 0.0 gr_base from "
					+ "(select groupId as groupId1 from groups)K1 cross join "
					+ "(select groupId as groupId2 from groups)K2);";
			System.out.println(part_ratio);
			stmt.executeUpdate(part_ratio);
			stmt.close();

			// assume that there is only one type of edge considered, therefore we have 
			// tables named E1 and G1 .
			stmt = c.createStatement();
			String update_ratio = "UPDATE ratios SET partic1 = (select count(*) from "
					+ "(select distinct src from G1, (select id1 from groups_init where groups_init.groupId = ratios.groupId1)K1, "
					+ "(select id1 as id2 from groups_init where groups_init.groupId = ratios.groupId2)K2 where "
					+ "G1.src = K1.id1 and G1.dst = K2.id2) as temp);";
			System.out.println(update_ratio);
			stmt.executeUpdate(update_ratio);
			stmt.close();

			stmt = c.createStatement();
			String update_ratio2 = "UPDATE ratios SET partic2 = (select count(*) from "
					+ "(select distinct src from G1, (select id1 from groups_init where groups_init.groupId = ratios.groupId2)K1, "
					+ "(select id1 as id2 from groups_init where groups_init.groupId = ratios.groupId1)K2 where "
					+ "G1.src = K1.id1 and G1.dst = K2.id2) as temp);";
			System.out.println(update_ratio2);
			stmt.executeUpdate(update_ratio2);
			stmt.close();

			stmt = c.createStatement();
			String update_ratio3 = "UPDATE ratios SET sum_partic = partic1 + partic2;";
			System.out.println(update_ratio3);
			stmt.executeUpdate(update_ratio3);
			stmt.close();

			stmt = c.createStatement();
			String update_ratio4 = "UPDATE ratios SET size_group1 = (select count(*) from groups_init where groups_init.groupId = groupId1);";
			System.out.println(update_ratio4);
			stmt.executeUpdate(update_ratio4);
			stmt.close();

			stmt = c.createStatement();
			String update_ratio8 = "UPDATE ratios SET size_group2 = (select count(*) from groups_init where groups_init.groupId = groupId2);";
			System.out.println(update_ratio8);
			stmt.executeUpdate(update_ratio8);
			stmt.close();

			stmt = c.createStatement();
			String update_ratio5 = "UPDATE ratios SET total_size = size_group1 + size_group2;";
			System.out.println(update_ratio5);
			stmt.executeUpdate(update_ratio5);
			stmt.close();

			stmt = c.createStatement();
			String update_ratio6 = "UPDATE ratios SET ratio1 = partic1/size_group1;";
			System.out.println(update_ratio6);
			stmt.executeUpdate(update_ratio6);
			stmt.close();

			stmt = c.createStatement();
			String update_ratio9 = "UPDATE ratios SET ratio2 = partic2/size_group2;";
			System.out.println(update_ratio9);
			stmt.executeUpdate(update_ratio9);
			stmt.close();
			
			stmt = c.createStatement();
			String update_ratio14 = "UPDATE ratios SET ratio_total = sum_partic/total_size;";
			System.out.println(update_ratio14);
			stmt.executeUpdate(update_ratio14);
			stmt.close();

			stmt = c.createStatement();
			String update_ratio7 = "UPDATE ratios SET delta1 = case when ratio1<=0.5 then partic1 else size_group1 - partic1 end;";
			System.out.println(update_ratio7);
			stmt.executeUpdate(update_ratio7);
			stmt.close();

			stmt = c.createStatement();
			String update_ratio10 = "UPDATE ratios SET delta2 = case when ratio2<=0.5 then partic2 else size_group2 - partic2 end;";
			System.out.println(update_ratio10);
			stmt.executeUpdate(update_ratio10);
			stmt.close();

			stmt = c.createStatement();
			String update_ratio11 = "UPDATE ratios SET max_delta = case when delta1<delta2 then delta2 else delta1 end;";
			System.out.println(update_ratio11);
			stmt.executeUpdate(update_ratio11);
			stmt.close();

			stmt = c.createStatement();
			String update_ratio12 = "UPDATE ratios SET gr_with_max = case when delta1<delta2 then groupId2 else groupId1 end;";
			System.out.println(update_ratio12);
			stmt.executeUpdate(update_ratio12);
			stmt.close();

			stmt = c.createStatement();
			String update_ratio13 = "UPDATE ratios SET gr_base = case when gr_with_max=groupId1 then groupId2 else groupId1 end;";
			System.out.println(update_ratio13);
			stmt.executeUpdate(update_ratio13);
			stmt.close();
			
			
		} catch ( Exception e ) {
			System.err.println("Failed to create tables E1, G1: - error:" + e.getClass().getName()+": "+ e.getMessage() );
			System.exit(0);
		}

		while (counter!= k && noMoreSplittingsPossible == false){
			try {

				stmt = c.createStatement();
				ResultSet rs = stmt.executeQuery( "SELECT * FROM ratios;" );
				System.out.println("\nid1 | id2 | partic1 | partic2 | sum_partic | size_group1 | size_group2 | total_size | ratio1 | ratio2 | delta1 | delta2 | max_delta | gr_with_max | gr_base ");
				while(rs.next()){
					int id1 = rs.getInt("groupId1");
					int id2 = rs.getInt("groupId2");
					int part = rs.getInt("partic1");
					int reverse = rs.getInt("partic2");
					int summ = rs.getInt("sum_partic");
					int size_group1 = rs.getInt("size_group1");
					int size_group2 = rs.getInt("size_group2");
					int total_size = rs.getInt("total_size");
					float ratio = rs.getFloat("ratio1");
					float ratio2 = rs.getFloat("ratio2");
					int delta = rs.getInt("delta1");
					int delta2 = rs.getInt("delta2");
					int max_delta = rs.getInt("max_delta");
					int gr_with_max = rs.getInt("gr_with_max");
					int gr_base = rs.getInt("gr_base");
					System.out.println(id1 + " | " + id2 + " | " + part + " | " + reverse + " | " + summ + " | " + size_group1 + " | " + size_group2 + " | " + total_size + " | " + ratio + " | " + ratio2 + " | " + delta + " | " + delta2 + " | " + max_delta + " | " + gr_with_max + " | " + gr_base);
				}
				rs.close();
				stmt.close();

				System.out.println();
				stmt = c.createStatement();
				String q =  "SELECT gr_with_max, gr_base, case when ratio1 <= 0.5 then ratio1 else 1-ratio1 end as ratio01, case when ratio2 <= 0.5 then ratio2 else 1-ratio2 end as ratio02, max_delta as maxx_val from ratios order by maxx_val;" ;
				System.out.println(q);
				ResultSet rs2 = stmt.executeQuery(q);

				System.out.println("\nMaximum deltas:");
				System.out.println("gr_with_max | gr_base | ratio1 | ratio2 | max_delta |");
				int grMax = -1;
				int grBase = -1;
				int max_val = -1;

				while(rs2.next()){
					int gr_with_max = rs2.getInt("gr_with_max");
					int gr_base = rs2.getInt("gr_base");
					float ratio1 = rs2.getFloat("ratio01");
					float ratio2 = rs2.getFloat("ratio02");
					int delta = rs2.getInt("maxx_val");
					grMax = gr_with_max;
					grBase = gr_base;
					max_val = delta;
					System.out.println(gr_with_max + " | " + gr_base + " | " + ratio1 + " | " + ratio2 + " | " + delta + " |");
				}

				rs2.close();
				stmt.close();

				if (max_val == 0){
					// then stop splitting algorithm! 
					noMoreSplittingsPossible = true;
					System.out.println("No more groups to split based on the given criteria! Stopping split algorithm! ");
				} else {

					System.out.println("\nGroup that should be split = " + grMax);
					System.out.println("\nGroup based on which split is happening = " + grBase);


					// Splitting a chosen group: based on whether vertices have relations with 
					// arg max or not. Update tables - groups, groups_init.

					// One more split happened
					counter++;
					System.out.println("Need to create a new group = " + (counter));
					// 1. updating table groups
					stmt = c.createStatement();
					String upd1 = "INSERT INTO groups VALUES ('', '', " + (counter)+ ");";
					System.out.println(upd1);
					stmt.executeUpdate(upd1);
					stmt.close();

					//2. updating groups_init table 
					stmt = c.createStatement();
					String upd2 = "CREATE TABLE idsToChange AS (select distinct src from G1, (select id1 from groups_init where groups_init.groupId = " + grMax +")K1, (select id1 as id2 from groups_init where groups_init.groupId ="+ grBase + ")K2 where G1.src = K1.id1 and G1.dst = K2.id2);";
					System.out.println(upd2);
					stmt.executeUpdate(upd2);
					stmt.close();

					stmt = c.createStatement();
					String upd3 = "UPDATE groups_init SET groupId = " + counter + " from idsToChange where idsToChange.src = id1;";
					System.out.println(upd3);
					stmt.executeUpdate(upd3);
					stmt.close();

					System.out.println("Splitting of the group is finished! ");
					System.out.println("==============================================\n");

					//if (counter!= k && noMoreSplittingsPossible == false){
						// drop unnecessary tables: 'ratios' - before the next while loop
						stmt = c.createStatement();
						String dropping = "DROP TABLE ratios, idsToChange;";
						System.out.println(dropping);
						stmt.executeUpdate(dropping);
						stmt.close();
					//}
						stmt = c.createStatement();
						String part_ratio = "CREATE TABLE ratios as (select K1.groupId1, K2.groupId2, 0.0 as partic1, 0.0 as partic2,"
								+ "0.0 as sum_partic, 0.0 as size_group1, 0.0 as size_group2,"
								+ "0.0 as total_size, 0.0 as ratio1, 0.0 as ratio2, 0.0 as ratio_total, 0.0 as delta1, 0.0 as delta2, 0.0 as max_delta, 0.0 as gr_with_max, 0.0 gr_base from "
								+ "(select groupId as groupId1 from groups)K1 cross join "
								+ "(select groupId as groupId2 from groups)K2);";
						System.out.println(part_ratio);
						stmt.executeUpdate(part_ratio);
						stmt.close();

						// assume that there is only one type of edge considered, therefore we have 
						// tables named E1 and G1 .
						stmt = c.createStatement();
						String update_ratio = "UPDATE ratios SET partic1 = (select count(*) from "
								+ "(select distinct src from G1, (select id1 from groups_init where groups_init.groupId = ratios.groupId1)K1, "
								+ "(select id1 as id2 from groups_init where groups_init.groupId = ratios.groupId2)K2 where "
								+ "G1.src = K1.id1 and G1.dst = K2.id2) as temp);";
						System.out.println(update_ratio);
						stmt.executeUpdate(update_ratio);
						stmt.close();

						stmt = c.createStatement();
						String update_ratio2 = "UPDATE ratios SET partic2 = (select count(*) from "
								+ "(select distinct src from G1, (select id1 from groups_init where groups_init.groupId = ratios.groupId2)K1, "
								+ "(select id1 as id2 from groups_init where groups_init.groupId = ratios.groupId1)K2 where "
								+ "G1.src = K1.id1 and G1.dst = K2.id2) as temp);";
						System.out.println(update_ratio2);
						stmt.executeUpdate(update_ratio2);
						stmt.close();

						stmt = c.createStatement();
						String update_ratio3 = "UPDATE ratios SET sum_partic = partic1 + partic2;";
						System.out.println(update_ratio3);
						stmt.executeUpdate(update_ratio3);
						stmt.close();

						stmt = c.createStatement();
						String update_ratio4 = "UPDATE ratios SET size_group1 = (select count(*) from groups_init where groups_init.groupId = groupId1);";
						System.out.println(update_ratio4);
						stmt.executeUpdate(update_ratio4);
						stmt.close();

						stmt = c.createStatement();
						String update_ratio8 = "UPDATE ratios SET size_group2 = (select count(*) from groups_init where groups_init.groupId = groupId2);";
						System.out.println(update_ratio8);
						stmt.executeUpdate(update_ratio8);
						stmt.close();

						stmt = c.createStatement();
						String update_ratio5 = "UPDATE ratios SET total_size = size_group1 + size_group2;";
						System.out.println(update_ratio5);
						stmt.executeUpdate(update_ratio5);
						stmt.close();

						stmt = c.createStatement();
						String update_ratio6 = "UPDATE ratios SET ratio1 = partic1/size_group1;";
						System.out.println(update_ratio6);
						stmt.executeUpdate(update_ratio6);
						stmt.close();

						stmt = c.createStatement();
						String update_ratio9 = "UPDATE ratios SET ratio2 = partic2/size_group2;";
						System.out.println(update_ratio9);
						stmt.executeUpdate(update_ratio9);
						stmt.close();
						
						stmt = c.createStatement();
						String update_ratio14 = "UPDATE ratios SET ratio_total = sum_partic/total_size;";
						System.out.println(update_ratio14);
						stmt.executeUpdate(update_ratio14);
						stmt.close();

						stmt = c.createStatement();
						String update_ratio7 = "UPDATE ratios SET delta1 = case when ratio1<=0.5 then partic1 else size_group1 - partic1 end;";
						System.out.println(update_ratio7);
						stmt.executeUpdate(update_ratio7);
						stmt.close();

						stmt = c.createStatement();
						String update_ratio10 = "UPDATE ratios SET delta2 = case when ratio2<=0.5 then partic2 else size_group2 - partic2 end;";
						System.out.println(update_ratio10);
						stmt.executeUpdate(update_ratio10);
						stmt.close();

						stmt = c.createStatement();
						String update_ratio11 = "UPDATE ratios SET max_delta = case when delta1<delta2 then delta2 else delta1 end;";
						System.out.println(update_ratio11);
						stmt.executeUpdate(update_ratio11);
						stmt.close();

						stmt = c.createStatement();
						String update_ratio12 = "UPDATE ratios SET gr_with_max = case when delta1<delta2 then groupId2 else groupId1 end;";
						System.out.println(update_ratio12);
						stmt.executeUpdate(update_ratio12);
						stmt.close();

						stmt = c.createStatement();
						String update_ratio13 = "UPDATE ratios SET gr_base = case when gr_with_max=groupId1 then groupId2 else groupId1 end;";
						System.out.println(update_ratio13);
						stmt.executeUpdate(update_ratio13);
						stmt.close();
						
					
				}

			} catch ( Exception e ) {
				System.err.println("Failed to run split groups algorithm! - error: " + e.getClass().getName()+": "+ e.getMessage() );
				System.exit(0);
			}
		}
		System.out.println("\nFinished Split Algorithm! ");
		System.out.println("Total number of groups = " + counter);
	}

	public static void kSnap(Connection c){
		Statement stmt = null;
		try {
			// gather data from a vertex_anno related to the usual fields
			// use data structure called attrVertices 
			ArrayList<String> tableNames = new ArrayList<String>();
			int counter = 1;
			for (int i=0; i<attrVertices.length; i++){
				stmt = c.createStatement();
				String up = "CREATE TABLE A"+ counter + " as (SELECT vertex_anno.id as id"+counter+", vertex_anno.field as field"+ counter +", vertex_anno.value as value"+ counter +" from vertex_anno where field = \'" + attrVertices[i] +"\');";
				System.out.println(up);
				stmt.executeUpdate(up);
				stmt.close();
				tableNames.add("A"+counter);
				counter++;
			}

			int counter2 = counter;

			// do inner join to create one larger table with all attr values
			if (tableNames.size()>1){
				String largeInnerJoin = helper(tableNames, counter2);
				stmt = c.createStatement();
				stmt.executeUpdate(largeInnerJoin);
				stmt.close();
				counter2++;
			}

			// we can create next new table with the name A+counter2
			// do inner join with tables for aggrFields
			int rcounter = namesOfAggrTables.size();
			if (namesOfAggrTables.size()>1){
				rcounter++;
				String joinAggr = helper2(namesOfAggrTables, rcounter);
				stmt = c.createStatement();
				stmt.executeUpdate(joinAggr);
				stmt.close();
			}

			// inner join of large aggrTable and large table with regular attr
			if (rcounter!=0){
				stmt = c.createStatement();
				String up = "CREATE TABLE A"+counter2 + " as (select * from A"+ (counter2-1)+ " inner join R" + (rcounter) + " on "
						+ "A"+ (counter2-1)+".id1 = R" + (rcounter)+".aggrId1);";
				System.out.println(up);
				stmt.executeUpdate(up);
				stmt.close();
			}

			// if rcounter!=0 then work further with table A+counter2
			// if rcounter==0 then work further with table (A+counter2-1)
			String mainTable = "";
			if (rcounter!=0){
				mainTable = "A"+counter2;
			} else {
				mainTable = "A"+(counter2-1);
			}

			String listOfVals= "";
			for (int i=1; i<=attrVertices.length; i++){
				if (i==attrVertices.length){
					listOfVals += "value" + i + " ";
				} else {
					listOfVals += "value" + i + ",";
				}
			}
			if (rcounter!=0){
				listOfVals+=",";
			}
			for (int i=1; i<namesOfAggrTables.size()+1; i++){
				if (i==namesOfAggrTables.size()){
					listOfVals += "aggrValue" + i + " ";
				} else {
					listOfVals += "aggrValue" + i + ", ";
				}
			}
			stmt = c.createStatement();
			String res = "CREATE TABLE M as (SELECT * FROM " + mainTable + " order by "+ listOfVals +" );";
			System.out.println(res);
			stmt.executeUpdate(res);
			stmt.close();

			// find number of groups in this initial grouping! assign groupIDs to each node

			System.out.println("\nAssigning group IDs to each node! ");

			stmt = c.createStatement();
			String updat = "CREATE TABLE groups as (select "+ listOfVals + ", row_number() over () as groupId from (select " + listOfVals + " from M group by " + listOfVals +")K);";
			System.out.println(updat);
			stmt.executeUpdate(updat);
			stmt.close();

			stmt = c.createStatement();
			String numOfG = "SELECT MAX(groupId) as max from groups;";
			System.out.println(numOfG);
			ResultSet rs = stmt.executeQuery(numOfG);
			int initialNumOfGroups = 0;
			while (rs.next()){
				System.out.print("Number of groups in the initial grouping is = " );
				String mx = rs.getString("max");
				System.out.println(mx);
				initialNumOfGroups = Integer.parseInt(mx);
			}
			rs.close();
			stmt.close();

			// assigning groupIds to each vertex
			stmt = c.createStatement();
			String assQ = "create table groups_init as (select * from M natural join groups);";
			System.out.println(assQ);
			stmt.executeUpdate(assQ);
			stmt.close();

			// Split groups algorithm now! 
			splitGroups(c, initialNumOfGroups);

			System.out.println("\n4. Writing results to a file! ");
			
			stmt = c.createStatement();
			String visTable = "CREATE TABLE visoutput as (SELECT groupId1, groupId2, ratio_total, size_group1, " + listOfVals + " from (select * from ratios inner join (select "+ listOfVals + ", groupId from groups_init group by " + listOfVals + ", groupId)KK on KK.groupId = ratios.groupId1)FF);";
			System.out.println(visTable);
			stmt.executeUpdate(visTable);
			stmt.close();
			
			// copy table for drawing visualization of a summary
			stmt = c.createStatement();
			String writingFile1 = "COPY visoutput TO '~/vis_" + attrEdgeValue[0] + ".csv' DELIMITER ',' CSV HEADER;";
			System.out.println(writingFile1);
			stmt.executeUpdate(writingFile1);
			stmt.close();
			
			// copy the groups_init table into csv file 
			stmt = c.createStatement();
			String writingFile = "COPY groups_init TO '~/output_" +attrEdgeValue[0] +".csv' DELIMITER ',' CSV HEADER;";
			System.out.println(writingFile);
			stmt.executeUpdate(writingFile);
			stmt.close();

			// close connection!   
			c.close();
		} catch ( Exception e ) {
			System.err.println("Failed to read data from database! - error: " + e.getClass().getName()+": "+ e.getMessage() );
			System.exit(0);
		}
	}
}
