# k-SNAP-Graph-Summarization Algorithm 

Link to the paper describing this algorithm that summarizes large graphs by grouping the nodes into meaningful clusters: 
http://pages.cs.wisc.edu/~jignesh/publ/summarization.pdf 

Instructions on how to run the program: 

1. You have to install PostgreSQL database, create new DB called “TestDB” and 
create three tables inside named “vertex_anno”, “edge_anno”, “edge” by running these queries:

create database TestDB;
\connect TestDB;
create table vertex_anno (id BIGINT, field TEXT, value TEXT);
create table edge_anno (id BIGINT, field TEXT, value TEXT);
create table edge (id BIGINT, src BIGINT, dst BIGINT);


2. Load data into these tables using COPY command from three files named 
“vertex_anno.csv”, “edge_anno.csv”, “edge.csv” correspondingly. These files can be 
found in the folder ‘dataset’ (These files were too big to upload to github). You have to 
change the path of the data files when running following queries:

COPY vertex_anno FROM '~/dataset/vertex_anno.csv' DELIMITER ',' CSV;

COPY edge_anno FROM ‘~/dataset/edge_anno.csv’ DELIMITER ‘,’ CSV;

COPY edge FROM ‘~/dataset/edge.csv’ DELIMITER ‘,’ CSV;


3. Install JDBC for postgres. 


4. Compile and run java file called “KSnap.java”. Several lines that might need to be changed : 

- connection to your DB (line 41-43). You might need to change username and password for connecting 
to your DB. 

- program runs without passing any parameters, but if you want to change some parameters related 
to the program you might need to change data structures at the top of the file “KSnap.java”. 
For example, number k - number of groups in the summary, vertex attributes that need to be 
aggregated, other vertex attributes that need to be considered as well without aggregation 
and only one edge attribute type. They are explained in the comments at the top of the file. 

For example in the current program, it tries to create 9 groups in the summary, so k=9. 
Then, it aggregates vertex attributes such as “type” and “subtype” into one value. Another 
vertex attribute considered is “source”, without aggregation. My program works only for 
one edge attribute type (“operation” is the field, and value is “update”). 
If you don’t want to have any aggregate vertex attributes when creating summary, 
you can leave ‘aggrF’ list empty, and add necessary names of the vertex attributes 
to the list ‘attrVertices’ as many as you want. 

- Lastly, regarding output of the program, there is output to the console that explains 
how the program works and what it is doing, but there is also output into two files that 
are useful for drawing the visualization of the graph summary.  My program writes 
two tables into two files called “vis_update.csv” and “output_update.csv” (lines 614 and 621, 
word ‘update’ appears in the name of the file because that’s the name of the edge attribute type 
based on which summary was made). 
You might need to change those lines 614 and 621 by adding the path on your computer 
where you want to have those output files. I have attached also my two output files 
for the given parameters (inside folder ‘output’). 

5. There is also a word file called “Understanding data graphs“ that contains general useful 
information about dataset that might be used if you want to run the program with other attributes. 

6. A little explanation about the structure of the output files. File vis_update.cvs contains 
information about participation ratios between two groups, size of the groups, and 
values of the vertex attributes based on which summary was created, so it is going to be not very 
large file. File output_update.csv is going to be larger because it shows the assignment of different nodes to different groups. 

