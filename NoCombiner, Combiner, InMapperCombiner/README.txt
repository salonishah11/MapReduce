README:

Please Unzip the folder.

---------------------------------------------------------------------------------------------------
[IntelliJ Idea 2016.3.3 IDE is used]

To run each program, create an input folder, and put the input file there.

Included:
NoCombiner: Includes the source code for Program 1 NoCombiner part.

Combiner: Includes the source code for Program 1 Combiner part.

InMapperCombiner: Includes the source code for Program 1 InMapperComb part.

Secondary Sort: Includes the source code for Program 2 (secondary sort).

[Each of the above folders include its own Makefile, pom.xml, output folder containing the output files (part-r-..) and syslog.txt (of one successful run on AWS)]

Report.pdf: The required report

---------------------------------------------------------------------------------------------------

Building and Executing Standalone:
Copy MakeFile and pom.xml
Make changes in MakeFile:
 - set hadoop.root
 - set job.name
 - set jar.name

Write following commands in terminal of IDE:
 - make switch-standalone
 - make alone
Above commands runs application in standalone mode

---------------------------------------------------------------------------------------------------

Building and Executing on AWS:
Copy MakeFile and pom.xml
Make changes in MakeFile:
 - set aws.region
 - set aws.bucket.name
 - set aws.subnet.id
 - aws.num.nodes
 - aws.instance.type

Write following commands to terminal:
 - make upload-input-aws
 - make cloud 
 - make download-output-aws
Above commands runs the application on AWS EMR via AWS command line (CLI)
