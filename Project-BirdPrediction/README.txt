README:

Team Members: Deep Chheda, Saloni Shah

Please Unzip the folder.

---------------------------------------------------------------------------------------------------
[IntelliJ Idea 2016.3.3 IDE is used]

To run each program, create an input_labeled folder, and put the labeled.csv there and create another folder input_unlabeld and put unlabeled.csv there. After both the jobs are run, output file will be located in output folder

Included:
Project_Prediction: Includes the source code for the project. It also includes its own Makefile and pom.xml. 

---------------------------------------------------------------------------------------------------
(Please Note: We have different jobs for Training and Prediction program, and one MakeFile. So to make it easier we have mentioned the necessary changes in both MakeFile and below. You can simply copy paste the changes to mentioned locations)

Building and Executing Standalone:
Copy MakeFile and pom.xml
Make changes in MakeFile:
 - set spark.root
 - set job.name
 - set jar.name

To run the training program, please make sure following lines are uncommented in the file:
job.name=birdprediction.RandomForestTrainer
local.input_labeled=input_labeled
local.output=model

To run the prediction program, please make sure following lines are uncommented in the file:
job.name=birdprediction.BirdPrediction
local.input_unlabeled=input_unlabeled
local.model=model
local.output=output

Write following commands in terminal of IDE:
 - make alone

To run the training program, please make sure, the make alone command has the required input and output varaibles. (To run training program, the make alone command should be like:
alone: jar clean-local-output
	${spark.root}/bin/spark-submit --class ${job.name} --master local[*] ${jar.path} ${local.input_labeled} ${local.output})

To run the prediction program, please make sure, the make alone command has the required input and output varaibles. (To run prediction program, the make alone command should be like:
alone: jar clean-local-output
	${spark.root}/bin/spark-submit --class ${job.name} --master local[*] ${jar.path} ${local.model} ${local.input_unlabeled} ${local.output})

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

To run the training program on AWS, please make sure following lines are uncommented in the file:
aws.input_labeled=input_labeled
aws.output=model

To run the prediction program on AWS, please make sure following lines are uncommented in the file:
aws.input_unlabeled=input_unlabeled
aws.model=model
aws.output=output

Write following commands to terminal:
 - make upload-input-aws
 - make cloud 
 - make download-output-aws

To run training program on AWS, please make sure the make cloud command has the required input and ouput variables (the steps arguments should look like:
--steps '[{"Name":"Spark Program", "Args":["--class", "${job.name}", "--master", "yarn", "--deploy-mode", "cluster", "s3://${aws.bucket.name}/${jar.name}", "s3://${aws.bucket.name}/${aws.input_labeled}","s3://${aws.bucket.name}/${aws.output}"],"Type":"Spark","Jar":"s3://${aws.bucket.name}/${jar.name}","ActionOnFailure":"TERMINATE_CLUSTER"}]' \
)

To run prediction program on AWS, please make sure the make cloud command has the required input and output variables (the step arguments should look like:
--steps '[{"Name":"Spark Program", "Args":["--class", "${job.name}", "--master", "yarn", "--deploy-mode", "cluster", "s3://${aws.bucket.name}/${jar.name}", "s3://${aws.bucket.name}/${aws.model}", "s3://${aws.bucket.name}/${aws.input_unlabeled}","s3://${aws.bucket.name}/${aws.output}"],"Type":"Spark","Jar":"s3://${aws.bucket.name}/${jar.name}","ActionOnFailure":"TERMINATE_CLUSTER"}]' \
)

Above commands runs the application on AWS EMR via AWS command line (CLI)
