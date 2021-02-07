SET-UP INSTRUCTIONS

* Copy the project into the directory where Vagrant exists
* You may have to make the files executable by running (you will have to replace <project-dir> with the project directory in all cases below)

```
chmod +x <project-dir>/src/common_neighbors.py <project-dir>/src/jaccard_coefficient.py
```
* Start the vagrant VM & then start the workers for hadoop dfs & spark

```
vagrant up && vagrant ssh
hadoop/sbin/start-dfs.sh
spark/sbin/start-all.sh
```

* Copy the data files to hadoop and prepare the output dir

```
hadoop/bin/hadoop fs -mkdir /input
hadoop/bin/hadoop fs -put /vagrant/<project-dir>/data/* /input
hadoop/bin/hadoop fs -mkdir /output
```

* Run the Common Neighbors executable

```
spark/bin/spark-submit /vagrant/<project-dir>/src/common_neighbors.py --input=hdfs://localhost:54310/input/CA-CondMat.txt --output hdfs://localhost:54310/output/common_neighborsMat --limit=10
spark/bin/spark-submit /vagrant/<project-dir>/src/common_neighbors.py --input=hdfs://localhost:54310/input/CA-AstroPh.txt --output hdfs://localhost:54310/output/common_neighborsAstro --limit=10
```

* Run the Jaccard Coefficiency executable

```
spark/bin/spark-submit /vagrant/<project-dir>/src/jaccard_coefficient.py --input=hdfs://localhost:54310/input/CA-CondMat.txt --output hdfs://localhost:54310/output/jaccard_coefficiencyMat --limit=10
spark/bin/spark-submit /vagrant/<project-dir>/src/jaccard_coefficient.py --input=hdfs://localhost:54310/input/CA-AstroPh.txt --output hdfs://localhost:54310/output/jaccard_coefficiencyAstro --limit=10
```

* View the results:
```
hadoop/bin/hadoop fs -cat /output/common_neighborsMat/part-0000x
hadoop/bin/hadoop fs -cat /output/common_neighborsPh/part-0000x
hadoop/bin/hadoop fs -cat /output/jaccard_coefficiencyMat/part-0000x
hadoop/bin/hadoop fs -cat /output/jaccard_coefficiencyPh/part-0000x
```


Extra:

If you want to run the project locally (on the driver), follow the following steps:

(Not recommended because of the files' size)

* Create a venv in the project directory, activate it & install the requirements
```
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

* Run the executables from the terminal by using the commands below:

```
./src/common_neighbors.py --input ./data/CA-CondMat.txt --output outputCMCM --limit 10
./src/common_neighbors.py --input ./data/CA-AstroPh.txt --output outputCMAP --limit 10
./src/jaccard_coefficient.py --input ./data/CA-CondMat.txt --output outputJCCM --limit 10
./src/jaccard_coefficient.py --input ./data/CA-AstroPh.txt --output outputJCAP --limit 10
```

You should have 4 new directories created and you can see the respective results in theses directories
