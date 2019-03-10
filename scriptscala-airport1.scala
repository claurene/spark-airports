// export SPARK_HOME=~/spark-2.4.0-bin-hadoop2.7/bin
// $SPARK_HOME/spark-shell -i scriptscala-airport1.scala
// ou utiliser :paste dans le shell
// zeppelin-0.8.1-bin-all$ bin/zeppelin-daemon.sh start

// Charger les 3 fichiers (2006, 2007 et 2008)
val df = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("/home/laurene/Documents/M2 MIAGE/CM Big Data/data/2006_shuf.csv") //full/*.csv
// Afficher les colonnes de la dataframe
df.printSchema

// Charger les fichiers complémentaires
val df_airports = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("/home/laurene/Documents/M2 MIAGE/CM Big Data/data/airports.csv")
df_airports.printSchema

val df_carriers = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("/home/laurene/Documents/M2 MIAGE/CM Big Data/data/carriers.csv")
df_carriers.printSchema

// Q1: meilleurs moois, jour et jourSemaine pour minimiser les retards
val df1 = df
    .withColumn("ArrDelay", col("ArrDelay").cast("double"))
    .filter($"Cancelled" =!= 1 && $"Diverted" =!= 1)
    .na.fill(0,Seq("ArrDelay"))

// Meilleur jour
val req1 = df1.groupBy("DayOfMonth").agg(avg("ArrDelay").as("AvgDelay"))
req1.sort(asc("AvgDelay")).select("DayOfMonth").limit(1).show

// Meilleur jour de la semaine
val req2 = df1.groupBy("DayOfWeek").agg(avg("ArrDelay").as("AvgDelay"))
req2.sort(asc("AvgDelay")).select("DayOfWeek").limit(1).show

// Meilleur mois
val req3 = df1.groupBy("Month").agg(avg("ArrDelay").as("AvgDelay"))
req3.sort(asc("AvgDelay")).select("Month").limit(1).show

//req1.registerTempTable("req1") // pour visualiser les données sur Zeppelin

// Exemple de fonctions pour formatter les données pour afficher les résultats
def jourSemaineUDF = udf { (day:Int) =>
    day match {
        case 1 => "Lundi"
        case 2 => "Mardi"
        case 3 => "Mercredi"
        case 4 => "Jeudi"
        case 5 => "Vendredi"
        case 6 => "Samedi"
        case 7 => "Dimanche"
    }
}
req2.sort(asc("AvgDelay")).withColumn("FullDayOfWeek",jourSemaineUDF(col("DayOfWeek"))).show

def moisUDF = udf { (day:Int) =>
    day match {
        case 1 => "Janvier"
        case 2 => "Février"
        case 3 => "Mars"
        case 4 => "Avril"
        case 5 => "Mai"
        case 6 => "Juin"
        case 7 => "Juillet"
        case 8 => "Août"
        case 9 => "Septembre"
        case 10 => "Octobre"
        case 11 => "Novembre"
        case 12 => "Décembre"
    }
}
req3.sort(asc("AvgDelay")).withColumn("FullMonth",moisUDF(col("Month"))).show

// Q2: cause principale de retard
val col2 = Array("CarrierDelay","WeatherDelay","NASDelay","SecurityDelay","LateAircraftDelay")

val df2 = col2.map { name:String => (name, df.filter(name+">0").count)}.toSeq.toDF("Name","CountDelay")
df2.sort(desc("CountDelay")).show

// Q3: 5 groupes de compagnies en fonction des retards
val df3 = df
    .select(col("UniqueCarrier"),col("ArrDelay").cast("double"),col("DepDelay").cast("double"))
    .filter($"Cancelled" =!= 1 && $"Diverted" =!= 1)
    .na.fill(0,Seq("ArrDelay"))
    .na.fill(0,Seq("DepDelay"))

val df3g = df3.groupBy("UniqueCarrier").agg(avg("ArrDelay").as("ArrDelay"),avg("DepDelay").as("DepDelay"))

// K-means
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.clustering.{KMeans,KMeansModel}
import org.apache.spark.ml.Pipeline

val assembler = new VectorAssembler().setInputCols(Array("ArrDelay","DepDelay")).setOutputCol("features")
val kmeans = new KMeans().setK(5).setFeaturesCol("features").setPredictionCol("prediction")
/* val df3a = assembler.transform(df3g)
val model = kmeans.fit(df3a)
val predictions = model.transform(df3a) */
val pipeline = new Pipeline().setStages(Array(assembler, kmeans))
val model = pipeline.fit(df3g)
val predictions = model.transform(df3g)

predictions.select("UniqueCarrier","prediction").show

// Afficher les coordonées des clusters pour caractériser les différents groupes
//model.clusterCenters.foreach(println)
model.stages.last.asInstanceOf[KMeansModel].clusterCenters.foreach(println)

// On peut également faire la moyenne des coordonées de chaque groupe (moins efficace pour un petit nombre de clusters car nécéssite de nouveaux calculs):
//predictions.groupBy("prediction").agg((avg("ArrDelay")+avg("DepDelay")).as("TotalDelay")).sort("TotalDelay").show

// Après avoir visualisé les clusters (via Zeppelin ou autre) ou analysé les résultats, on affiche les companies des clusters pertinents :
predictions.select("UniqueCarrier","prediction").filter($"prediction" === 0).show

// On utilise le fichier des companies pour obtenir le nom des companies concernées
predictions
    .join(df_carriers,predictions("UniqueCarrier")===df_carriers("Code"))
    .select("UniqueCarrier","Description","ArrDelay","DepDelay","Prediction")
    //.filter($"prediction" === 0)
    .show(false) // afficher le nom complet (sans limite de caractères)

// Q4 Quels sont les 3 aéroports les plus/moins sujets aux retards (départ/arrivé)
val df4 = df.select(col("Origin"),col("Dest"),col("ArrDelay").cast("double"),col("DepDelay").cast("double"))
    .filter($"Cancelled" =!= 1 && $"Diverted" =!= 1)
    .na.fill(0,Seq("ArrDelay"))
    .na.fill(0,Seq("DepDelay"))

// On récupère les retards au départ et à l'arrivée
val df4o = df4.groupBy("Origin").agg(avg("DepDelay").as("DepDelay"))
val df4d = df4.groupBy("Dest").agg(avg("ArrDelay").as("ArrDelay"))

val df4g = df4o
    .join(df4d,df4o("Origin")===df4d("Dest"))
    .groupBy("Origin")
    .agg((avg("DepDelay")+avg("ArrDelay")).as("TotalDelay")) //avg nécéssaire pour compiler

df4g.sort(desc("TotalDelay")).limit(3).show

// On utilise le fichier des aéroports pour obtenir le nom des aéroports concernés
df4g
    .join(df_airports,df4g("Origin")===df_airports("iata"))
    .select("Origin","airport","TotalDelay")
    .sort(desc("TotalDelay"))
    .limit(3)
    .show(false)