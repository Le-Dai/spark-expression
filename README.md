# spark-expression
基于spark 使用janino实现动态字节码生成 动态函数调用

```scala

val conf = new SparkConf()
conf.set("spark.master",conf.get("spark.master","local[*]"))
/** ***************************************************************************************************************
 * sparksession
 */
val spark = SparkSession
  .builder()
  .config(conf)
  .config("spark.sql.crossJoin.enabled","true")
  .config(StaticSQLConf.CATALOG_IMPLEMENTATION.key, "in-memory")
  //    .enableHiveSupport()
  .appName(this.getClass.getName)
  .config("spark.sql.shuffle.partitions", "5")
  .getOrCreate()
import spark.implicits._

val random = new java.util.Random()
val frame: DataFrame = spark.sparkContext.parallelize(1 to 100000, 50).map(i => {
  (i, "name", random.nextInt(100))
}).toDF("id", "name", "age")

val code =
  """
    |public String process(String a,int b){
    |   return a + " age is :" + b;
    |}
    |
    |""".stripMargin

frame.select(ssfunctions.codeInvoke(code, "process", frame.col("name").expr, frame.col("age").expr).as("alias")).show()

```