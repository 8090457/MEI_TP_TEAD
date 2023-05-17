import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

val spark = SparkSession.builder.appName("MyApp").getOrCreate()

val csvQuartosReservados = "/Users/joaobraganca/Documents/ESTG/TEAD/MEI_TP_TEAD/Analise_Dados/DatasetOriginais/Quartos_Reservados.csv"
val QuartosReservadosTable = "QuartosReservados"

var df = spark.read.format("csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .option("delimiter", ";")
  .option("trim", "true")
  .load(csvQuartosReservados)
  .withColumnRenamed("Hotel ID", "hotel_ID")
  .withColumnRenamed("Reserve ID", "Reserve_ID")
  .withColumnRenamed("País", "pais")
  .withColumnRenamed("Estado da reserva", "estado_reserva")
  .withColumnRenamed("Room ID", "room_ID")
  .withColumnRenamed("Tipo de Quarto", "tipo_quarto")
  .withColumnRenamed("RatePlan", "rate_plan")
  .withColumn("data_reserva", to_date(col("Data da reserva"), "dd/MM/yyyy"))
  .withColumn("data_chegada", to_date(col("Data chegada"), "dd/MM/yyyy"))
  .drop("Data chegada")
  .withColumn("data_partida", to_date(col("Data de partida"), "dd/MM/yyyy"))
  .drop("Data de partida")
  .withColumnRenamed("Número de noites", "num_noites")
  .withColumnRenamed("Ocupação", "ocupacao")
  .withColumnRenamed("Adultos", "adultos")
  .withColumnRenamed("Crianças", "criancas")
  .withColumnRenamed("Bebés", "bebes")
  .withColumnRenamed("Preço (€)", "preco_euros")

df.createOrReplaceTempView(QuartosReservadosTable)
df.cache()
df.printSchema()
df.show()

// Calcular diferença de dias
df = df.withColumn("days_diff", datediff(col("data_partida"), col("data_chegada")))

// Comparar colunas
df = df.withColumn("compareDaysDiff", when(col("days_diff") === col("num_noites"), true).otherwise(false))

// Contar quantas linhas são falsas
val false_rows = df.filter(col("compareDaysDiff") === false)
val num_false_rows = false_rows.count()
println(num_false_rows)

// Validar se data de partida é maior que data de chegada
df = df.withColumn("partidaMaiorChegada", when(col("data_chegada") < col("data_partida"), true).otherwise(false))

// Contar falsos
val false_rows = df.filter(col("partidaMaiorChegada") === false)
val num_false_rows = false_rows.count()
println(num_false_rows)

// Validar se data de reserva menor que data de chegada 
df = df.withColumn("reservaMenorChegada", when(col("data_reserva") <= col("data_chegada"), true).otherwise(false))

// Contar falsos
val false_rows = df.filter(col("reservaMenorChegada") === false)
val num_false_rows = false_rows.count()
println(num_false_rows)

df.write.csv("results")
