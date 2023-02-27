from pyspark.sql import SparkSession
import pyspark.sql.functions as psf
from pyspark.sql.types import *


spark = SparkSession.builder.getOrCreate()

df1 = spark.read.option("delimiter", ";").csv("C:/Users/etowe/Downloads/2013-2016_wind_prediction_data/la-haute-borne-data-2013-2016.csv")

Schema=StructType([
    StructField("Wind_turbine_name", StringType(),nullable=True),
    StructField("Date_time", TimestampType(),nullable=True),
    StructField("Ba_avg", FloatType(), nullable=True),
    StructField("Ba_min", FloatType(), nullable=True),
    StructField("Ba_max", FloatType(), nullable=True),
    StructField("Ba_std", FloatType(), nullable=True),
    StructField("Rt_avg", FloatType(), nullable=True),
    StructField("Rt_min", FloatType(), nullable=True),
    StructField("Rt_max", FloatType(), nullable=True),
    StructField("Rt_std", FloatType(), nullable=True),
    StructField("DCs_avg", FloatType(), nullable=True),
    StructField("DCs_min", FloatType(), nullable=True),
    StructField("DCs_max", FloatType(), nullable=True),
    StructField("DCs_std", FloatType(), nullable=True),
    StructField("Cm_avg", FloatType(), nullable=True),
    StructField("Cm_min", FloatType(), nullable=True),
    StructField("Cm_max", FloatType(), nullable=True),
    StructField("Cm_std", FloatType(), nullable=True),
    StructField("P_avg", FloatType(), nullable=True),
    StructField("P_min", FloatType(), nullable=True),
    StructField("P_max", FloatType(), nullable=True),
    StructField("P_std", FloatType(), nullable=True),
    StructField("Q_avg", FloatType(), nullable=True),
    StructField("Q_min", FloatType(), nullable=True),
    StructField("Q_max", FloatType(), nullable=True),
    StructField("Q_std", FloatType(), nullable=True),
    StructField("S_avg", FloatType(), nullable=True),
    StructField("S_min", FloatType(), nullable=True),
    StructField("S_max", FloatType(), nullable=True),
    StructField("S_std", FloatType(), nullable=True),
    StructField("Cosphi_avg", FloatType(), nullable=True),
    StructField("Cosphi_min", FloatType(), nullable=True),
    StructField("Cosphi_max", FloatType(), nullable=True),
    StructField("Coshpi_std", FloatType(), nullable=True),
    StructField("Ds_avg", FloatType(), nullable=True),
    StructField("Ds_min", FloatType(), nullable=True),
    StructField("Ds_max", FloatType(), nullable=True),
    StructField("Ds_std", FloatType(), nullable=True),
    StructField("Db1t_avg", FloatType(), nullable=True),
    StructField("Db1t_min", FloatType(), nullable=True),
    StructField("Db1t_max", FloatType(), nullable=True),
    StructField("Db1t_std", FloatType(), nullable=True),
    StructField("Db2t_avg", FloatType(), nullable=True),
    StructField("Db2t_min", FloatType(), nullable=True),
    StructField("Db2t_max", FloatType(), nullable=True),
    StructField("Db2t_std", FloatType(), nullable=True),
    StructField("Dst_avg", FloatType(), nullable=True),
    StructField("Dst_min", FloatType(), nullable=True),
    StructField("Dst_max", FloatType(), nullable=True),
    StructField("Dst_std", FloatType(), nullable=True),
    StructField("Gb1t_avg", FloatType(), nullable=True),
    StructField("Gb1t_min", FloatType(), nullable=True),
    StructField("Gb1t_max", FloatType(), nullable=True),
    StructField("Gb1t_std", FloatType(), nullable=True),
    StructField("Gb2t_avg", FloatType(), nullable=True),
    StructField("Gb2t_min", FloatType(), nullable=True),
    StructField("Gb2t_max", FloatType(), nullable=True),
    StructField("Gb2t_std", FloatType(), nullable=True),
    StructField("Git_avg", FloatType(), nullable=True),
    StructField("Git_min", FloatType(), nullable=True),
    StructField("Git_max", FloatType(), nullable=True),
    StructField("Git_std", FloatType(), nullable=True),
    StructField("Gost_avg", FloatType(), nullable=True),
    StructField("Gost_min", FloatType(), nullable=True),
    StructField("Gost_max", FloatType(), nullable=True),
    StructField("Gost_std", FloatType(), nullable=True),
    StructField("Ya_avg", FloatType(), nullable=True),
    StructField("Ya_min", FloatType(), nullable=True),
    StructField("Ya_max", FloatType(), nullable=True),
    StructField("Ya_std", FloatType(), nullable=True),
    StructField("Yt_avg", FloatType(), nullable=True),
    StructField("Yt_min", FloatType(), nullable=True),
    StructField("Yt_max", FloatType(), nullable=True),
    StructField("Yt_std", FloatType(), nullable=True),
    StructField("Ws1_avg", FloatType(), nullable=True),
    StructField("Ws1_min", FloatType(), nullable=True),
    StructField("Ws1_max", FloatType(), nullable=True),
    StructField("Ws1_std", FloatType(), nullable=True),
    StructField("Ws2_avg", FloatType(), nullable=True),
    StructField("Ws2_min", FloatType(), nullable=True),
    StructField("Ws2_max", FloatType(), nullable=True),
    StructField("Ws2_std", FloatType(), nullable=True),
    StructField("Ws_avg", FloatType(), nullable=True),
    StructField("Ws_min", FloatType(), nullable=True),
    StructField("Ws_max", FloatType(), nullable=True),
    StructField("Ws_std", FloatType(), nullable=True),
    StructField("Wa_avg", FloatType(), nullable=True),
    StructField("Wa_min", FloatType(), nullable=True),
    StructField("Wa_max", FloatType(), nullable=True),
    StructField("Wa_std", FloatType(), nullable=True),
    StructField("Va1_avg", FloatType(), nullable=True),
    StructField("Va1_min", FloatType(), nullable=True),
    StructField("Va1_max", FloatType(), nullable=True),
    StructField("Va1_std", FloatType(), nullable=True),
    StructField("Va2_avg", FloatType(), nullable=True),
    StructField("Va2_min", FloatType(), nullable=True),
    StructField("Va2_max", FloatType(), nullable=True),
    StructField("Va2_std", FloatType(), nullable=True),
    StructField("Va_avg", FloatType(), nullable=True),
    StructField("Va_min", FloatType(), nullable=True),
    StructField("Va_max", FloatType(), nullable=True),
    StructField("Va_std", FloatType(), nullable=True),
    StructField("Ot_avg", FloatType(), nullable=True),
    StructField("Ot_min", FloatType(), nullable=True),
    StructField("Ot_max", FloatType(), nullable=True),
    StructField("Ot_std", FloatType(), nullable=True),
    StructField("Nf_avg", FloatType(), nullable=True),
    StructField("Nf_min", FloatType(), nullable=True),
    StructField("Nf_max", FloatType(), nullable=True),
    StructField("Nf_std", FloatType(), nullable=True),
    StructField("Nu_avg", FloatType(), nullable=True),
    StructField("Nu_min", FloatType(), nullable=True),
    StructField("Nu_max", FloatType(), nullable=True),
    StructField("Nu_std", FloatType(), nullable=True),
    StructField("Rs_avg", FloatType(), nullable=True),
    StructField("Rs_min", FloatType(), nullable=True),
    StructField("Rs_max", FloatType(), nullable=True),
    StructField("Rs_std", FloatType(), nullable=True),
    StructField("Rbt_avg", FloatType(), nullable=True),
    StructField("Rbt_min", FloatType(), nullable=True),
    StructField("Rbt_max", FloatType(), nullable=True),
    StructField("Rbt_std", FloatType(), nullable=True),
    StructField("Rm_avg", FloatType(), nullable=True),
    StructField("Rm_min", FloatType(), nullable=True),
    StructField("Rm_max", FloatType(), nullable=True),
    StructField("Rm_std", FloatType(), nullable=True),
    StructField("Pas_avg", FloatType(), nullable=True),
    StructField("Pas_min", FloatType(), nullable=True),
    StructField("Pas_max", FloatType(), nullable=True),
    StructField("Pas_std", FloatType(), nullable=True),
    StructField("Wa_c_avg", FloatType(), nullable=True),
    StructField("Wa_c_min", FloatType(), nullable=True),
    StructField("Wa_c_max", FloatType(), nullable=True),
    StructField("Wa_c_std", FloatType(), nullable=True),
    StructField("Na_c_avg", FloatType(), nullable=True),
    StructField("Na_c_min", FloatType(), nullable=True),
    StructField("Na_c_max", FloatType(), nullable=True),
    StructField("Na_c_std", FloatType(), nullable=True),

])
#Needed columns; Date(Date_time), temp_c(Ot), wind_ms(ws,ws1,ws2), gust_ms, wind_degree, wind_dir(wa,wa_c), active_power(p)
df2 = spark.read.option("mode","DROPMALFORMED").option("delimiter", ";").option("header", True).schema(Schema).csv("C:/Users/etowe/Downloads/2017-2020_wind_prediction_data/la-haute-borne-data-2017-2020.csv")
"""ba_avg_avg = df2.select(psf.mean("Ba_avg")).collect()
ba_avg_avg_float = float(ba_avg_avg[0][0])
print(ba_avg_avg_float)

ba_min_avg = df2.select(psf.mean("Ba_min")).collect()
ba_min_avg_float = float(ba_min_avg[0][0])
print(ba_min_avg_float)
df2.limit(100).fillna(value=ba_avg_avg_float).show(truncate=False)"""

df3 = df2.select(df2['Date_time'].alias("date"),
           df2['Ot_avg'].alias("tempt_c"),
           df2['Wa_c_avg'].alias("wind_direction"),
           df2['Ws_avg'].alias("wind_speed"),
           df2['Ws1_avg'].alias("wind_speed_2"),
           df2['Ws2_avg'].alias("wind_speed_3"),
           df2['P_avg'].alias("active_power"),
           df2['Q_avg'].alias("reactive_power")
           )
df4=df3.withColumn("active_power", psf.when(df3["active_power"] < 0, 0).otherwise(psf.col("active_power")))
df5=df4.withColumn("reactive_power", psf.when(df4["reactive_power"] < 0, 0).otherwise(psf.col("reactive_power")))

tempt_c_avg = df5.select(psf.mean("tempt_c")).collect()
tempt_c_avg_flt = float(tempt_c_avg[0][0])
#print(tempt_c_avg_flt)

wind_direction_avg = df5.select(psf.mean("wind_direction")).collect()
wind_direction_avg_flt = float(wind_direction_avg[0][0])
#print(wind_direction_avg_flt)

wind_speed_avg = df5.select(psf.mean("wind_speed")).collect()
wind_speed_avg_flt = float(wind_speed_avg[0][0])
#print(wind_speed_avg_flt)

wind_speed_2_avg = df5.select(psf.mean("wind_speed_2")).collect()
wind_speed_2_avg_flt = float(wind_speed_2_avg[0][0])
#print(wind_speed_2_avg_flt)

wind_speed_3_avg = df5.select(psf.mean("wind_speed_3")).collect()
wind_speed_3_avg_flt = float(wind_speed_3_avg[0][0])
#print(wind_speed_3_avg_flt)

active_power_avg = df5.select(psf.mean("active_power")).collect()
active_power_avg_flt = float(active_power_avg[0][0])
#print(active_power_avg_flt)

reactive_power_avg = df5.select(psf.mean("reactive_power")).collect()
reactive_power_avg_flt = float(reactive_power_avg[0][0])
#print(reactive_power_avg_flt)

values={"tempt_c": tempt_c_avg_flt, "wind_direction":wind_direction_avg_flt, "wind_speed":wind_speed_avg_flt, "wind_speed_2":wind_speed_2_avg_flt, "wind_speed_3":wind_speed_3_avg_flt, "active_power":active_power_avg_flt,"reactive_power":reactive_power_avg_flt}
#Fill Null values inside Department column with the word 'Generalist'
#df3.fillna(value=values).show()
#df5.fillna(value=values).show()

df5.fillna(value=values).coalesce(1).write.mode('overwrite').options(header='True', delimiter=';').csv("C:\\Users\\etowe\\Downloads\\2017-2020_wind_prediction_data\\2017-2020_cleaned")
