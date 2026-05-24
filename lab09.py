import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Plik CSV 
csv_filename = "sprzedaz_dane.csv"
if not os.path.exists(csv_filename):
    with open(csv_filename, "w", encoding="utf-8") as f:
        f.write("id,kategoria,cena,ilosc\n")
        f.write("1,Elektronika,1500.0,2\n")
        f.write("2,AGD,800.0,1\n")
        f.write("3,Elektronika,200.0,5\n")
        f.write("4,Zabawki,50.0,10\n")
        f.write("5,AGD,120.0,3\n")

print(f"Utworzono plik: {csv_filename}")

# Inicjalizacja sesji Spark
spark = SparkSession.builder \
    .appName("DataFrameExample") \
    .getOrCreate() 

print("\nRozpoczęcie operacji DataFrame")

# Wczytanie danych do DataFrame 
df = spark.read.csv(csv_filename, header=True, inferSchema=True)

# Wyświetlanie i schemat 
print("Schemat danych:")
df.printSchema()
print("Podgląd danych:")
df.show()

# Selekcja kolumn
df_selected = df.select("kategoria", "cena")
print("Tylko kategoria i cena:")
df_selected.show()

# Filtrowanie wierszy (gdzie ilosc > 2)
df_filtered = df.filter(col("ilosc") > 2)
print("Filtrowanie: Ilość > 2:")
df_filtered.show()

# Grupowanie i agregacje 
df_grouped = df.groupBy("kategoria").agg({"cena": "sum", "ilosc": "sum"})
print("Grupowanie po kategorii i sumowanie:")
df_grouped.show()

# Zapis przetworzonego DataFrame do pliku Parquet
output_parquet = "wynik_sprzedaz.parquet"
df_grouped.write.mode("overwrite").parquet(output_parquet)
print(f"Zapisano wynik do katalogu: {output_parquet}")


sc = spark.sparkContext

# Wczytanie tego samego pliku CSV jako RDD bez nagłówka
rdd_raw = sc.textFile(csv_filename)
header = rdd_raw.first() 
rdd_data = rdd_raw.filter(lambda row: row != header) 

# Parsowanie wierszy - podział po przecinku
rdd_parsed = rdd_data.map(lambda row: row.split(","))

# Zliczanie liczby wierszy (akcja)
row_count = rdd_parsed.count()
print(f"Całkowita liczba wierszy (bez nagłówka): {row_count}")

# Obliczenie sumy z kolumny "ilosc" (indeks 3) przy użyciu map i reduce 
total_quantity = rdd_parsed.map(lambda row: int(row[3])).reduce(lambda a, b: a + b)
print(f"Suma sprzedanych produktów (łączna ilość): {total_quantity}")

spark.stop()