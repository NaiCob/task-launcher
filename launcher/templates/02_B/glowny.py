import os
import findspark
from local_python_transformations.wspolne.funkcje import *
from local_python_transformations.wspolne.konfiguracja_wspolne import *
from pyspark.sql import SparkSession
from funkcje_specyficzne_dostawca import transformacje_specyficzne_dla_dostawcy, wylicz_standardowe_kolumny
import konfiguracja
from local_python_transformations.wspolne.stale import kolumnyDoDodania, kolumnyDoDodania_02_B

os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"
findspark.init()

spark = SparkSession.builder.getOrCreate()

df = wczytaj_plik_excel(konfiguracja.sciezka, konfiguracja.odWiersza, konfiguracja.arkusz)
df_klient = wczytaj_mapowanie_klienta(konfiguracja.sciezka_mapping)
df_asortyment = wczytaj_mapowanie_asortymentu(konfiguracja.sciezka_mapping)

df = transformacje_specyficzne_dla_dostawcy(df)

df = dodaj_wybrane_kolumny(df, kolumnyDoDodania_02_B)
df = dodaj_miesiac(df, miesiac, rok)
df = dodaj_dystrybutor(df, konfiguracja.dystrybutor)
df = wyczysc_NIP(df, 'NIP')

df = wylicz_standardowe_kolumny(df, df_asortyment, "Indeks_Opis", df_klient, 'Identyfikator odbiorcy')


df.to_csv(f'..\\output\{konfiguracja.dystrybutor}_out.csv', header=True, sep=';', encoding='cp1250', index=False)