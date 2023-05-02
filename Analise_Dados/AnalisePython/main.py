import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
import math

hotel = pd.read_csv("../DatasetOriginais/Hotel.csv", delimiter=';')
facilities = pd.read_csv("../DatasetOriginais/Facilities.csv", delimiter=';')
reservas = pd.read_csv("../DatasetOriginais/Quartos_Reservados.csv", delimiter=';')
tipologias = pd.read_csv("../DatasetOriginais/Tipologias.csv", delimiter=';')
eventos = pd.read_csv("../DatasetOriginais/Eventos.csv", delimiter=';')
meteorologia = pd.read_csv("../DatasetOriginais/Meteorologia.csv", delimiter=';')
feriados = pd.read_csv("../DatasetOriginais/Feriados.csv", delimiter=';')
final = pd.read_csv("../DataSetFinal.csv", delimiter=";")

fig, axs = plt.subplots(1, 2)

df = reservas[reservas['Número de noites'] <= 14]

axs[0].boxplot(df["Número de noites"])
axs[1].boxplot(reservas["Número de noites"])

plt.show()