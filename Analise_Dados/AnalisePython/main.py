import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
import math
import Analises as an

num_noites = "Número de noites"
ocupacao = "Ocupação"
adultos = "Adultos"
criancas = "Crianças"
bebes = "Bebés"
preco = "Preço (€)"

hotel = pd.read_csv("./Analise_Dados/DatasetOriginais/Hotel.csv", delimiter=';')
facilities = pd.read_csv("./Analise_Dados/DatasetOriginais/Facilities.csv", delimiter=';')
reservas = pd.read_csv("./Analise_Dados/DatasetOriginais/Quartos_Reservados.csv", delimiter=';')
tipologias = pd.read_csv("./Analise_Dados/DatasetOriginais/Tipologias.csv", delimiter=';')
eventos = pd.read_csv("./Analise_Dados/DatasetOriginais/Eventos.csv", delimiter=';')
meteorologia = pd.read_csv("./Analise_Dados/DatasetOriginais/Meteorologia.csv", delimiter=';')
feriados = pd.read_csv("./Analise_Dados/DatasetOriginais/Feriados.csv", delimiter=';')
final = pd.read_csv("./Analise_Dados/DataSetFinal.csv", delimiter=";")

an.outliersComplex(reservas, preco, "<= 1000")
