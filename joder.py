import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# Crear la conexión con SQL Server
engine = create_engine('mssql+pymssql://sa:PassFranklinTec!@192.168.0.28:1433', isolation_level="AUTOCOMMIT")

# Comprobar y crear la base de datos si no existe
with engine.connect() as conn:
    result = conn.execute(text("SELECT name FROM sys.databases WHERE name='lab06db'"))
    if not result.fetchone():
        conn.execute(text("CREATE DATABASE lab06db"))
        print("Base de datos 'lab06db' creada.")
    else:
        print("La base de datos 'lab06db' ya existe.")

# Leer archivos de datos
def process_data_files():
    try:
        # Asegúrate de que la carpeta que contiene los archivos existe
        if not os.path.exists('./ml-100k'):
            raise FileNotFoundError("La carpeta 'ml-100k' no existe. Asegúrate de que los archivos estén en el directorio correcto.")
        
        # 1. Leer 'u.genre' (GENERO)
        genre_df = pd.read_csv('./ml-100k/u.genre', sep='|', names=['genre_name', 'id'])
        print(f"Leídos {len(genre_df)} géneros")

        # 2. Leer 'u.occupation' (OCUPACION)
        occupation_df = pd.read_csv('./ml-100k/u.occupation', sep='|', names=['occupation_name'])
        print(f"Leídas {len(occupation_df)} ocupaciones")

        # 3. Leer 'u.user' (información demográfica)
        users_df = pd.read_csv('./ml-100k/u.user', sep='|', names=['user_id', 'age', 'gender', 'occupation', 'zip_code'])
        print(f"Leídos {len(users_df)} usuarios")

        # 4. Leer 'u.item' (información sobre películas)
        items_columns = ['movie_id', 'title', 'release_date', 'video_release_date', 'imdb_url'] + [f'genre_{i}' for i in range(19)]
        items_df = pd.read_csv('./ml-100k/u.item', sep='|', names=items_columns, encoding='ISO-8859-1')
        print(f"Leídas {len(items_df)} películas")

        # Agrupar géneros
        genres_columns = items_columns[5:]
        items_df['genres'] = items_df[genres_columns].apply(
            lambda row: ','.join([str(i) for i, val in enumerate(row) if val == 1]), axis=1
        )
        items_df = items_df[['movie_id', 'title', 'release_date', 'video_release_date', 'imdb_url', 'genres']]

        # 5. Leer 'u.data' (valoraciones)
        ratings_df = pd.read_csv('./ml-100k/u.data', sep='\t', names=['user_id', 'movie_id', 'rating', 'timestamp'])
        ratings_df['timestamp'] = pd.to_datetime(ratings_df['timestamp'], unit='s')
        print(f"Leídos {len(ratings_df)} valoraciones")

        # 6. Análisis de los datos
        analyze_data(users_df, items_df, ratings_df)

        # 7. Insertar en SQL Server (no incluido en el código que proporcionaste)
        # insert_data_sql(users_df, items_df, ratings_df)

    except SQLAlchemyError as e:
        print(f"Error al conectar con la base de datos: {e}")
    except FileNotFoundError as e:
        print(f"Error de archivo: {e}")
    except Exception as e:
        print(f"Ocurrió un error: {e}")

# Asegúrate de que la carpeta para guardar los gráficos exista
if not os.path.exists('plots'):
    os.makedirs('plots')

def analyze_data(users_df, items_df, ratings_df):
    # Análisis básico de los datos
    print("Número de usuarios:", users_df.shape[0])
    print("Número de películas:", items_df.shape[0])
    print("Número de calificaciones:", ratings_df.shape[0])

    # Gráfico de distribución de calificaciones
    plt.figure(figsize=(10, 5))
    sns.histplot(ratings_df['rating'], bins=5, kde=False, color='purple', edgecolor='black')
    plt.title('Distribución de Calificaciones', fontsize=16)
    plt.xlabel('Calificación', fontsize=14)
    plt.ylabel('Cantidad', fontsize=14)
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    plt.savefig('plots/distribucion_calificaciones.png')
    plt.close()

    # Calcular el promedio de calificaciones por película
    avg_ratings_per_movie = ratings_df.groupby('movie_id')['rating'].mean()

    # Gráfico de promedio de calificaciones por película
    plt.figure(figsize=(8, 6))
    sns.histplot(avg_ratings_per_movie, kde=True, color='orange', bins=30)
    plt.title('Distribución de Promedio de Calificaciones por Película', fontsize=14, fontweight='bold')
    plt.xlabel('Promedio de Calificación', fontsize=12)
    plt.ylabel('Frecuencia', fontsize=12)
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    plt.savefig('plots/distribucion_promedio_calificaciones.png')
    plt.close()

    # Gráfico de calificaciones por usuario
    plt.figure(figsize=(10, 5))
    ratings_per_user = ratings_df.groupby('user_id')['rating'].count()
    sns.histplot(ratings_per_user, kde=True, color='blue')
    plt.title('Distribución de Calificaciones por Usuario', fontsize=16)
    plt.xlabel('Número de Calificaciones', fontsize=14)
    plt.ylabel('Frecuencia', fontsize=14)
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    plt.savefig('plots/distribucion_calificaciones_usuario.png')
    plt.close()

    # Gráfico de calificaciones por película
    plt.figure(figsize=(10, 5))
    ratings_per_item = ratings_df.groupby('movie_id')['rating'].count()
    sns.histplot(ratings_per_item, kde=True, color='green')
    plt.title('Distribución de Calificaciones por Película', fontsize=16)
    plt.xlabel('Número de Calificaciones', fontsize=14)
    plt.ylabel('Frecuencia', fontsize=14)
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    plt.savefig('plots/distribucion_calificaciones_pelicula.png')
    plt.close()

    print("Gráficos guardados en la carpeta 'plots'.")

if __name__ == "__main__":
    process_data_files()
