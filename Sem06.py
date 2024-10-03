import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# Crear la conexión con SQL Server
engine = create_engine('mssql+pymssql://sa:PassFranklinTec!@localhost:1433', isolation_level="AUTOCOMMIT")

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
        )  # Añadir paréntesis que faltan aquí
        items_df = items_df[['movie_id', 'title', 'release_date', 'video_release_date', 'imdb_url', 'genres']]

        # 5. Leer 'u.data' (valoraciones)
        ratings_df = pd.read_csv('./ml-100k/u.data', sep='\t', names=['user_id', 'movie_id', 'rating', 'timestamp'])
        ratings_df['timestamp'] = pd.to_datetime(ratings_df['timestamp'], unit='s')  # Convertir la marca de tiempo a formato datetime
        print(f"Leídos {len(ratings_df)} valoraciones")

        # 6. Análisis de los datos
        analyze_data(users_df, items_df, ratings_df)

        # 7. Insertar en SQL Server
        insert_data_sql(users_df, items_df, ratings_df)

    except SQLAlchemyError as e:
        print(f"Error al conectar con la base de datos: {e}")
    except FileNotFoundError as e:
        print(f"Error de archivo: {e}")
    except Exception as e:
        print(f"Ocurrió un error: {e}")

def analyze_data(users_df, items_df, ratings_df):
    # 1. Distribución de calificaciones
    rating_distribution = ratings_df['rating'].value_counts().sort_index()
    print("\nDistribución de calificaciones:")
    print(rating_distribution)

# Gráfico de distribución de calificaciones
    plt.figure(figsize=(10, 5))
    sns.histplot(ratings_df['rating'], bins=5, kde=False, color='purple', edgecolor='black')
    plt.title('Distribución de Calificaciones', fontsize=16)
    plt.xlabel('Calificación', fontsize=14)
    plt.ylabel('Cantidad', fontsize=14)
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    plt.show()

    # 2. Promedio de calificaciones por película
    avg_ratings_per_movie = ratings_df.groupby('movie_id')['rating'].mean().sort_values(ascending=False)
    print("\nPromedio de calificaciones por película:")
    print(avg_ratings_per_movie.head())

    # Gráfico de promedio de calificaciones por película
    plt.figure(figsize=(8, 6))
    sns.histplot(avg_ratings_per_movie, kde=True, color='orange', bins=30)  # Cambié el color a 'orange'
    plt.title('Distribución de Promedio de Calificaciones por Película', fontsize=14, fontweight='bold')  # Cambié la fuente
    plt.xlabel('Promedio de Calificación', fontsize=12)  # Cambié la fuente
    plt.ylabel('Frecuencia', fontsize=12)  # Cambié la fuente
    plt.xticks(fontsize=10)  # Cambié la fuente de los ejes
    plt.yticks(fontsize=10)  # Cambié la fuente de los ejes
    plt.grid(axis='y', linestyle='--', alpha=0.7)  # Añadí una cuadrícula para mejorar la visualización
    plt.show()

    # 3. Distribución de usuarios por ocupación y género
    user_distribution_by_gender = users_df['gender'].value_counts()
    user_distribution_by_occupation = users_df['occupation'].value_counts()

    print("\nDistribución de usuarios por género:")
    print(user_distribution_by_gender)

    print("\nDistribución de usuarios por ocupación:")
    print(user_distribution_by_occupation)

    # Gráfico de distribución de género
    plt.figure(figsize=(8, 5))
    sns.countplot(x='gender', data=users_df, palette='Set2')
    plt.title('Distribución de Usuarios por Género', fontsize=16)
    plt.xlabel('Género', fontsize=14)
    plt.ylabel('Cantidad', fontsize=14)
    plt.show()

    # Gráfico de distribución de ocupación
    plt.figure(figsize=(12, 6))
    sns.countplot(y='occupation', data=users_df, palette='Paired', order=user_distribution_by_occupation.index)
    plt.title('Distribución de Usuarios por Ocupación', fontsize=16)
    plt.xlabel('Cantidad', fontsize=14)
    plt.ylabel('Ocupación', fontsize=14)
    plt.show()

def insert_data_sql(users_df, items_df, ratings_df):
    try:
        # Insertar datos en la tabla 'Users'
        users_df.to_sql('Users', engine, if_exists='replace', index=False)
        print("Datos de usuarios insertados con éxito en la tabla 'Users'.")

        # Insertar datos en la tabla 'Movies'
        items_df.to_sql('Movies', engine, if_exists='replace', index=False)
        print("Datos de películas insertados con éxito en la tabla 'Movies'.")

        # Insertar datos en la tabla 'Ratings'
        ratings_df.to_sql('Ratings', engine, if_exists='replace', index=False)
        print("Datos de valoraciones insertados con éxito en la tabla 'Ratings'.")

    except SQLAlchemyError as e:
        print(f"Error al insertar datos en la base de datos: {e}")

# Ejecutar el procesamiento de archivos de datos
process_data_files()
