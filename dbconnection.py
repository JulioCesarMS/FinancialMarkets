import mysql.connector
from mysql.connector import Error
import pandas as pd
import mysql

# Clase 
class MySQLDatabase():
    def __init__(self, database):
        self.host = "localhost"
        self.user = "root"
        self.password = "astro123"
        self.database = database
        self.connection = None

    # método para establecer conexión
    def connect(self):
        host = self.host
        database = self.database
        username = self.user
        password = self.password
        try:
            self.connection = mysql.connector.connect(host=host, 
                                            database=database, 
                                            user=username, 
                                            password=password)
            
            print("✅ Conexión exitosa")
            #return self.connection 
        except Error as e:
            print("Error: ",e)
            #self.connection = None
        
    # método para ejecutar un query
    def execute_query(self, query, values=None):
        """Ejecuta una consulta SQL y retorna un DataFrame."""

        
        if not self.connection or not self.connection.is_connected():
            self.connect()
        try:
            conn = self.connection
            cursor = conn.cursor(dictionary=True)
            cursor.execute(query, values)
            rows = cursor.fetchall()

            if rows:
                df = pd.DataFrame(rows)
            else:
                df = pd.DataFrame()

            cursor.close()
            return df

        except Error as e:
            print(f"Error ejecutando query: {e}")
            return None
    

    # método par cargar datos a una tabla
    def insert_to_db(self, df, tabla="viajes", batch_size=800, ignore_duplicates=True):
        """
        Inserta un DataFrame en una tabla MySQL por lotes.
        
        Parámetros:
            df : pd.DataFrame
                DataFrame a insertar.
            tabla : str
                Nombre de la tabla destino.
            batch_size : int
                Tamaño del lote para inserciones masivas.
        """
        # Conexión a MySQL
        if self.connection is None:
            self.connect()
            
        conn = self.connection
        cursor = conn.cursor()
        # Reemplazar NaN con None para evitar errores
        df = df.where(pd.notnull(df), None)
        # columnas
        cols = df.reset_index(drop=True).columns.tolist() 
        # Convertir DataFrame a lista de tuplas
        values = [tuple(row) for row in df.itertuples(index=False, name=None)]

        # Crear placeholders dinámicamente
        placeholders = ", ".join(["%s"] * len(cols))
        # Crear query
        if ignore_duplicates:
            insert_query = f"INSERT IGNORE INTO {tabla} ({', '.join(cols)}) VALUES ({placeholders})"
        else:
            insert_query = f"INSERT INTO {tabla} ({', '.join(cols)}) VALUES ({placeholders})"

        #insert_query = f"INSERT INTO {tabla} ({', '.join(cols)}) VALUES ({placeholders})"


        # Inserción por lotes
        if len(values) > batch_size:
            for start in range(0, len(values), batch_size):
                end = start + batch_size
                cursor.executemany(insert_query, values[start:end])
                conn.commit()
        else:
            cursor.executemany(insert_query, values)
            conn.commit()
        # Cerrar conexión
        cursor.close()


    def close(self):
        """Cierra la conexión."""
        if self.connection and self.connection.is_connected():
            self.connection.close()
            print("🔒 Conexión cerrada")
            self.connection = None

    