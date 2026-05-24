from src.database.mysql_client import MySQLDatabase
from src.database.tables import table_markets, table_companies, tabla_precios 

# conexión a base de datos
#db = MySQLDatabase("financialmarkets")


class CreateTables(MySQLDatabase):
    
    def __init__(self, database):
        super().__init__(database)
    
    # creamos la tabla de mercados
    def markets(self):
        query = table_markets()
        self.execute(query)
        
    # creamos la tabla companias
    def companies(self):
        query = table_companies()
        self.execute(query)
        
    # creamos la tabla precios historicos
    def prices(self, market):
        query = tabla_precios(market)
        self.execute(query)




