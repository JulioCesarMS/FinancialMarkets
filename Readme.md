[![My Skills](https://skillicons.dev/icons?i=py,html,css,git,mysql,vscode)](https://skillicons.dev)

# 📁 **Financial Markets**

#![Financial Markets](./figures/ima01.png)


Proyecto de ingeniería de datos orientado a la extracción, transformación y almacenamiento automatizado de información financiera de distintos mercados internacionales.

El sistema descarga diariamente precios históricos y actualizados de múltiples índices y mercados financieros, incluyendo:

- S&P 500
- NASDAQ
- IPC México
- DAX
- FTSE100
- FOREX

La información es procesada mediante pipelines ETL desarrollado en Python y almacenada en una base de datos MySQL para su posterior análisis, visualización y modelado.

El proyecto integra:

- Extracción automatizada de datos financieros
- Transformación y limpieza de información
- Almacenamiento estructurado en MySQL
- Automatización de pipelines con Prefect
- Containerización utilizando Docker
- Arquitectura modular orientada a escalabilidad

La base de datos incluye tablas para:

- Mercados financieros
- Empresas
- Series históricas de precios


El objetivo principal del proyecto es construir una arquitectura reproducible y automatizada para análisis financiero y futuros procesos de analítica, machine learning y monitoreo de mercados.







# Requerimientos:
- [Python 3.12.0](https://www.python.org/)
- [MySQL](https://dev.mysql.com/downloads/workbench/)
- [Git](https://git-scm.com/)
- [Docker](https://www.docker.com/)
- [VScode](https://code.visualstudio.com/)

# Ejecución del proyecto

- Descargar el proyecto en local : **Desktop** <break> 
- Crear un ambiente virtual  <break>
- Intalar dependencias <break> 
- Activar el ambiente virtual <break> 


 
# Estructura del Proyecto

El proyecto está estructurado de la siguiente manera:
 
    FINANCIALMARKETS/
          │
          ├── config/                # Configuración general del proyecto
          |   ├── settings/          # configuraciones generales, 
          ├── data/                  # Datos crudos, procesados y temporales
          |   ├── processed/         #
          |   ├── raw/               # datos crudos: companies.xlsx, y markets.xlsx
          ├── figures/               # Imágenes y visualizaciones exportadas
          ├── logs/                  # Logs de ejecución y monitoreo
          ├── notebooks/             # Jupyter notebooks para análisis y exploración
          ├── orchestration/         # Flujos de orquestación (Prefect, schedules, deployments)
          |   ├── perfec_flow/       # tareas
          |   ├── deploy/            # tdespliegue
          ├── sql/                   # Scripts SQL, queries y creación de tablas
          ├── src/                   # Código fuente principal del proyecto
          │   ├── database/          # Conexión y operaciones con base de datos
          │   |   ├── tables/        # tablas formato SQL
          │   |   ├── create_tables/ # Creación de tablas en MySQL
          │   |   ├── mysql_client/  # Conexión a MySQL
          │   ├── extraction/        # Extracción de datos desde APIs y fuentes externas
          │   |   ├── downloader/    # descarga información de yahoo-finance
          │   |   ├── extractor/     # extrae información marcado y compañias
          │   ├── load/              # Carga de datos hacia almacenamiento o DB
          │   |   ├── loader/        # carga información en Base de datos
          │   ├── pipeline/          # Pipeline principal de procesamiento
          │   |   ├── run_pipeline/  # Ejecuta el todo el proceso 
          │   ├── transformation/    # Limpieza y transformación de datos
          │   |   ├── transformer/   # Transformación de datos antes de la carga a base de datos
          │   ├── utils/             # Funciones auxiliares y utilidades
          │   |   ├── getData/       # 
          │   |   ├── getLastDate/   # 
          │   |   ├── getMarketid/   # 
          │   |   ├── getSymbols/    # 
          ├── .env                   # Variables de entorno
          ├── docker-compose.yml     # Orquestación de contenedores Docker
          ├── Dockerfile             # Imagen Docker de la aplicación
          ├── main.py                # Punto de entrada principal del proyecto
          ├── README.md              # Documentación principal
          └── requirements.txt       # Dependencias Python

 # 1.- Intalación de Python y otras dependencias
 
Descargar e instalas todas herramientas en requerimientos.

# 2.- Clonar el proyecto a una carpeta en escritorio
 
- Crear una carpeta en escritorio p.e. "FinancialMarkets" <break> 
- Click derecho en cualquier lugar dentro de la carpeta y seleccionar **"Git Bash Here"** <break> 
- En la consola de Git ingtroducir siguiente comandos: <break> 
  ```bash
  git clone https://github.com/usuario/financialmarkets.git

  cd financialmarkets
  ```
  - Esperar unos minutos a que descargue los archivos en la carpeta
  

## 3. Crear archivo `.env`

Crear en MySQL una conexión, con usuario, y contraseña, posteriormente una base llamada "financialmarkets". Con esa información  crear un archivo `.env` en la raíz del proyecto:

```env
DB_HOST=host.docker.internal
DB_PORT=3306
DB_NAME=financialmarkets
DB_USER= usuario raíz en MySQL
DB_PASSWORD= contraseña para acceder a la conexión en MySQL
```
 
# 3.- Creación de ambiente virtual

 Es recomendable crear un ambiente virtual para fijar la versión de python, así como las dependencias instaladas.

Crear entorno virtual:

```bash
python -m venv venv
```

Activar entorno:

En Windows

```bash
venv\Scripts\activate
```


## 4. Construir y ejecutar Docker

Ejecutar el siguiente comando:

```bash
docker compose up --build
```

Este comando:
- Construye la imagen Docker
- Levanta el contenedor
- Ejecuta automáticamente el pipeline principal

para detener el contenedor:
```bash
docker compose down
```

ver logs del contenedor: 
```bash
docker compose logs -f
```

## 5. Orquestación con Prefect

El proyecto utiliza Prefect para automatizar la ejecución de pipelines.

Ejecutar deployment:

```bash
python orchestration/deploy.py
```


