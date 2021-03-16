# Proyecto DPA

## Integrantes del equipo

* Sergio Sánchez Reyes
* Fernanda Tellez Girón
* Pedro Latapi
* Oscar Allan Ruiz Toledo

## Setup

**Versión de Python:** >= 3.7.0

Es altamente recomendable crear un [ambiente virtual](https://docs.python.org/3/library/venv.html) para evitar problemas en las dependencias. 

### Instalación

Para instalar todas las dependencias del proyecto correr el siguiente comando en la raíz del proyecto:

```bash
pip install -r requirements.txt
```

## Datos a utilizar

* Fuente : 'https://data.cityofchicago.org/Health-Human-Services/Food-Inspections/4ijn-s7e5/data'
* Número de registros : 215,026
* Número de columnas : 17

## Variables

* Inspection ID : ID de la inspección.
* DBA NAME : (Doing Business As) Nombre del establecimiento
* AKA NAME : (Also known as) Nombre alternativo del establecimiento
* License # : Número de licencia 
* Facility Type : Tipo de establecimiento
* Risk : Nivel de riesgo asociado
* Address : Dirección
* City : Ciudad
* State : Estado
* Zip : Código Postal
* Inspection Date : Fecha de inspección
* Inspection Type : Tipo de inspección
* Results : Resultado de la inspección
* Violations : Violaciones detectadas
* Latitude : Latitud 
* Longitude : Longitud
* Location : Ubicación

**Frecuencia de actualización:** Diaria

## Problema 

¿Es posible predecir el resultado de una inspección sanitaria y cuales son los drivers de estos resultados?


## EDA

En la carpeta `notebooks/eda` podrás encontrar un análsis exploratorio de los datos.

Para la correcta visualización del notebook **EDA_foof_inspections.ipynb** es necesario actualizar
la variable `PATH` con la ruta en la que se encuentran el archivo **Food_Inspections.csv**

## Ingesta

Antes iniciar hay que crear un archivo [Yaml](https://yaml.org/) en la carpeta `conf/local` que tenga la siguiente estructura:

```yaml
s3:
  aws_access_key_id: your_access_key_id
  aws_secret_access_key: your_secret_key
food_inspections:
  api_token: your_socrata_app_token
```

El nombre del archivo deberá ser `credentials.yml`.

1. Crear un cliente para conectarse al API de Socrata usando la función `get_client()`. Esta función usará el app token declarado en el archivo `credentials.yml`

```python
client = get_client()
```

2. Ejecutar la función `ingesta_inicial` o `ingesta_consecutiva` según sea el caso. Estas funciones pedirán al API de Socrata la información de las inspecciones. Para el caso de la función `ingesta inicial` solo recibe 2 parámetros: el cliente generado en el paso 1 y el límite de registros a consultar. Por su parte la función `ingesta_consecutiva` recibe los mismos parámetros de `ingesta_inicial` y además recibe la fecha a partir de la cual se quieren consultar registros. Una vez que los datos se consultan se suben a [S3](https://aws.amazon.com/es/s3/). La función `guardar_ingesta` internamente crea un cliente para conectarse con el API de AWS usando la función `get_s3_resource`. Las credenciales para conectarse a AWS se obtienen del archivo `credentials.yml`. Los parámetros recibe `guardar_ingesta` son: el nombre del bucket, la ruta donde se guardará y los datos a guardar. Los datos consultados se guardan serializados en binario usando el módulo [pickle](https://docs.python.org/3/library/pickle.html).

```python
# ingesta inicial
ingesta_inicial(client, limit=250000)

# ingesta consecutiva
ingesta_consecutiva(client, date="2021-02-21", limit=1000)
```

### Ingesta inicial

Ejemplo de como generar la ingesta inicial

```python
from src.pipeline.ingesta_almacenamiento import (
    get_client,
    ingesta_inicial
)

client = get_client()
ingesta_inicial(client, 250000)
```

### Ingesta consecutiva

Ejemplo de como generar la ingesta consecutiva

```python
from src.pipeline.ingesta_almacenamiento import (
    get_client,
    ingesta_consecutiva
)

client = get_client()
date = "2021-02-17"
ingesta_consecutiva(client, date, 1000)
```

## Corriendo los tasks en Luigi

Actualizar las dependencias de tu virtual environment

```bash
pip install -r requirements.txt
```

Iniciar el servidor de Luigi

```bash
luigid --background
```

A partir de este momento ya se puede acceder desde tu navegador a la interfaz gráfica de Luigi en la dirección http://localhost:8082

Para ejecutar los tasks, correr el siguiente comando en la raíz del proyecto

```bash
PYTHONPATH='.' luigi --module src.pipeline.luigi.almacenamiento Almacenamiento --ingesta inicial --date "2021-03-16"
```

Los parámetros de los tasks son `ingesta` y `date`, los valores posibles para `ingesta` son `inicial` y `consecutiva`. El formato de la fecha es `Y-m-d`. Esto correra los dos tasks en caso de ser necesario.

### Nota

Los scripts ejemplos se deben correr en la raíz del proyecto. El proyecto está configurado esperando que el usuario tenga un bucket llamado **data-product-architecture-equipo-2**. En caso de necesitar modificar una variable como la ubicación del archivo `credentials.yml` o el nombre del bucket, éstas se encuentran en el archivo `src/utils/constants.py`
