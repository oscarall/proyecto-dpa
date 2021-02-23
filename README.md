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


## Nota
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

2. Ejecutar la función `ingesta_inicial` o `ingesta_consecutiva` según sea el caso. Estas funciones pedirán al API de Socrata la información de las inspecciones. Para el caso de la función `ingesta inicial` solo recibe 2 parámetros: el cliente generado en el paso 1 y el límite de registros a consultar. Por su parte la función `ingesta_consecutiva` recibe los mismos parámetros de `ingesta_inicial` y además recibe la fecha a partir de la cual se quieren consultar registros. Ambos regresan los datos consultados serializados en binario usando el módulo [pickle](https://docs.python.org/3/library/pickle.html). 

```python
# ingesta inicial
data = ingesta_inicial(client, limit=250000)

# ingesta consecutiva
data = ingesta_consecutiva(client, date="2021-02-21", limit=1000)
```

3. Por último, el resultado del paso 2 se debe almacenar en [S3](https://aws.amazon.com/es/s3/). La función `guardar_ingesta` internamente crea un cliente para conectarse con el API de AWS usando la función `get_s3_resource`. Las credenciales para conectarse a AWS se obtienen del archivo `credentials.yml`. Los parámetros recibe `guardar_ingesta` son: el nombre del bucket, la ruta donde se guardará y los datos a guardar. 

```python
guardar_ingesta("bucket", "ingestion/consecutive/consecutive-inspections", data)
```

### Ingesta inicial

Ejemplo de como generar la ingesta inicial

```python
from src.pipeline.ingesta_almacenamiento import (
    get_client,
    ingesta_inicial, 
    guardar_ingesta
)

client = get_client()
bucket = "data-product-architecture-equipo-2"
path = "ingestion/initial/historic-inspections"

data = ingesta_inicial(client, 250000)

guardar_ingesta(bucket, path, data)
```

### Ingesta consecutiva

Ejemplo de como generar la ingesta consecutiva

```python
from src.pipeline.ingesta_almacenamiento import (
    get_client,
    ingesta_consecutiva, 
    guardar_ingesta
)

client = get_client()
bucket = "data-product-architecture-equipo-2"
date = "2021-02-17"
path = "ingestion/consecutive/consecutive-inspections"

data = ingesta_consecutiva(client, date, 1000)

guardar_ingesta(bucket, path, data)
```

### Nota

Los scripts ejemplos se deben correr en la raíz del proyecto. Para correrlo en otro lugar habría que modificar la variable `CREDENTIALS_FILE` en el archivo `ingesta_almacenamiento.py` para modificar la ruta relativa.

Del mismo modo se espera que el usuario tenga un bucket llamado **data-product-architecture-equipo-2**, a lo que en su defecto se tendrá que actualizar en los scripts de ejemplo la variable `bucket` 