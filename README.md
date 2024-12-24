
## Tabla de contenidos
1. [General Info](#general-info)
2. [Tecnologias](#Tecnologias)
3. [Recursos CML](#Recursos_CML)
4. [Colaboradores](#Colaboradores)
5. [Instalación](#Instalación)
6. [Ejecución](#Ejecución)
7. [Explicación](#Explicación)
8. [Sugerencias y pasos siguientes](#Sugerencias)
9. [Estructura de carpetas](#plantilla_proyectos_python)
10. [Estructura de tablas y rutas](#Estructura_rutas_tablas)

# Visualización de Racionales Positivos

## Información General

El proyecto Visualización de Racionales Positivos tiene como objetivo la creación de un instrumento único que centraliza y visualiza los datos provenientes de diversas bases de datos. Este instrumento permite realizar un análisis profundo sobre los contribuyentes y sus características particulares, así como sobre sus relaciones comerciales con otras entidades.

Adicionalmente, este proyecto proporciona un análisis detallado del flujo de IVA dentro de los grupos económicos a los cuales pertenecen los contribuyentes. Esto permite obtener una visión integral y más clara sobre las interacciones comerciales que no son evidentes a simple vista. El resultado final es un dashboard que servirá como una herramienta útil para la toma de decisiones en áreas tributarias y financieras, permitiendo a los responsables regionales evaluar de forma más precisa el comportamiento y las interacciones de los contribuyentes.

##  Fases del proyecto

### Fase 1: Recolección de Datos

En la primera fase del proyecto, el objetivo fue reunir todas las fuentes de datos relevantes, incluyendo datos de contaminación, oscuridad y familiaridad que provienen de proyectos anteriores. Además, se utilizaron recursos de proyectos pasados como la clasificación y búsqueda de grupos económicos.

Un aspecto clave de esta fase fue la incorporación de datos provenientes de un proyecto desarrollado por Cristian Insunza, que calculó el crédito remanente contaminado para los contribuyentes en diferentes periodos. Esta información adicional fue fundamental para ampliar las fuentes de datos de contaminación por emisión de documentos sin autorización.

### Fase 2: Creación del Dashboard

La segunda fase se centró en la construcción de un dashboard interactivo que muestra toda la información recopilada en la fase anterior. Este dashboard facilita la visualización y análisis de los indicadores clave relacionados con cada contribuyente

## Tecnologías Utilizadas


### Apache Spark
- *Versión*: 3.2.3
- *Uso*: Consulta de tablas ubicadas en el Data Lake.
- *Configuración*:
  - Memoria de ejecución: 16 GB
  - Memoria del driver: 12 GB
  - Núcleos de ejecución: 2
  - Instancias de ejecución: 2

### Python
- *Versión*: 3.10
- *Uso*: Procesamiento adicional en Cloudera Machine Learning (CML).
- *Tipo de sesi*: Por otro lado y acerca de la ejecución del proyecto, se requiere en particular que las sesiones de trabajo tengan una memoria ram de 96 gb y 7 CPU como mínimo.


### Packages de Python mas utilizados
- *pandas*: Manejo de dataframes
- *numpy*: Operaciones matematicas
- *pyspark*: Para interactuar con Spark desde Python.
- *seaborn*: Visualización de datos.
- *matplotlib*: Generación de gráficos y visualizaciones.
- *warnings*: Gestión de mensajes de advertencia.
- *json*: Manipulación de datos en formato JSON.
- *os*: Interacción con el sistema operativo.
- *gradio*: herramienta de Python que permite crear interfaces de usuario web rápidas y fáciles para modelos de machine learning y funciones, facilitando su interacción sin necesidad de programación web avanzada.

## Recursos CML

En esta sección se detallan las herramientas y procesos utilizados para trabajar con datos y generar el dashboard:

### Enlaces de Datos:
Los enlaces de datos se establecen a través de notebooks y archivos ejecutables en Python (.py).
Estos enlaces conectan los datos almacenados en la nube con los proyectos relacionados. Sin embargo, debido a problemas con CML (Cloudera Machine Learning), parte de los datos se manejan de manera temporal en archivos locales.

### Almacenamiento de Datos:
Debido a la imposibilidad de guardar datos de manera estable en la nube a través de CML, los datasets temporales se gestionan principalmente utilizando archivos CSV.
Estos archivos CSV son exportados para construir el dashboard posterior y realizar el análisis.

### Recopilación y Unión de Datos:
Un notebook específico está dedicado a la recopilación de datos desde diversas fuentes, como proyectos previos y datasets temporales.
Otro notebook se encarga de procesar y unificar estos datos para generar los datasets finales que serán utilizados en el análisis.

### Generación del Dashboard:
En un notebook adicional, se utiliza Gradio para generar el dashboard que visualiza los datos recopilados y procesados.
El dashboard permite a los usuarios interactuar con los datos de manera dinámica y tomar decisiones informadas basadas en los indicadores presentados.

## Colaboradores
***
En este proyecto participa Henry Vega, Data Analyst de APIUX asi como el equipo del Área de Riesgo e Informalidad del SII, compuesto por Daniel Cantillana, Jorge Bravo y Yasser Nanjari, además Arnol Garcia, jefe de área y Manuel Barrientos, con soporte técnico en el espacio de trabajo y Jorge Estéfane, jefe de proyecto por parte de APIUX.


## Instalacion
***
Los pasos para instalar el proyecto y sus configuraciones en un entorno nuevo en caso que sea necesario, por ejemplo:
1. Instalar paquetes
  - Ingresar a la carpeta package_requeriment y abrir la terminal 
  - Escribir el siguiente comando: pip install -r requirements.txt
2. Ejecutar Notebooks (analisis de grupos economicos y/o busqueda de grupos economicos) y scripts en sus jobs respectivos.

3. Uso posterior de la data output obtenida. 


## Ejecucion del proyecto
************************************************

### 1. Fuentes de Datos y Recopilación

1. **`src/cantidad_trabajadores/Cantidad de Trabajadores.ipynb`**
   - **Descripción**: Este notebook se utiliza para analizar la cantidad de trabajadores de cada contribuyente.

2. **`src/materia_oscura_juridica_familiaridad/materia_oscura/materia_oscura_1.py`**
   - **Descripción**: Este script implementa un algoritmo de búsqueda de oscuridad que guarda en el Data Lake los datos de oscuridad. Toma personas jurídicas y naturales con un índice de oscuridad y propaga esta información a través de la malla societaria.

3. **`src/materia_oscura_juridica_familiaridad/depuracion_sociedades/depuracion_sociedades.ipynb`**
   - **Descripción**: Este notebook se encarga de tratar los datos de la malla societaria antes de utilizarlos en el algoritmo de oscuridad. Debe ser ejecutado antes de `materia_oscura_1.py`.

4. **`src/materia_oscura_juridica_familiaridad/familiaridad/familiares_sociedad_v2.ipynb`**
   - **Descripción**: Este notebook calcula el grado de familiaridad de las personas naturales que están detrás de una sociedad.

5. **`src/materia_oscura_juridica_familiaridad/familiaridad/familiares_sociedad_v2.py`**
   - **Descripción**: Este es el archivo ejecutable correspondiente al notebook anterior, realizando la misma función de cálculo de familiaridad.

6. **`src/contaminacion/agregar_labels_contaminados.ipynb`**
   - **Descripción**: Este notebook se ejecuta para agregar características a los datos de contaminados, ayudando en la identificación de contribuyentes con problemas de contaminación.

7. **`src/contaminacion/data_contaminados.ipynb`**
   - **Descripción**: Este notebook obtiene y analiza los datos de contribuyentes contaminados.

8. **`src/contaminacion/propagacion_contaminacion_iva_representante.py`**
   - **Descripción**: Este script permite la propagación de la contaminación a lo largo de la red comercial, ayudando a rastrear y entender las conexiones contaminadas.

9. **`src/credito_remanente/IVA_Credito_Remanente_v5.ipynb`**
   - **Descripción**: Este notebook toma el código de Cristian Insunza y lo mejora, agregando fuentes de contaminación para la propagación de IVA contaminado, guardando el IVA contaminado histórico por períodos.

10. **`src/credito_remanente/IVA_Credito_Remanente_v5.py`**
    - **Descripción**: Este es el archivo ejecutable correspondiente al notebook anterior, realizando la misma función de cálculo y mejora de IVA contaminado.

11. **`src/credito_remanente/Lectura de datos.ipynb`**
    - **Descripción**: Este notebook ayuda en la lectura de datos de la base histórica generada a través del cálculo del IVA contaminado.

### 2. Creación de Dashboard

12. **`src/data_final/union_data.ipynb`**
    - **Descripción**: Este notebook se encarga de unir todas las fuentes de datos para poder guardarlas en una base de datos.

13. **`notebooks/Dashboard_test.ipynb`**
    - **Descripción**: Este notebook despliega la información de la base de datos unida, permitiendo hacer consultas por ID.

### 3. Detalles sobre las Fuentes de Datos

- En el algoritmo de familiaridad y oscuridad, existe la idea de guardar la nube. Sin embargo, al final, cuando se hace la lectura de datos, se hace desde una lectura anterior debido a problemas para guardar la data en el Data Lake.
  
- Los proyectos pueden re-ejecutarse para reconectar los datos y utilizar la información más fidedigna y actualizada posible, siempre que se utilicen las tablas correspondientes para cada proyecto. Esta es una simplificación de proyectos anteriores para facilitar el uso de la fuente de datos de manera más ágil.

### 4. Variables del Dataset Final


### Fuentes de Datos y Estructura del Conjunto de Datos

En este apartado se presenta un resumen de las fuentes de datos utilizadas en los proyectos anteriores, así como una descripción de las tablas y variables del conjunto de datos final que alimenta el dashboard.

#### Resumen de Proyectos Anteriores

En el algoritmo de familiaridad y oscuridad, se contempla la idea de guardar la nube. Sin embargo, debido a problemas para almacenar los datos en el Data Lake, al final, la lectura de datos se realiza desde un archivo anterior. Esto permite que los proyectos puedan re-ejecutarse y reconectar los datos, utilizando las tablas correspondientes para cada proyecto y garantizando así el uso de la información más fidedigna y actualizada posible. 

Este informe simplifica la información de los proyectos anteriores para facilitar el uso de las fuentes de datos de manera más ágil.

#### Estructura del Conjunto de Datos Final

El dashboard está diseñado para mostrar, en una primera sección, toda la información del contribuyente en particular. Posteriormente, incluye una sección que presenta el flujo de IVA para el grupo al que pertenece el contribuyente, diferenciando entre recepción y emisión intra y extra-grupo. Además, hay una sección que ofrece una visión general del grupo, mostrando la información de emisión y recepción de IVA.

A continuación, se detalla la lista de variables que componen las tablas del conjunto de datos final:

##### Tabla: contribuyente_iva
- **contribuyente**: Identificación del contribuyente.
- **total_iva_intragrupo_emisor**: Total de IVA emitido por contribuyentes dentro del mismo grupo.
- **total_iva_extragrupo_emisor**: Total de IVA emitido por contribuyentes fuera del grupo.
- **total_iva_intragrupo_receptor**: Total de IVA recibido por contribuyentes dentro del mismo grupo.
- **total_iva_extragrupo_receptor**: Total de IVA recibido por contribuyentes fuera del grupo.
- **arcos_intragrupo_emisor**: Cantidad de conexiones comerciales desde las cuales se emitió IVA intragrupo.
- **arcos_extragrupo_emisor**: Cantidad de conexiones comerciales desde las cuales se emitió IVA extragrupo.
- **arcos_intragrupo_receptor**: Cantidad de conexiones comerciales desde las cuales se recibió IVA intragrupo.
- **arcos_extragrupo_receptor**: Cantidad de conexiones comerciales desde las cuales se recibió IVA extragrupo.

##### Tabla: iva
- **com**: Identificador o código de la comunidad.
- **perct_emision_extra**: Porcentaje de emisión fuera del grupo.
- **perct_recepcion_extra**: Porcentaje de recepción fuera del grupo.
- **emision_extragrupo**: Total de emisión fuera del grupo.
- **recepcion_extragrupo**: Total de recepción fuera del grupo.
- **emision_intragrupo**: Total de emisión dentro del grupo.
- **tasa_emision_extra_intra**: Tasa de emisión extra dentro del grupo.
- **arcos_emision_intra**: Cantidad de conexiones comerciales con emisión dentro del grupo.
- **arcos_emision_extra**: Cantidad de conexiones comerciales con emisión fuera del grupo.
- **arcos_reception_intra**: Cantidad de conexiones comerciales con recepción dentro del grupo.
- **arcos_reception_extra**: Cantidad de conexiones comerciales con recepción fuera del grupo.
- **grupo_louvain**: Grupo asignado según el algoritmo de Louvain basado en relaciones familiares, societarias y contables.
- **tamanio_comunidad**: Tamaño del grupo en el que se encuentra el contribuyente.

##### Tabla: contribuyentes
- **RUT_UNICO**: Identificación única del contribuyente.
- **SOCIEDAD**: Indica si el contribuyente es una sociedad (1) o no (0).
- **CONTADOR**: Indica si el contribuyente tiene contador (1) o no (0).
- **NATURAL**: Indica si el contribuyente es una persona natural (1) o no (0).
- **REPRESENTANTE**: Indica si hay un representante legal (1) o no (0).
- **Contaminacion**: Relación del contribuyente con conexiones contaminadas, donde la contaminación proviene de personas que tienen alerta.
- **Total_pago_f29**: Total de pago correspondiente a la declaración F29.
- **IVA_neto**: Total de IVA neto a pagar.
- **Unidad_regional**: Unidad geográfica del contribuyente.
- **Numero_documentos**: Número total de documentos emitidos.
- **Tiempo_de_vida_dias**: Días de actividad del contribuyente.
- **Alerta_inicial**: Indicador de alerta inicial asociado al contribuyente.
- **Oscuridad**: Medida de la "oscuridad" de la información disponible sobre el contribuyente.
- **Familiares**: Número de familiares en la sociedad.
- **Personas_naturales_socios**: Total de personas naturales que son socios.
- **Tasa_familiaridad**: Grado de familiaridad entre los socios, donde 100% indica conexión familiar en el grupo.
- **Grupo_economico_inicial**: Grupo económico inicial al que pertenece el contribuyente.
- **Grupo_louvain**: Grupo asignado según el algoritmo de Louvain basado en relaciones.
- **Trabajadores_honorarios**: Número de trabajadores contratados bajo honorarios.
- **Trabajadores_dependientes**: Número de trabajadores dependientes.
- **Patrimonio_2022**: Patrimonio declarado en el año 2022.
- **ultimo_periodo_remanente_contaminado**: Último periodo en el cual se encontró el remanente contaminado para ese contribuyente.
- **remanente_cont**: Monto del remanente contaminado en el último periodo.

## Sugerencias

Se sugiere en un futuro cercano reejecutar todos los ejecutables del proyecto con data actualizada. De este modo el dashboard será alimentado con data mas actualizada. Asimismo, al terminar este proyecto está en solicitud las tablas necesarias para obtener el remanente a partir de una ventana de tiempo de los ultimos 3 años. 

## Plantilla del proyecto
***
Se especifica a continuacion la estructura del proyecto.

Proyecto/
│			
├── notebooks/          				# Jupyter notebooks para exploración de datos, prototipado de modelos, etc.

│   ├── [noteebok1.ipynb]			

│   ├── [notebook2.ipynb]		

│   ├── [notebook3.ipynb]		

│			
├── src/                				# Código fuente Python

│   ├── data/           				# Módulos para cargar, limpiar y procesar datos.

│   ├── models/         				# Definiciones de modelos

│   ├── evaluation/     				# Scripts para evaluar el rendimiento de los modelos.

│   └── utils/          				# Utilidades y funciones auxiliares.

│			

├── data/        				        # Bases de dato de todo tipo que se utilizan o generan en el proyecto.

│   ├── external/     				    # Data externa

│   ├── processed/          			# Data procesada

│   └── raw/                            # Data cruda

│					

├── requirements.txt    				# Archivo de requisitos para reproducir el entorno de Python.

│			

└── readme.md           				# Descripción general del proyecto y su estructura.

** Archivos entre [ ] son ejemplos solamente y no se entregan por ahora.
