# Globant Data Engineering Challenge 🚀

Este proyecto resuelve el coding challenge propuesto por Globant para el rol de Data Engineer. Incluye un pipeline ETL desarrollado con PySpark, una API REST con FastAPI y pruebas automatizadas.

---

## Estructura del Proyecto

```
.
├── api/                     # Endpoints y servicios de la API
├── config/                  # Configuración de la DB (prod y test)
├── data/uploads/            # Carpeta donde se suben los CSVs a cargar
├── etl_outputs/             # Resultados del ETL (JSON)
├── logs/                    # Logs rotativos de cada ejecución ETL
├── queries_sql/             # Queries SQL centralizadas
├── spark_jobs/              # Script PySpark de validación y carga
├── tests/                   # Tests automáticos con pytest
├── Dockerfile               # Imagen Docker de FastAPI
├── docker-compose.yml       # Contenedores: API + MySQL + Test DB
├── main_api.py              # Punto de entrada de la API
├── requirements.txt         # Dependencias del proyecto
```

---

## Setup local

### 1. Clonar repositorio

```bash
git clone <REPO_URL>
cd globant
```

### 2. Crear entorno virtual

```bash
python -m venv venv
venv\Scripts\activate  # Windows
source venv/bin/activate  # Linux/Mac
```

### 3. Instalar dependencias

```bash
pip install -r requirements.txt
```

---

## Docker

### Levantar entorno completo (API + MySQL + TestDB)

```bash
docker-compose up -d
```

---

## API Endpoints (FastAPI)

Una vez levantada la API en `http://localhost:8000`, accede a la documentación en:

Swagger UI: [`http://localhost:8000/docs`](http://localhost:8000/docs)

### Endpoints principales:

- `POST /upload_csv/{table_name}` → Subida de CSVs
- `POST /run_etl` → Ejecuta el proceso ETL completo
- `GET /metrics/hires_per_quarter` → Contrataciones por trimestre
- `GET /metrics/departments_above_avg` → Departamentos con más contrataciones

---

## Proceso ETL (PySpark)

El proceso ETL hace lo siguiente:

1. Lee los archivos CSV cargados en `data/uploads/`
2. Valida duplicados, claves foráneas, campos nulos, etc.
3. Inserta los nuevos registros en MySQL
4. Guarda un `.json` con el resumen del batch
5. Genera un `.log` con el detalle de la ejecución

Se puede ejecutar manualmente:

```bash
spark-submit --jars mysql-connector-java-8.0.33.jar spark_jobs/validate_and_load.py
```

---

## Pruebas

Este proyecto incluye pruebas automatizadas:

```bash
set IS_TEST=1 && pytest
```

Cubre:

- Subida de archivos
- Métricas
- ETL completo (mock + real)
- Casos de error

---

##  Cloud-ready

Este proyecto está listo para deploy en cualquier plataforma cloud:

- **Base de datos:** Contenerizada
- **ETL:** Compatible con Spark local o cluster
- **API:** Dockerizada y portable
- **Logging:** Local + archivo
- **Pruebas:** Independientes, con base de datos de test

---

##  Bonus

- Batch ID único por ejecución
- Rollback completo en caso de errores
- Logging detallado (`logs/`)
- Validaciones robustas por tabla
- Modularización profesional de código

---

## 💡 Autor

Desarrollado por Jonathan Villegas  — Abril 2025\
Desafío técnico para Globant.

---

