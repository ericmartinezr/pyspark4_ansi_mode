# PySpark ANSI mode on

Pequeño proyecto para probar el modo ANSI habilitado en PySpark.

## Excepciones

El archivo [main_exceptions.py](main_exceptions.py) contiene la variante con try-except.
Esto permite capturar el error que lanza Spark cuando encuentra alguna operación inválida.

## Variantes try\_\*

El archivo [main_try.py](main_try.py) contiene la variante con try\_\* (try_cast, try_divide, etc) que en vez de lanzar una excepción retorna `NULL`

### Ejecución

```bash
# Creacion de ambiente virtual
python -m venv .venv
source .venv/bin/activate

# Instalar dependencias
pip install -r requirements.txt

# Ejecucion
python main_exceptions.py
python main_try.py
```

### Nota

El modo ANSI en esta versión está habilitado por defecto, pero igualmente se agregó como configuración para que sea visualmente más claro el intento de este proyecto.

# Referencia

- https://spark.apache.org/docs/latest/sql-ref-ansi-compliance.html
- https://www.youtube.com/watch?v=wSWdmS78ENE
