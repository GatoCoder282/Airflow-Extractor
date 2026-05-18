# Spec Compliance Matrix - Modulo Extractor

## Estado General

- Estado: Implementado con brechas menores
- Referencia: Especificacion Tecnica Modulo Extractor ETL Observability (Marzo 2026)

## Matriz de Cumplimiento

| Requisito | Estado | Evidencia |
|---|---|---|
| Arquitectura hexagonal (domain/ports/adapters/services/infrastructure) | Cumple | extraction/domain, extraction/ports, extraction/adapters, extraction/services, extraction/infrastructure |
| Flujo 1 de inicializacion al arranque | Cumple | extraction/services/initialization_service.py, extraction/extractor/main.py |
| Flujo 2 de polling continuo cada 10s | Cumple | extraction/services/extraction_service.py, extraction/extractor/config.py |
| No duplicar eventos cuando no hay cambio de estado | Cumple | extraction/services/state_tracker.py |
| Persistir ultimo estado para sobrevivir reinicios | Cumple | extraction/ports/state_repository.py, extraction/adapters/sql_state_repository.py |
| Health check y evento scheduler unhealthy | Cumple | extraction/adapters/http_airflow_client.py, extraction/services/extraction_service.py |
| Import errors periodicos | Cumple | extraction/adapters/http_airflow_client.py, extraction/services/extraction_service.py |
| Publicacion en Redis Streams por tipo de evento | Cumple | extraction/adapters/redis_publisher.py |
| Contratos de puertos completos (Airflow, Publisher, State, Catalog) | Cumple | extraction/ports/airflow_client.py, extraction/ports/event_publisher.py, extraction/ports/state_repository.py, extraction/ports/catalog_repository.py |
| Catalogo DAG con upsert batch + refresh materialized view | Cumple | extraction/adapters/postgres_catalog_repository.py |
| Factory como unico wiring de concreciones | Cumple | extraction/infrastructure/factory.py |
| Configuracion via entorno (.env) | Cumple | extraction/extractor/config.py |
| Graceful shutdown SIGINT/SIGTERM | Cumple | extraction/extractor/main.py |
| Dependencias declaradas en requirements | Cumple | requirements.txt |
| Pruebas unitarias de tracker y service | Cumple | tests/unit/test_state_tracker.py, tests/unit/test_extraction_service.py |
| Pruebas integracion cliente Airflow (paginacion + batch) | Cumple | tests/integration/test_airflow_client.py |

## Brechas Menores Detectadas

- En extraction/services/extraction_service.py el evento scheduler usa region fija "global" en lugar de la region configurada; no rompe el flujo pero puede ajustarse por consistencia de tagging.
- En extraction/adapters/http_airflow_client.py el body de taskInstances/list usa clave "state"; la especificacion menciona "state" en ejemplo de ese endpoint, por lo que se considera compatible, pero conviene validar contra el Airflow real en ambiente.
- Falta documentacion operativa en README.md (arranque local, variables de entorno, modo dev/prod).

## Resultado de Validacion Ejecutada

- Unit tests: 3 passed
- Integration tests del cliente Airflow: agregados
