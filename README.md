# nqr1g
Cliente de consulta contratación publica

## Deployment in local
```bash
docker build -t socrata-nqr1g-api
docker run -d -p 5000:5000 --name contenedor-nqr1g socrata-nq1g-api
```

Una vez desplegado el container, se puede visitar localmente en http://localhost:5000
