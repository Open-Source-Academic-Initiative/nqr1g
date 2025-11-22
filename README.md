# nqr1g
cliente de consulta contratación publica

## Deployment in local
```bash
docker build -t socrata-nqr1g-api
docker run -d -p 5000:5000 --name contenedor-nqr1g socrata-nq1g-api
```

Se puede visitar en http://localhost.5000
