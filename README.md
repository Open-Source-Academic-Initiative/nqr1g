# nqr1g
Cliente de consulta contratación publica

## Deployment in local
```bash
docker build -t socrata-nqr1g-api
docker run -d -p 5000:5000 --name contenedor-nqr1g socrata-nq1g-api
```
## Deployment using podman
The first step is to install podman in your system (to complete).

In the folder of the project:
```
podman build -t nqr1g .
podman run -d -p 5000:5000 --replace --name nqr1g nqr1g
```
To stop the container:
```
podman stop nqr1g
```

To delete the container:
```
podman rm nqr1g
```
To check if is running: 
```
podman ps
```




Una vez desplegado el container, se puede visitar localmente en http://localhost:5000
