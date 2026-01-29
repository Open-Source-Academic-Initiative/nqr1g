# nqr1g
Cliente de consulta contratación publica

## Deployment in local
```bash
docker build -t socrata-nqr1g-api .
docker run -d -p 5000:5000 --name contenedor-nqr1g socrata-nq1g-api
```
## Run app using podman

In the folder of the project:
```
podman build -t nqr1g .
podman run -d -p 5000:5000 --replace --name nqr1g nqr1g
```

Una vez desplegado el container, se puede visitar localmente en http://localhost:5000

## Additional technical notes

The first step is to install podman in your system:

CentOS/RHEL like systems:
```
sudo dnf install podman
```
To build the container named **nqr1g** using the local Dockerfile in the current directory:
```
podman build -t nqr1g .
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
### Your are using apache and you like to publish the service:
Can be created a vhost pointing to the port of the container running. A config file in /etc/httpd/conf.d/nqr1g.conf
```
<VirtualHost *:80>
    ServerName nqr1g.mydomain.com

    ProxyPreserveHost On
    ProxyPass        /  http://127.0.0.1:5000/
    ProxyPassReverse /  http://127.0.0.1:5000/

    ErrorLog /var/log/httpd/nqr1g.log
    CustomLog /var/log/httpd/nqr1g-access.log combined
</VirtualHost>
```
Restart apache:
```
systemctl restart httpd
```
In your DNS service, must be created an A register with the value nqr1g.mydomain.com
