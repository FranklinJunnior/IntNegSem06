# Usa la imagen oficial de SQL Server
FROM mcr.microsoft.com/mssql/server:2022-latest

# Establece variables de entorno para la configuración de SQL Server
ENV ACCEPT_EULA=Y
ENV SA_PASSWORD=PassFranklinTec!

# Exponer el puerto 1433 para la conexión
EXPOSE 1433

# Comando para iniciar SQL Server
CMD /opt/mssql/bin/sqlservr