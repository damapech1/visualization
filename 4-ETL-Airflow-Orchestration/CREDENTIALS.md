# Login Credentials - Airflow Lab

##  Airflow Web UI

**URL**: http://localhost:8080

**Username**: `airflow`  
**Password**: `airflow`

##  Database (PostgreSQL)

If you need direct database access:

**Host**: `localhost`  
**Port**: `5432`  
**Database**: `airflow`  
**Username**: `airflow`  
**Password**: `airflow`

```bash
# Connect using psql
docker-compose exec postgres psql -U airflow

# Or from your machine (if you have psql installed)
psql -h localhost -p 5432 -U airflow -d airflow
```

##  Container Access

**Access Airflow container**:
```bash
docker-compose exec webserver bash
```

**Access scheduler container**:
```bash
docker-compose exec scheduler bash
```

**Access database container**:
```bash
docker-compose exec postgres bash
```

##  Security Note

These are **development credentials** for learning purposes only.

**Never use these in production!**

In production, you should:
- Use strong, unique passwords
- Enable LDAP/OAuth/SSO authentication
- Use environment variables or secrets management (Vault, AWS Secrets Manager)
- Enable RBAC (Role-Based Access Control)
- Use HTTPS/TLS everywhere
- Regularly rotate credentials

---

**Need help?** See [README.md](README.md) or [QUICKSTART.md](QUICKSTART.md)

