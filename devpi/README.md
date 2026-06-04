## Configuration

- devpi-server
  - Core product
  - Runs in the foreground
- devpi-web
  - Provides search capabilities to pip
  - Provides search capabilities through web interface
- devpi-lockdown
  - Locks down read access to indexes
  - Unsure whether devpi-web is required
  - Dependent on nginx
- nginx
  - Provides tls termination
  - Required for devpi-lockdown
  - Runs in the background

### General

```
DEVPI_SERVER_ROLE=standalone
DEVPI_ROOT=/opt/devpi
DEVPI_SERVER_ROOT=/opt/devpi/server
DEVPI_SERVER_PORT=3141
DEVPI_ROOT_PASSWD=
DEVPI_BASE_ENDPOINT=http://localhost:3141
```

### Add client

```
DEVPI_CLIENT_<client-id>_USERNAME=guest
DEVPI_CLIENT_<client-id>_PASSWORD=password
```

### Add index

```
DEVPI_INDEX_<index-id>_NAME=stable
DEVPI_INDEX_<index-id>_OWNER=root
DEVPI_INDEX_<index-id>_BASES=root/pypi
DEVPI_INDEX_<index-id>_ACL_UPLOAD=cicd
DEVPI_INDEX_<index-id>_VOLATILE=False
```

### Create token

```bash
devpi-client token-create --user root --allowed pkg_read
```

Notes: because you can't assign an id to a token, we have no way of knowing if one was already created.
https://github.com/devpi/devpi-tokens

```
DEVPI_TOKEN_<index-id>_NAME=cicd
DEVPI_TOKEN_<index-id>_OWNER=root
DEVPI_TOKEN_<index-id>_PERMISSIONS=pkg_read
DEVPI_TOKEN_<index-id>_EXPIRES=...
DEVPI_TOKEN_<index-id>_INDEXES=root/pypi,root/alternate
DEVPI_TOKEN_<index-id>_PROJECTS=...
```

Token permissions
- del_entry
- del_project
- del_verdata
- index_create
- index_delete
- index_modify
- pkg_read
- toxresult_upload
- upload

Token expires

expiration as epoch timestamp or delta with units: y(ear(s)), m(onth(s)), w(eek(s)), d(ay(s)), h(our(s)), min(ute(s)) and s(econd(s))

Token indexes

comma separated list of indexes to limit the token to.

Token projects

comma separated list of projects to limit the token to.

## Deploy

1. Add devpi.local.lab to hosts

```bash
sudo vi /etc/hosts
```

```/etc/hosts
127.0.0.1       localhost ... devpi.local.lab
```

2. Start nginx proxy

```bash
./dev-server.sh --profile devpi-server
```

3. Start devpi server

```bash
./dev-server.sh --profile devpi-proxy
```

4. Open devpi ui

```
https://devpi.local.lab
```

## Pip

### Pip Install (no credentials)

```bash
pip install --index https://devpi.local.lab/root/stable/+simple/ \
  --trusted-host devpi.local.lab \
  --force-reinstall \
  --no-cache-dir \
  requests
```

### Pip Install (Username + password)

```bash
pip install --index https://root:password@devpi.local.lab/root/stable/+simple/ \
  --trusted-host devpi.local.lab \
  --force-reinstall \
  --no-cache-dir \
  requests
```

### Pip Install (token)

https://github.com/devpi/devpi-tokens

1. Create token

```bash
devpi-client token-create --user root --allowed pkg_read
```

```bash
TOKEN=...
pip install --index https://${TOKEN}@devpi.local.lab/root/stable/+simple/ --trusted-host devpi.local.lab requests
```

### Pip Search

```bash
pip search --index https://devpi.local.lab/root/pypi/ --trusted-host devpi.local.lab requests
```

## Poetry

### Add source

```bash
poetry source add --priority=primary internal https://devpi.local.lab/root/pypi/+simple/
```
