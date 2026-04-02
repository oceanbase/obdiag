# obdiag Dockerfiles

This directory contains all Dockerfile files for OceanBase Diagnostic Tool (obdiag).

## File Overview

| File | Purpose | Base Image |
|------|---------|------------|
| `Dockerfile.production` | Production image build | AnolisOS 8.9 |
| `Dockerfile.release` | Official release image | AnolisOS 8.9 |
| `Dockerfile.builder` | CI/CD builder image (Anolis / el8-style) | AnolisOS 8.9 |
| `Dockerfile.builder-centos7` | CI RPM build on CentOS 7 (GitHub Actions) | `centos:centos7.9.2009` |
| `Dockerfile.dev` | Development environment | CentOS 7.9 (Huawei SWR mirror) |
| `Dockerfile.local` | Local code testing | AnolisOS 8.9 |

---

## Detailed Description

### 1. Dockerfile.production

**Purpose**: Build obdiag from source code and generate a production-ready Docker image

**Features**:
- Multi-stage build
- Stage 1: Build RPM package
- Stage 2: Install RPM in minimal container
- Supports x86_64 and aarch64 architectures
- Minimal final image size

**Use Cases**:
- Build production image from current codebase
- Test complete build process locally

**Build Command**:
```bash
docker build -f dockerfiles/Dockerfile.production -t obdiag:local .
```

**Run Command**:
```bash
docker run -it --rm obdiag:local obdiag --help
```

---

### 2. Dockerfile.release

**Purpose**: Install obdiag from official YUM repository for releasing official Docker images

**Features**:
- Direct installation from OceanBase official repository
- Minimal image size
- Used for CI/CD automated releases

**Use Cases**:
- GitHub Actions automated image build and release
- Generate DockerHub official images

**Build Command**:
```bash
docker build -f dockerfiles/Dockerfile.release -t obdiag:release .
```

**Associated Workflow**: `.github/workflows/build_obdiag_docker.yml`

---

### 3. Dockerfile.builder

**Purpose**: Build base environment image with all compilation dependencies

**Features**:
- Pre-installed Python 3.11 (Miniconda)
- Pre-installed rpm-build, gcc, and other build tools
- Serves as base image for CI/CD

**Use Cases**:
- CI/CD build acceleration
- Development base image

**Build Command**:
```bash
docker build -f dockerfiles/Dockerfile.builder -t obdiag-builder:latest .
```

**Associated Workflow**: `.github/workflows/build_base_docker.yml`

---

### 4. Dockerfile.builder-centos7

**Purpose**: Build an RPM on **CentOS 7** inside Docker (used when GitHub-hosted runners no longer provide CentOS 7)

**Features**:
- Default **`vault.centos.org`** (HTTPS): right for GitHub-hosted runners (overseas). Stock `.repo` files contain commented `#baseurl=http://mirror.centos.org/...` lines only; the Dockerfile replaces that pattern — **yum does not use `mirror.centos.org` at runtime** (EOL / unavailable).
- Tunes `yum` for CI (`ip_resolve=4`, `minrate=0`, `retries=10`, fastestmirror off)
- Miniconda + Python 3.11, then `make pack` during image build (RPM lands in image root as `oceanbase-diagnostic-tool-*.rpm`)

**Use Cases**:
- GitHub Actions: job `build-rpm-centos7` in `.github/workflows/build_package.yml`
- Local verification of el7-compatible RPM packaging

**Build Command** (default vault, suitable for GitHub-hosted runners):
```bash
docker build -f dockerfiles/Dockerfile.builder-centos7 -t obdiag-builder-centos7 .
```

**Build Command** (mainland China / slow vault; optional):
```bash
docker build -f dockerfiles/Dockerfile.builder-centos7 -t obdiag-builder-centos7 \
  --build-arg CENTOS_BASEURL_PREFIX=https://mirrors.aliyun.com/centos-vault .
```

**Copy RPM to host** (after `docker build`):
```bash
mkdir -p centos7-rpm-out
docker run --rm \
  -v "$(pwd)/centos7-rpm-out:/out" \
  --entrypoint /bin/bash obdiag-builder-centos7 \
  -c 'shopt -s nullglob; cp -av /oceanbase-diagnostic-tool-*.rpm /out/'
```

**Associated Workflow**: `.github/workflows/build_package.yml` (artifact `obdiag-rpm-packages-centos7`)

---

### 5. Dockerfile.dev

**Purpose**: Local development environment with complete toolchain

**Features**:
- Based on CentOS 7.9 (Huawei Cloud SWR mirror of `centos:centos7.9.2009`)
- Yum configured via Aliyun CentOS 7 repo template
- Pre-installed Python 3.11 (Miniconda under `/opt/miniconda`) and editable install of obdiag deps
- Mount local code for development

**Use Cases**:
- Local development and debugging
- IDE remote development container
- Unified development environment

**Build Command**:
```bash
docker build -f dockerfiles/Dockerfile.dev -t obdiag-dev:latest .
```

**Run Command** (mount local code):
```bash
docker run -it --rm \
  -v $(pwd):/workspaces/obdiag \
  obdiag-dev:latest \
  /bin/bash
```

**Usage Inside Container**:
```bash
source /opt/miniconda/bin/activate obdiag
cd /workspaces/obdiag
python3 src/main.py --help
```

---

### 6. Dockerfile.local

**Purpose**: Build complete image using local code for testing local modifications

**Features**:
- Multi-stage build
- Copies local code into image
- Builds and installs RPM
- Multi-architecture support

**Use Cases**:
- Test local code modifications
- Verify RPM packaging correctness
- Local integration testing

**Build Command**:
```bash
docker build -f dockerfiles/Dockerfile.local -t obdiag:test .
```

**Run Command**:
```bash
docker run -it --rm obdiag:test obdiag --help
```

---

## Quick Reference

### Development Workflow

```bash
# 1. Build development environment
docker build -f dockerfiles/Dockerfile.dev -t obdiag-dev .

# 2. Enter development container
docker run -it --rm -v $(pwd):/workspaces/obdiag obdiag-dev /bin/bash

# 3. Develop inside container
source /opt/miniconda/bin/activate obdiag
cd /workspaces/obdiag
python3 src/main.py check
```

### CentOS 7 RPM (CI-style)

```bash
docker build -f dockerfiles/Dockerfile.builder-centos7 -t obdiag-builder-centos7 .
mkdir -p centos7-rpm-out
docker run --rm -v "$(pwd)/centos7-rpm-out:/out" --entrypoint /bin/bash obdiag-builder-centos7 \
  -c 'shopt -s nullglob; cp -av /oceanbase-diagnostic-tool-*.rpm /out/'
```

### Test Local Modifications

```bash
# Build test image
docker build -f dockerfiles/Dockerfile.local -t obdiag:test .

# Run test
docker run -it --rm obdiag:test obdiag version
```

### Build Production Image

```bash
# Build production image
docker build -f dockerfiles/Dockerfile.production -t obdiag:prod .

# Verify
docker run -it --rm obdiag:prod obdiag --help
```

---

## Architecture Support

| Dockerfile | x86_64 | aarch64 |
|------------|--------|---------|
| Dockerfile.production | ✅ | ✅ |
| Dockerfile.release | ✅ | ✅ |
| Dockerfile.builder | ✅ | ❌ |
| Dockerfile.builder-centos7 | ✅ | ❌ |
| Dockerfile.dev | ✅ | ❌ |
| Dockerfile.local | ✅ | ✅ |

---

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `OBDIAG_HOME` | obdiag configuration directory | `~/.obdiag` |
| `PATH` | Includes obdiag executable path | Auto-configured |

### Docker build arguments (`Dockerfile.builder-centos7`)

| Build arg | Description | Default |
|-----------|-------------|---------|
| `CENTOS_BASEURL_PREFIX` | yum `baseurl` prefix for CentOS vault tree (`…/$releasever/os/...`) | `https://vault.centos.org/centos` |

---

## Related Files

- `dev_helper.sh` - Development helper script
- `Makefile` - Build and development commands (`make pack`, etc.)
- `rpm/oceanbase-diagnostic-tool.spec` - RPM packaging specification
- `.github/workflows/build_package.yml` - Package builds (includes CentOS 7 Docker RPM + artifact upload)
- `.github/workflows/build_base_docker.yml` - Publishes builder image from `Dockerfile.builder`
- `.github/workflows/build_obdiag_docker.yml` - Release image from `Dockerfile.release`
