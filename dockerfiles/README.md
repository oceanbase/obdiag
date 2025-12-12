# obdiag Dockerfiles

This directory contains all Dockerfile files for OceanBase Diagnostic Tool (obdiag).

## File Overview

| File | Purpose | Base Image |
|------|---------|------------|
| `Dockerfile.production` | Production image build | AnolisOS 8.9 |
| `Dockerfile.release` | Official release image | AnolisOS 8.9 |
| `Dockerfile.builder` | CI/CD builder image | AnolisOS 8.9 |
| `Dockerfile.dev` | Development environment | CentOS 7.9 |
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

### 4. Dockerfile.dev

**Purpose**: Local development environment with complete toolchain

**Features**:
- Based on CentOS 7.9 (good compatibility)
- Pre-installed Python 3.11 and all dependencies
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

### 5. Dockerfile.local

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
| Dockerfile.dev | ✅ | ❌ |
| Dockerfile.local | ✅ | ✅ |

---

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `OBDIAG_HOME` | obdiag configuration directory | `~/.obdiag` |
| `PATH` | Includes obdiag executable path | Auto-configured |

---

## Related Files

- `dev_helper.sh` - Development helper script
- `Makefile` - Build and development commands
- `rpm/oceanbase-diagnostic-tool.spec` - RPM packaging specification
- `.github/workflows/` - CI/CD workflow configurations
