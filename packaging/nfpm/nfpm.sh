#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only

set -euo pipefail

# Check for mandatory env vars
if [[ -z "${VERSION}" ]]; then
    echo "VERSION is not set"
    exit 1
fi

# Prepare build environment
rm -rf dist/tmp && mkdir -p dist/tmp/packages
cp dist/*-linux-* dist/tmp/packages

for name in metaconvert mimir mimirtool query-tee ; do
    for arch in amd64 arm64; do
        for packager in deb rpm; do
            config_path="dist/tmp/config-${name}-${packager}-${arch}.json"
            pkg_dependencies_path="dist/tmp/dependencies-${name}-${packager}-${arch}"
            os_env_dir='/etc/default'
            if [ "$packager" == 'rpm' ]; then
                os_env_dir='/etc/sysconfig'
            fi

            # Generate NFPM configuration using jsonnet
            docker run --rm \
              -v "$(pwd)/packaging/nfpm/nfpm.jsonnet:/nfpm/nfpm.jsonnet" \
              -it 'bitnamilegacy/jsonnet:0.21.0-debian-12-r6' \
              -V "name=${name}" -V "arch=${arch}" -V "packager=${packager}" "/nfpm/nfpm.jsonnet" > "${config_path}"

            # Generate package dependencies using envsubst
            mkdir "${pkg_dependencies_path}"

            if [ -d packaging/nfpm/"${name}" ]; then
              for dependency_path in packaging/nfpm/"${name}"/*; do
                  dependency=$(basename "${dependency_path}")
                  docker run --rm \
                    -v "$(pwd)/packaging/nfpm/${name}:/work" \
                    -v "$(pwd)/${pkg_dependencies_path}:/processed" \
                    -e "OS_ENV_DIR=${os_env_dir}" \
                    -it 'bhgedigital/envsubst' \
                    sh -c "envsubst '\${OS_ENV_DIR}' < /work/${dependency} > /processed/${dependency}"
              done
            fi

            docker run --rm \
              -v  "$(pwd):/work:delegated,z" \
              -w /work \
              -e "VERSION=${VERSION}" \
              -it goreleaser/nfpm:v2.22.2 \
              package \
              --config ${config_path} \
              --packager ${packager} \
              --target /work/dist/

            # Rename mimir packages as we want to keep the same standard as the one built by FPM
            if [ "${name}" == 'mimir' ] && [ "${packager}" == 'deb' ] && [ "${arch}" == 'amd64' ]; then
              mv -f "dist/mimir_${VERSION}_amd64.deb" "dist/mimir-${VERSION}_amd64.deb"
            fi
            if [ "${name}" == 'mimir' ] && [ "${packager}" == 'deb' ] && [ "${arch}" == 'arm64' ]; then
              mv -f "dist/mimir_${VERSION}_arm64.deb" "dist/mimir-${VERSION}_arm64.deb"
            fi
            if [ "${name}" == 'mimir' ] && [ "${packager}" == 'rpm' ] && [ "${arch}" == 'amd64' ]; then
              mv -f "dist/mimir-${VERSION}.x86_64.rpm" "dist/mimir-${VERSION}_amd64.rpm"
            fi
            if [ "${name}" == 'mimir' ] && [ "${packager}" == 'rpm' ] && [ "${arch}" == 'arm64' ]; then
              mv -f "dist/mimir-${VERSION}.aarch64.rpm" "dist/mimir-${VERSION}_arm64.rpm"
            fi
        done
    done
done

# Compute checksum of builded packages
for pkg in dist/*.deb dist/*.rpm; do
	sha256sum "${pkg}" | cut -d ' ' -f 1 > "${pkg}-sha-256";
done

# Cleanup build environment
rm -rf dist/tmp
