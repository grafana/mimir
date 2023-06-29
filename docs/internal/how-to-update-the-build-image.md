The build image currently can only be updated by a Grafana Mimir maintainer. If you're not a maintainer you can still open a PR with the changes, asking a maintainer to assist you in publishing the updated image.

The automatic procedure is as follows:

1. Update `mimir-build-image/Dockerfile` and create a pull request.
2. When the PR is changed to ready to review, wait for CI to add an additional commit to your pull request, which updates `LATEST_BUILD_IMAGE_TAG` in the Makefile.
3. Merge the pull request.

The manual procedure is:

1. Update `mimir-build-image/Dockerfile` on a new branch. Note: the resulting images have the tag name derived from the branch name.
2. Make sure to have [Docker Buildx](https://docs.docker.com/buildx/working-with-buildx/). Docker Desktop and major distributions have it in the docker package.
3. On Linux you'll need some extra qemu packages as well: `sudo apt-get install qemu qemu-user-static binfmt-support debootstrap`. And set up docker buildx: `docker buildx create --name armBuilder ; docker buildx use armBuilder`.
4. Build and publish the image by using `make push-multiarch-build-image`. This will build and push multiplatform docker image (for Linux/amd64 and Linux/arm64).
5. Replace the image tag in Makefile (variable `LATEST_BUILD_IMAGE_TAG`).
6. Open a PR and make sure the CI with the new build image passes