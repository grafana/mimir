# Release processes

## GoReleaser (`cortextool` only)

1. Create a changelog file in `changelog/` with the name of the tag e.g. `changelog/v0.3.0.md`. This will be used as the template for the release page in GitHub.
2. Create a new tag that follows semantic versioning:

```bash
$ tag=v0.3.0
$ git tag -s "${tag}" -m "${tag}"
$ git push origin "${tag}"
```

3. Run `$ goreleaser release --release-notes=changelogs/v0.3.0.md --rm-dist` where the changelog file is the one created as part of step 1.
4. The docker image will be pushed automatically.


## Manual (all the other binaries)

1. Manually build and test the new additions
2. Create a new tag based on:  
    $ tag=v0.2.1  
    $ git tag -s "${tag}" -m "${tag}"  
    $ git push origin "${tag}"  

3. Create the binaries with `make cross`, they are in dist/
4. Create the GitHub release, copy the release notes from the previous ones, and adjust as necessary. Upload the binaries created and click publish on the release.
5. The last step is creating and uploading the docker images. Use make image to create them and then tag them. Keep in mind that there is only 1 image for cortextool in Dockerhub at the moment.
6. Make sure to update the latest tag to the most recent version.
