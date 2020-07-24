# Steps to create the release:

1. Manually build and test the new additions
2. Create a new tag based on:  
    $ tag=v0.2.1  
    $ git tag -s "${tag}" -m "${tag}"  
    $ git push origin "${tag}"  

3. Create the binaries with build cross, they are in dist/
4. Create the GitHub release, copy the release notes from the previous ones, and adjust as necessary. Upload the binaries created and click publish on the release.
5. The last step is creating and uploading the docker images. Use make image to create them and then tag them. Keep in mind that there is only 1 image for cortextool in Dockerhub at the moment.