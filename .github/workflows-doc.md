# GitHub Actions CI/CD

The purpose of this workflow is to run all continuous integration (CI) and continuous deployment (CD) jobs when needed while respecting their internal dependencies. The continuous integration jobs serve to ensure new code passes linting, unit tests and integration tests before reaching the main branch. The continuous deployment jobs serve to deploy the latest version of the code to mimir and the website when merged with main.

## Contributing

If you wish to add a new CI or CD job, add it to the existing current test-build-deploy workflow and make sure it does not prevent any of the other jobs from passing. If you wish to change any of the build or testing images, update it in all sections are containers are often reused. If you wish to add an entirely new workflow, create a new yml file with separate triggers and filters. In all cases, clearly document any changes made to the workflows, images and dependencies below.

## Test, Build and Deploy

test-build-deploy.yml specifies a workflow that runs all Mimir continuous integration and continuous deployment jobs. The workflow is triggered on every pull request and commit to main, however the CD jobs only run when changes are merged onto main. The workflow combines both CI and CD jobs, because the CD jobs are dependent on artifacts produced the CI jobs.


## Specific Jobs

| Job                    | Description                                                                                                                   | Type |
|------------------------|-------------------------------------------------------------------------------------------------------------------------------|------|
| lint                   | Runs linting and ensures vendor directory, protos and generated documentation are consistent.                                 | CI   |
| test                   | Runs units tests on Cassandra testing framework.                                                                              | CI   |
| integration            | Runs integration tests after upgrading golang, pulling necessary docker images and downloading necessary module dependencies. | CI   |
| build                  | Builds and saves an up-to-date Mimir image and website.                                                                       | CI   |
| deploy_website         | Deploys the latest version of Mimir website to gh-pages branch. Triggered within workflow.                                    | CD   |
| deploy                 | Deploys the latest Mimir image.                                                                                               | CD   |
