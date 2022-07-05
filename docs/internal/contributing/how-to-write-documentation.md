# How to write documentation

## Style guide

Grafana Mimir documentation follows Grafana Labs technical documentation [style guide](https://github.com/grafana/grafana/blob/main/contribute/documentation/README.md).

## Content organization

Grafana Mimir documentation uses [Hugo page bundles](https://gohugo.io/content-management/page-bundles/) to group resources, such as images, with the pages.

The documentation adopts the following conventions:

- If a directory contains only one page, then it's a _leaf bundle_ and the content filename must be `index.md`.
- If a directory contains multiple pages or subdirectories, then it's a _branch bundle_ and the index filename must be `_index.md`.
- Images need to be in the same directory as the Markdown file that links to the image, and the Markdown file that links to an image can only be `index.md` or `_index.md`. If any page that is different than `index.md` or `_index.md` needs to link to an image, covert the page to a _leaf bundle_.

Run `make check-doc-validator` to validate the documentation follows the mentioned conventions.
The validation also runs in CI on every pull request in the `doc-validator` job.

For more information, refer to Hugo’s [Page bundles](https://gohugo.io/content-management/page-bundles/) documentation.
