---
title: "How to write documentation"
---

# How to write documentation

## Style guide

Grafana Mimir documentation follows Grafana Labs technical documentation [style guide](https://github.com/grafana/technical-documentation/tree/main/docs/style-guide).

## Content organization

Grafana Mimir documentation uses [Hugo page bundles](https://gohugo.io/content-management/page-bundles/) to group resources, such as images, with the pages.

The documentation adopts the following conventions:

- If a directory contains only one page, then it's a "Leaf bundle" and the content filename must be `index.md`.
- If a directory contains multiple pages or subdirectories, then it's a "Branch bundle" and the index filename must be `_index.md`.
- Images must be placed in the same directory as the markdown file linking the image, and the markdown file linking an image can only be `index.md` or `_index.md`. If any page which is different than `index.md` or `_index.md` needs to link an image, covert the page into a "Leaf bundle".

For more information, refer to [Hugo page bundles documentation](https://gohugo.io/content-management/page-bundles/).
