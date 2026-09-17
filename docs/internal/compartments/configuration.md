# Configuration

This page covers the rationale behind **where** compartments configuration lives. The configuration
options themselves are documented in the generated configuration reference, so they are not repeated
here.

## Why a root-level compartments configuration block

Whether compartments are enabled, and how many read compartments exist, is information that **every**
Mimir component needs. For that reason the shared compartments configuration lives in a single
top-level block, rather than nested under any one component's configuration.

## Why some settings are component-specific

Some compartment-related settings are only relevant to a single component. For example, the read
compartment that an ingester serves is specific to the ingester. Such a setting belongs in that
component's own configuration, not in the shared top-level block.
