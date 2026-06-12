# Authoring guide for the compartments documentation

This directory documents the **design** of Mimir compartments and the **rationale** behind the design
decisions. It is read by both humans and agents.

## Keep this documentation updated

Whenever a compartments design decision is taken, or the compartments implementation iterates, update
the relevant file in this directory **as part of the same change**. Treat these docs as living
documentation that tracks the architecture as it evolves.

## What to include

- How the architecture works.
- Why decisions were made (the rationale).

## What to exclude

- Implementation detail: source code, function/type/symbol names, file paths, or component internals.
  Describe behavior, not code.
- Before/after narration ("we changed X to Y", "we removed Z"). Document how things work **today** as
  plain facts.
- Sensitive or internal-only data: customer names, customer sizes (e.g. active-series counts), tenant
  IDs, namespaces. Refer to internal-only components generically (for example, "an ingress auth gateway
  or a load balancer").

## Style

- One topic per file, with self-contained sections and explicit headings.
- Prefer prose and **pseudo-code** over real code.
- Describe each topic generically first (a dedicated Kafka cluster per compartment). Put any
  Warpstream-specific behavior in a clearly-marked "Warpstream specifics" subsection within the same
  file.
- Do not invent rationale: every "why" must trace to an actual design decision.
