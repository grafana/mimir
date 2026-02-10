# Taylor Voice and Pattern Guide

This file describes Taylor’s preferred voice and structural patterns for conceptual and task-based documentation. The documentation agent should use these patterns when drafting and editing content for Taylor.

---

## 1. General voice characteristics

- Direct, neutral, and practical.
- Focus on what the user needs to know to complete their goals.
- Prefer short, clear sentences over complex or decorative phrasing.
- Avoid marketing tone. Explain what something is, what it does, and why the user cares.
- Use second person ("you") to speak to the user.
- Use present tense to describe system behavior.
- Use active voice whenever possible.

---

## 2. Conceptual topics

### 2.1 Typical structure

Conceptual topics usually follow this pattern:

1. **Definition / What it is**
   - Start with a clear definitional sentence.
   - Example pattern:
     - “A <thing> is <short description> that <what it enables or does>.”

2. **Why it matters / What it’s for**
   - Immediately tie the concept to user value or workflow.
   - Example pattern:
     - “Use <thing> to <help the user achieve a specific goal>.”
     - “Create <thing> to provide your users with <benefit or workflow>.”

3. **Requirements or environment differences (if applicable)**
   - Clearly call out when behavior differs by environment or product edition.
   - Example pattern:
     - “In <environment A>, you must <do X>.”
     - “In <environment B>, you can <do Y> as a best practice.”

4. **Use cases**
   - Add a “Use cases” section when appropriate.
   - Introduce with a short sentence, then a list.
   - Use per-item explanation plus a concrete example:
     - State the scenario.
     - Add “For example,” and describe a specific case.

5. **How it works**
   - Describe how the feature behaves at a high level.
   - Focus on user-visible behavior and configuration, not internal implementation, unless needed for user decisions.
   - Keep this section minimal.

### 2.2 Patterns from samples

- Preferred intro pattern:
  - “A <feature> is <short definition> that <what it enables>.”
- Preferred value pattern:
  - “Use <feature> to <do something specific>.”
  - “Create <feature> to provide your users with <workflow or benefit>.”
- Preferred environment contrast:
  - “In <product A>, you must…”
  - “In <product B>, you can… as a best practice.”

### 2.3 Lists and examples

- Use headings like “Use cases for <thing>” or “How <thing> works.”
- Under “Use cases”, present each use case as:
  - A clear statement of the user intention.
  - Followed by “For example,” and a concrete scenario that shows what the user actually does or sees.

---

## 3. Task / procedure topics

### 3.1 Typical structure

Task topics usually follow this pattern:

1. **Task title**
   - Imperative, starting with a verb.
   - Pattern: “<Verb> <object>…”
   - Examples:
     - “Manage access to custom REST endpoints”
     - “Configure <feature>…”
     - “Create <resource>…”

2. **Purpose and context paragraph**
   - Short intro that explains:
     - What the task does.
     - Why a user would do it.
     - Any important default behavior.
   - Example patterns:
     - “<Verb phrase> to <reason or outcome>.”
     - “You can <do X> to <achieve Y>.”
     - “By default, <system behavior>. You can change this by <doing Z>.”

3. **Prerequisites (if needed)**
   - Inline in the intro or as a small “Before you begin” section.
   - Only include prerequisites that are necessary to perform the task.

4. **Numbered steps**
   - Each step starts with a clear imperative verb:
     - “Create…”, “Open…”, “Add…”, “Run…”
   - Keep steps short and sequential.
   - Embed file paths or commands inline in the step when needed.
   - Example:
     - “Create an `authorize.conf` file in the `default` directory of your app. For example, create `authorize.conf` in `$SPLUNK_HOME/etc/apps/app_name/default`.”

5. **Follow-up or result**
   - Optionally close with a brief description of the result, especially if the outcome isn’t obvious.

### 3.2 Patterns from samples

- Task intro pattern:
  - “<Verb phrase> to <explain purpose>.”
  - “You can <do X> to <achieve Y>.”
  - “By default, <behavior>. You can <change it> by <doing Z>.”

- Step pattern:
  - Step 1: Create or open a file or UI surface.
  - Step 2: Edit or add specific lines or settings.
  - Step 3+: Apply, save, or confirm changes.

- File path usage:
  - Present file paths in plain text or code formatting.
  - Show a concrete example path in the same step:
    - “For example, create `<file>` in `<full/path>`.”

---

## 4. Sentence-level preferences

The agent should use these sentence-level habits when drafting and editing:

1. **Definition sentences**
   - Use “A/An <thing> is…” to introduce a concept.
   - Keep the first sentence concrete and specific, not abstract.

2. **User-focused phrasing**
   - Prefer “You can…” and “You do X to…” over “Users can…” or “The system lets you…”.
   - Frame descriptions in terms of what the user is trying to do.

3. **Default behavior statements**
   - Use “By default, <behavior>.” to surface defaults clearly.
   - Follow with how to change the default if relevant.

4. **Example phrasing**
   - Use “For example,” to introduce specific cases.
   - Place the example immediately after the general use case description.

5. **Clarity and minimalism**
   - Remove sentences that restate the same point without adding value.
   - Avoid long chains of clauses; prefer two shorter sentences instead.

---

## 5. Headings and organization patterns

- Use clear, descriptive headings like:
  - “Use cases for <thing>”
  - “How <thing> works”
  - “Workflow for <thing>”
  - “Manage <object>”
  - “Configure <feature>”

- For conceptual pages:
  - Start with a short introductory section before lists or detailed breakdowns.

- For tasks:
  - Use a single-sentence intro or short paragraph before the steps.

---

## 6. How the agent should use this file

When drafting:

- For conceptual topics:
  - Use the definition → value → environment → use cases → how it works pattern,
    unless the context strongly implies a different structure.

- For task topics:
  - Use the imperative title → purpose paragraph → steps pattern.

When editing:

- Adjust sentences to match the patterns in this file.
- Rewrite intros to:
  - Clearly define what the thing is or what the task achieves.
  - Quickly tie to user goals.
- Streamline conceptual sections to keep them minimal and directly helpful.

If there is a conflict between this pattern guide and a specific instruction in `taylor_agent.md` or the Grafana Style Guide:

- Follow the Grafana Style Guide first,
- Then `taylor_agent.md`,
- Then this pattern guide.
