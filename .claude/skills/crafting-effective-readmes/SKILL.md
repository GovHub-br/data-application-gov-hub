---
name: crafting-effective-readmes
description: Use when writing or improving README files. Not all READMEs are the same — provides templates and guidance matched to your audience and project type.
---

<!-- PROCEDÊNCIA
Copiada de GovHub-br/GovHub-skills, pasta 06-docs-relatorios/crafting-effective-readmes
Commit de origem: 8ffa097e6c3e5f63c6de73aca9d976897b1307b0 (2026-08-10)

Cópia, e não instalação por marketplace: a equipe recebe as skills ao
clonar, sem rodar nada. O custo é que correção feita lá em cima não chega
sozinha aqui — para atualizar, recopie a pasta e troque o commit acima.
-->

# Crafting Effective READMEs

## Overview

READMEs answer questions your audience will have. Different audiences need different information - a contributor to an OSS project needs different context than future-you opening a config folder.

**Always ask:** Who will read this, and what do they need to know?

## Process

### Step 1: Identify the Task

**Ask:** "What README task are you working on?"

| Task | When |
|------|------|
| **Creating** | New project, no README yet |
| **Adding** | Need to document something new |
| **Updating** | Capabilities changed, content is stale |
| **Reviewing** | Checking if README is still accurate |

### Step 2: Task-Specific Questions

**Creating initial README:**
1. What type of project? (see Project Types below)
2. What problem does this solve in one sentence?
3. What's the quickest path to "it works"?
4. Anything notable to highlight?

**Adding a section:**
1. What needs documenting?
2. Where should it go in the existing structure?
3. Who needs this info most?

**Updating existing content:**
1. What changed?
2. Read current README, identify stale sections
3. Propose specific edits

**Reviewing/refreshing:**
1. Read current README
2. Check against actual project state (package.json, main files, etc.)
3. Flag outdated sections
4. Update "Last reviewed" date if present

### Step 3: Always Ask

After drafting, ask: **"Anything else to highlight or include that I might have missed?"**

## Project Type

Este repositório é **interno**: o público é a equipe e quem entra nela, mesmo o
repositório sendo público no GitHub. Ninguém instala isto como biblioteca.

| Audience | Key Sections | Template |
|----------|--------------|----------|
| Colegas de equipe, quem está chegando | Setup, arquitetura, runbooks | `templates/internal.md` |

A cópia original desta skill trazia templates para projeto open source, README
pessoal e pasta de configuração XDG, além de três ensaios externos sobre README.
Foram retirados por não terem uso aqui — se algum dia precisar, estão na versão
de origem, no [GovHub-skills](https://github.com/GovHub-br/GovHub-skills).

## Essential Sections (All Types)

Every README needs at minimum:

1. **Name** - Self-explanatory title
2. **Description** - What + why in 1-2 sentences  
3. **Usage** - How to use it (examples help)

## References

- `section-checklist.md` - Which sections to include by project type
- `style-guide.md` - Common README mistakes and prose guidance
