# QueryFlux documentation site

This directory is a [Docusaurus](https://docusaurus.io/) site that mirrors the root [`README.md`](https://github.com/lakeops-org/queryflux/blob/main/README.md) and the [`docs/`](https://github.com/lakeops-org/queryflux/tree/main/docs) Markdown in `docs/` here. **Canonical sources** stay in the repository root (`README.md`, `development.md`, `contribute.md`, `docs/`); edit those and refresh the copies under `website/docs/` when they drift, or automate sync in CI if you prefer.

## Commands

```bash
npm install   # if node_modules is missing
npm start     # dev server (default http://localhost:3000)
npm run build # static output in build/
npm run serve # preview production build (search works here)
```

## Search

Local search uses [`@cmfcmf/docusaurus-search-local`](https://github.com/cmfcmf/docusaurus-search-local) ([Docusaurus: local search](https://docusaurus.io/docs/search#using-local-search)): the index is built at compile time and shipped with the site—no Algolia or other hosted service.

The search bar appears in the navbar after a **production build**. It does **not** work with `npm start` (dev mode); use `npm run build` then `npm run serve` to try it locally. Versioned docs are supported (results follow the doc version you are viewing).

## SEO

Global metadata, JSON-LD, and `static/robots.txt` follow [Docusaurus SEO](https://docusaurus.io/docs/seo). If you change `url`, `baseUrl`, or hosting domain, update **`static/robots.txt`** `Sitemap:` to match (preset-classic already emits `sitemap.xml`).

## Versioning

Docs follow [Docusaurus versioning](https://docusaurus.io/docs/versioning): **`docs/`** is the **Next** draft (`/docs/next/...`). Published snapshots live under **`versioned_docs/`** and **`versions.json`**.

When a release is ready to freeze:

```bash
npm run docs:version 0.2.0   # example; use your semver
```

Then edit **`sidebars.ts`** only for **Next**; for an older release, edit **`versioned_sidebars/version-X-sidebars.json`** and files under **`versioned_docs/version-X/`**.

## Deployment URL

`docusaurus.config.ts` sets `url` and `baseUrl` for publication (e.g. GitHub Pages project sites often use `baseUrl: '/queryflux/'`). Adjust those values to match your hosting layout.
