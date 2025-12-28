# Fivetran Sync Orchestrator

A Python utility to trigger and monitor Fivetran connector syncs via the Fivetran REST API.

## Features
- Force-trigger syncs
- Poll connector status until completion
- Configurable polling intervals
- Global timeout protection

## Prerequisites
- Python 3.9+
- Fivetran API key and secret

## Setup

```bash
git clone https://github.com/<your-username>/fivetran-sync-orchestrator.git
cd fivetran-sync-orchestrator
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
