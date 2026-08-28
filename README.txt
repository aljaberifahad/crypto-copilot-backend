Crypto Copilot Intelligence Backend V40.21

Goal
Remove the FRED/API-key dependency wherever practical and use public, verified sources.

Primary sources with no API key:
- Binance public crypto data, with CoinGecko fallback
- U.S. Bureau of Labor Statistics public API
- U.S. Treasury official daily yield curve
- Cboe official VIX daily history
- BLS official release calendar
- Federal Reserve FOMC calendar
- Federal Reserve RSS feeds
- U.S. SEC press releases
- GDELT global news API

Secondary best-effort cross-asset source:
- Stooq quote snapshots for S&P 500, Nasdaq, WTI oil, gold and U.S. dollar futures.
  If this feed fails or changes access rules, it receives zero weight. Treasury and Cboe still remain active.

No fabricated data:
Any failed/unverified feed returns no score and has zero weight.

Important:
The backend still needs to be hosted somewhere your phone can reach. The HTML page cannot itself run Python.
After deployment, paste the HTTPS backend base URL into the dashboard once.

Run:
pip install -r requirements.txt
uvicorn app:app --host 0.0.0.0 --port 8000

Endpoints:
GET /health
GET /api/v1/source-health
GET /api/v1/daily-intelligence
GET /api/v1/market-shield
GET /api/v1/cross-asset
GET /api/v1/news
GET /api/v1/events
GET /api/v1/history?days=30
GET /api/v1/config

The Market Shield score is a risk index, not a guaranteed probability of a market decline.
