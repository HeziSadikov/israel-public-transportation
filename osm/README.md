# OSM data for routing (OSRM / Valhalla)

## Valhalla: prefer a manual `.pbf` download

The `valhalla-scripted` container downloads `tile_urls` with plain `curl`. **Geofabrik** sometimes returns a **tiny HTML/error file** (~200–500 bytes) instead of the real extract if the request looks like a bad bot.

**Check:** `israel-and-palestine-latest.osm.pbf` should be about **115 MB** (not a few hundred bytes).

### Option A — Browser (simplest)

1. Open: https://download.geofabrik.de/asia/israel-and-palestine.html  
2. Download **israel-and-palestine-latest.osm.pbf**  
3. Save it into this folder: `osm/israel-and-palestine-latest.osm.pbf`

### Option B — PowerShell (with User-Agent)

From the **project root**:

```powershell
New-Item -ItemType Directory -Force -Path osm | Out-Null
Invoke-WebRequest `
  -Uri "https://download.geofabrik.de/asia/israel-and-palestine-latest.osm.pbf" `
  -OutFile "osm/israel-and-palestine-latest.osm.pbf" `
  -UserAgent "Mozilla/5.0 (compatible; IsraelGTFS-Detour/1.0)"
```

### After the file is in place

1. Remove any **wrong** tiny file from a failed auto-download (a real extract is **~115 MB**; a few hundred bytes is an error page).  
2. With a valid `osm/*.pbf` present, Valhalla’s startup script **uses local files first** and does not need a successful `tile_urls` download.  
3. `docker compose up valhalla` — first **tile build** can take **a long time**.

## OSRM (optional)

The compose file expects preprocessed **`israel.osrm`** under `osm/` — see the main project README for extract/partition/customize.
